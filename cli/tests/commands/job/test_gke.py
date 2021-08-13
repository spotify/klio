# Copyright 2021 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import glom
import pytest

from kubernetes import client as k8s_client

from klio_core import config

from klio_cli import cli
from klio_cli.commands.job import gke as job_gke


@pytest.fixture
def mock_os_environ(mocker):
    return mocker.patch.dict(
        job_gke.base.os.environ, {"USER": "cookiemonster"}
    )


@pytest.fixture
def klio_config():
    conf = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {
            "worker_harness_container_image": (
                "gcr.io/sigint/gke-baseline-random-music-gke"
            ),
            "region": "some-region",
            "project": "test-project",
        },
        "job_config": {
            "inputs": [
                {
                    "topic": "foo-topic",
                    "subscription": "foo-sub",
                    "data_location": "foo-input-location",
                }
            ],
            "outputs": [
                {
                    "topic": "foo-topic-output",
                    "data_location": "foo-output-location",
                }
            ],
        },
    }
    return config.KlioConfig(conf)


@pytest.fixture
def docker_runtime_config():
    return cli.DockerRuntimeConfig(
        image_tag="foo-123",
        force_build=False,
        config_file_override="klio-job2.yaml",
    )


@pytest.fixture
def run_job_config():
    return cli.RunJobConfig(
        direct_runner=False, update=False, git_sha="12345678"
    )


@pytest.fixture
def mock_docker_client(mocker):
    mock_client = mocker.Mock()
    mock_container = mocker.Mock()
    mock_container.wait.return_value = {"StatusCode": 0}
    mock_container.logs.return_value = [b"a log line\n", b"another log line\n"]
    mock_client.containers.run.return_value = mock_container
    return mock_client


@pytest.fixture
def run_pipeline_gke(
    klio_config,
    docker_runtime_config,
    run_job_config,
    mock_docker_client,
    mock_os_environ,
    monkeypatch,
):
    job_dir = "/test/dir/jobs/test_run_job"
    pipeline = job_gke.RunPipelineGKE(
        job_dir=job_dir,
        klio_config=klio_config,
        docker_runtime_config=docker_runtime_config,
        run_job_config=run_job_config,
    )

    monkeypatch.setattr(pipeline, "_docker_client", mock_docker_client)
    return pipeline


@pytest.fixture
def deployment_config():
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "gke-baseline-random-music",
            "namespace": "sigint",
            "labels": {"app": "gke-baseline-random-music"},
        },
        "spec": {
            "replicas": 100,
            "strategy": {"type": "Recreate"},
            "selector": {
                "matchLabels": {
                    "app": "gke-baseline-random-music",
                    "role": "gkebaselinerandommusic",
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app": "gke-baseline-random-music",
                        "role": "gkebaselinerandommusic",
                    },
                    "annotations": {
                        "podpreset.admission.spotify.com/exclude": (
                            "container/" "ffwd-java-shim, environment/ffwd"
                        )
                    },
                },
                "spec": {
                    "volumes": [
                        {
                            "name": "google-cloud-key",
                            "secret": {"secretName": "lynn-podcast-key"},
                        }
                    ],
                    "containers": [
                        {
                            "name": "gke-baseline-random-music",
                            "image": (
                                "gcr.io/sigint/gke-base"
                                "line-random-music-gke"
                            ),
                            "resources": {
                                "requests": {"cpu": 4, "memory": "16G"},
                                "limits": {"cpu": 8, "memory": "20G"},
                            },
                            "volumeMounts": [
                                {
                                    "name": "google-cloud-key",
                                    "mountPath": "/var/secrets/google",
                                }
                            ],
                            "env": [
                                {
                                    "name": "GOOGLE_APPLICATION_CREDENTIALS",
                                    "value": "/var/secrets/google/key.json",
                                }
                            ],
                        }
                    ],
                },
            },
        },
    }


@pytest.fixture
def deployment_resp(deployment_config):
    container_config = glom.glom(
        deployment_config, "spec.template.spec.containers.0"
    )
    container = k8s_client.V1Container(
        name=container_config["name"],
        image=container_config["image"],
        ports=[k8s_client.V1ContainerPort(container_port=80)],
        resources=k8s_client.V1ResourceRequirements(
            requests=glom.glom(container_config, "resources.requests"),
            limits=glom.glom(container_config, "resources.limits"),
        ),
    )

    # Create and configure a spec section
    container_spec_config = glom.glom(
        deployment_config, "spec.template.metadata"
    )
    template = k8s_client.V1PodTemplateSpec(
        metadata=k8s_client.V1ObjectMeta(
            labels=container_spec_config["labels"],
            annotations=container_spec_config["annotations"],
        ),
        spec=k8s_client.V1PodSpec(containers=[container]),
    )

    # Create the specification of deployment
    deployment_spec_config = deployment_config["spec"]
    spec = k8s_client.V1DeploymentSpec(
        replicas=deployment_spec_config["replicas"],
        template=template,
        selector=deployment_spec_config["selector"],
    )

    # Instantiate the deployment object
    deployment = k8s_client.V1Deployment(
        api_version=deployment_config["apiVersion"],
        kind=deployment_config["kind"],
        metadata=k8s_client.V1ObjectMeta(
            name=glom.glom(deployment_config, "metadata.name")
        ),
        spec=spec,
    )

    return deployment


@pytest.fixture
def deployment_response_list(deployment_config, deployment_resp):
    resp = k8s_client.models.v1_deployment_list.V1DeploymentList(
        items=[deployment_resp]
    )
    return resp


@pytest.fixture
def deployment_response_list_not_exist():
    resp = k8s_client.models.v1_deployment_list.V1DeploymentList(items=[])
    return resp


@pytest.fixture
def active_context():
    return {
        "context": {
            "cluster": "gke_gke-xpn-1_us-east1_us-east1-kn0t",
            "namespace": "sigint",
            "user": "gke_gke-xpn-1_us-east1_us-east1-kn0t",
        },
        "name": "gke_gke-xpn-1_us-east1_us-east1-kn0t",
    }


def test_update_deployment(
    deployment_config,
    run_pipeline_gke,
    deployment_resp,
    active_context,
    monkeypatch,
    mocker,
):
    deployment_name = glom.glom(deployment_config, "metadata.name")
    namespace = glom.glom(deployment_config, "metadata.namespace")
    mock_k8s_client = mocker.Mock()
    mock_k8s_client.patch_namespaced_deployment.return_value = deployment_resp

    monkeypatch.setattr(
        run_pipeline_gke, "_kubernetes_client", mock_k8s_client
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_kubernetes_active_context", active_context
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_deployment_config", deployment_config
    )

    run_pipeline_gke._update_deployment()
    mock_k8s_client.patch_namespaced_deployment.assert_called_once_with(
        name=deployment_name, namespace=namespace, body=deployment_config,
    )


# Tests for internal functions
@pytest.mark.parametrize(
    "deployed,is_exists",
    (
        (["deployment-1", "gke-baseline-random-music"], True),
        (["deployment-2"], False),
    ),
)
def test_deployment_exists(
    deployment_resp,
    deployment_config,
    active_context,
    run_pipeline_gke,
    deployment_response_list,
    monkeypatch,
    mocker,
    deployed,
    is_exists,
):
    mock_k8s_client = mocker.Mock()
    mock_k8s_client.patch_namespaced_deployment.return_value = deployment_resp
    mock_k8s_client.list_namespaced_deployment.return_value = (
        deployment_response_list
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_kubernetes_client", mock_k8s_client
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_kubernetes_active_context", active_context
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_deployment_config", deployment_config
    )
    run_pipeline_gke._deployment_exists()
    mock_k8s_client.list_namespaced_deployment.assert_called_once_with(
        namespace=glom.glom(deployment_config, "metadata.namespace")
    )


@pytest.mark.parametrize(
    "label_dict",
    (
        {"foo": ""},
        {"f": "b"},
        {"foo/bar": "baz"},
        {"f" * 63: "b" * 63},
        {"foo.bar_baz-bla": "foo_bar-baz.bla"},
        # avoid hitting total max of 253 chars for prefix
        {
            "a" * 63
            + "."
            + "b" * 63
            + "."
            + "c" * 63
            + "."
            + "d" * 61
            + "/abcd": "bla"
        },
    ),
)
def test_validate_labels(label_dict):
    assert job_gke.RunPipelineGKE._validate_labels("f.b", label_dict) is None


@pytest.mark.parametrize(
    "label_dict",
    (
        # invalid keys
        {"": "bar"},
        {"-foo": "bar"},
        {"foo-": "bar"},
        {"f?oo": "bar"},
        {"f" * 64: "bar"},
        # invalid prefixes
        {"/foo": "bar"},
        {"-/foo": "bar"},
        {"foo/bar/baz": "bar"},
        {"kubernetes.io/foo": "bar"},
        {"k8s.io/foo": "bar"},
        # hit max chars for subdomain in prefix
        {"a" * 64 + "." + "b" * 63 + "/ab": "bla"},
        # hit total max chars for prefix
        {
            "a" * 63
            + "."
            + "b" * 63
            + "."
            + "c" * 63
            + "."
            + "d" * 63
            + "/abcd": "bla"
        },
        # invalid values
        {"foo": "-bar"},
        {"foo": "bar-"},
        {"foo": "ba?r"},
        {"foo": "b" * 64},
        {"foo": "bar=baz"},
    ),
)
def test_validate_labels_raises(label_dict):
    with pytest.raises(ValueError):
        job_gke.RunPipelineGKE._validate_labels("foo.bar", label_dict)


@pytest.mark.parametrize(
    "input_config",
    (
        # minimal
        {"metadata": {"name": "test-job"}},
        # metadata labels defined
        {"metadata": {"name": "test-job", "labels": {"app": "test-job"}}},
        # pod labels defined
        {
            "metadata": {"name": "test-job"},
            "spec": {
                "template": {
                    "metadata": {
                        "labels": {"app": "test-job", "role": "testjob"}
                    }
                }
            },
        },
        # selector labels defined
        {
            "metadata": {"name": "test-job"},
            "spec": {
                "selector": {
                    "matchLabels": {"app": "test-job", "role": "testjob"}
                }
            },
        },
    ),
)
@pytest.mark.parametrize(
    "is_ci,exp_deployed_by", (("true", "ci"), ("false", "stub-user"))
)
def test_apply_labels_to_deployment_config(
    input_config, is_ci, exp_deployed_by, run_pipeline_gke, monkeypatch
):
    monkeypatch.setitem(job_gke.os.environ, "USER", "stub-user")
    monkeypatch.setitem(job_gke.os.environ, "CI", is_ci)
    monkeypatch.setattr(job_gke, "klio_cli_version", "stub-version")

    # TODO: patch user config for user labels
    monkeypatch.setattr(run_pipeline_gke, "_deployment_config", input_config)
    user_labels = [
        "label_a=value_a",
        "label-b=value-b",
        "label-c=",
        "labeld",  # invalid, expected to be ignored
    ]
    monkeypatch.setattr(
        run_pipeline_gke.klio_config.pipeline_options, "labels", user_labels
    )

    expected_config = {
        "metadata": {"name": "test-job", "labels": {"app": "test-job"}},
        "spec": {
            "template": {
                "metadata": {
                    "labels": {
                        "app": "test-job",
                        "role": "testjob",
                        "klio/deployed_by": exp_deployed_by,
                        "klio/klio_cli_version": "stub-version",
                        "label_a": "value_a",
                        "label-b": "value-b",
                        "label-c": "",
                    }
                },
            },
            "selector": {
                "matchLabels": {"app": "test-job", "role": "testjob"}
            },
        },
    }

    run_pipeline_gke._apply_labels_to_deployment_config()
    assert expected_config == run_pipeline_gke.deployment_config


def test_apply_labels_to_deployment_config_overrides(
    run_pipeline_gke, monkeypatch
):
    monkeypatch.setitem(job_gke.os.environ, "USER", "stub-user")
    monkeypatch.setattr(job_gke, "klio_cli_version", "stub-version")

    input_config = {
        "metadata": {
            "name": "test-job",
            "labels": {"app": "different-app-name"},
        }
    }
    monkeypatch.setattr(run_pipeline_gke, "_deployment_config", input_config)

    expected_config = {
        "metadata": {
            "name": "test-job",
            "labels": {"app": "different-app-name"},
        },
        "spec": {
            "template": {
                "metadata": {
                    "labels": {
                        "app": "different-app-name",
                        "role": "differentappname",
                        "klio/deployed_by": "stub-user",
                        "klio/klio_cli_version": "stub-version",
                    }
                },
            },
            "selector": {
                "matchLabels": {
                    "app": "different-app-name",
                    "role": "differentappname",
                }
            },
        },
    }

    run_pipeline_gke._apply_labels_to_deployment_config()
    assert expected_config == run_pipeline_gke.deployment_config


# Tests for user facing functions
@pytest.mark.parametrize(
    "deployment_exists,update_flag,mismatched_image",
    (
        (True, False, False),
        (False, True, False),
        (False, False, False),
        (False, True, True),
    ),
)
def test_apply_deployment(
    monkeypatch,
    mocker,
    run_pipeline_gke,
    run_job_config,
    active_context,
    docker_runtime_config,
    deployment_config,
    deployment_resp,
    deployment_response_list,
    deployment_response_list_not_exist,
    deployment_exists,
    update_flag,
    mismatched_image,
    caplog,
):
    # New Deployment
    caplog_counter = 0
    test_image_base = "test-image"
    run_job_config = run_job_config._replace(update=update_flag)
    if mismatched_image:
        monkeypatch.setattr(
            run_pipeline_gke.klio_config.pipeline_options,
            "worker_harness_container_image",
            test_image_base,
        )
    monkeypatch.setattr(run_pipeline_gke, "run_job_config", run_job_config)
    mock_k8s_client = mocker.Mock()
    mock_k8s_client.create_namespaced_deployment.return_value = deployment_resp
    mock_k8s_client.patch_namespaced_deployment.return_value = deployment_resp
    mock_k8s_client.list_namespaced_deployment.return_value = (
        deployment_response_list
        if deployment_exists
        else deployment_response_list_not_exist
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_kubernetes_client", mock_k8s_client
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_kubernetes_active_context", active_context
    )
    monkeypatch.setattr(
        run_pipeline_gke, "_deployment_config", deployment_config
    )
    deployment_name = glom.glom(deployment_config, "metadata.name")
    namespace = glom.glom(deployment_config, "metadata.namespace")
    image_path = "spec.template.spec.containers.0.image"
    image_base = glom.glom(deployment_config, image_path)
    k8s_image = f"{image_base}:{docker_runtime_config.image_tag}"
    run_pipeline_gke._apply_image_to_deployment_config()
    run_pipeline_gke._apply_deployment()
    assert (
        glom.glom(run_pipeline_gke._deployment_config, image_path) == k8s_image
    )
    glom.assign(deployment_config, image_path, k8s_image)
    if deployment_exists:
        if update_flag:
            mock_k8s_client.patch_namespaced_deployment.assert_called_once_with(
                name=deployment_name,
                namespace=namespace,
                body=deployment_config,
            )
            caplog_counter += 1
        else:
            caplog_counter += 1
    else:
        mock_k8s_client.create_namespaced_deployment.assert_called_once_with(
            body=deployment_config, namespace=namespace
        )
        caplog_counter += 1

    if deployment_exists and not update_flag:
        assert caplog.records[-1].msg == (
            f"Cannot apply deployment for {deployment_name}. "
            "To update an existing deployment, run "
            "`klio job run --update`, or set `pipeline_options.update`"
            " to `True` in the job's`klio-job.yaml` file. "
            "Run `klio job stop` to scale a deployment down to 0. "
            "Run `klio job delete` to delete a deployment entirely."
        )

    if mismatched_image:
        caplog_counter += 1
        built_image = f"{test_image_base}:{docker_runtime_config.image_tag}"
        assert caplog.records[0].msg == (
            f"Image deployed by kubernetes {k8s_image} does not match "
            f"the built image {built_image}. "
            "This may result in an `ImagePullBackoff` for the deployment. "
            "If this is not intended, please change "
            "`pipeline_options.worker_harness_container_image` "
            "and rebuild  or change the container image"
            "set in kubernetes/deployment.yaml file."
        )
    assert len(caplog.records) == caplog_counter


def test_delete(
    monkeypatch,
    mocker,
    deployment_response_list,
    deployment_config,
    active_context,
):
    namespace = glom.glom(deployment_config, "metadata.namespace")
    deployment_name = glom.glom(deployment_config, "metadata.name")
    mock_k8s_client = mocker.Mock()
    mock_k8s_client.patch_namespaced_deployment.return_value = deployment_resp
    mock_k8s_client.list_namespaced_deployment.return_value = (
        deployment_response_list
    )

    delete_pipeline_gke = job_gke.DeletePipelineGKE("/some/job/dir")

    monkeypatch.setattr(
        delete_pipeline_gke, "_kubernetes_client", mock_k8s_client
    )
    monkeypatch.setattr(
        delete_pipeline_gke, "_kubernetes_active_context", active_context
    )
    monkeypatch.setattr(
        delete_pipeline_gke, "_deployment_config", deployment_config
    )
    delete_pipeline_gke.delete()
    mock_k8s_client.delete_namespaced_deployment.assert_called_once_with(
        name=deployment_name,
        namespace=namespace,
        body=k8s_client.V1DeleteOptions(
            propagation_policy="Foreground", grace_period_seconds=5
        ),
    )


def test_stop(
    deployment_resp, deployment_config, active_context, monkeypatch, mocker
):
    deployment_name = glom.glom(deployment_config, "metadata.name")
    namespace = glom.glom(deployment_config, "metadata.namespace")
    mock_k8s_client = mocker.Mock()
    mock_k8s_client.patch_namespaced_deployment.return_value = deployment_resp

    stop_pipeline_gke = job_gke.StopPipelineGKE("/some/job/dir")

    monkeypatch.setattr(
        stop_pipeline_gke, "_kubernetes_client", mock_k8s_client
    )
    monkeypatch.setattr(
        stop_pipeline_gke, "_kubernetes_active_context", active_context
    )
    monkeypatch.setattr(
        stop_pipeline_gke, "_deployment_config", deployment_config
    )
    stop_pipeline_gke.stop()
    mock_k8s_client.patch_namespaced_deployment.assert_called_once_with(
        name=deployment_name, namespace=namespace, body=deployment_config,
    )
