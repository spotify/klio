# Copyright 2019-2020 Spotify AB
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

import errno
import logging
import os
import re

import glom
import yaml
from kubernetes import client, config

from klio_cli.commands import base
from klio_cli.utils import docker_utils


class GKECommandMixin(object):
    # NOTE : This command requires a job_dir attribute

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._deployment_config = None
        self._kubernetes_client = None

    def _validate_deployment_config(self):
        # TODO: Where should we call this?
        #  We should also validate presence of fields @shireenk
        path_to_deployment_config = os.path.join(
            self.job_dir, "kubernetes", "deployment.yaml"
        )
        if not os.path.exists(path_to_deployment_config):
            raise FileNotFoundError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path_to_deployment_config,
            )

    @property
    def kubernetes_client(self):
        if not self._kubernetes_client:
            # TODO: This grabs configs from '~/.kube/config'. @shireenk
            #  We should add a check that this file exists
            # If it does not exist then we should create configurations.
            # See link:
            # https://github.com/kubernetes-client/python-base/blob/master/config/kube_config.py#L825
            config.load_kube_config()
            self._kubernetes_client = client.AppsV1Api()
        return self._kubernetes_client

    @property
    def deployment_config(self):
        if not self._deployment_config:
            path_to_deployment_config = os.path.join(
                self.job_dir, "kubernetes", "deployment.yaml"
            )
            with open(path_to_deployment_config) as f:
                self._deployment_config = yaml.safe_load(f)
        return self._deployment_config

    def _deployment_exists(self):
        """
        Check to see if a deployment already exists

        :return bool
            Whether a deployment for the given name-namespace combo exists
        """
        dep = self.deployment_config
        namespace = dep["metadata"]["namespace"]
        deployment_name = dep["metadata"]["name"]
        resp = self.kubernetes_client.list_namespaced_deployment(
            namespace=namespace
        )
        for i in resp.items:
            if i.metadata.name == deployment_name:
                return True
        return False


class RunPipelineGKE(GKECommandMixin, base.BaseDockerizedPipeline):
    def __init__(
        self, job_dir, klio_config, docker_runtime_config, run_job_config
    ):
        super().__init__(job_dir, klio_config, docker_runtime_config)
        self.run_job_config = run_job_config

    def _apply_image_to_deployment_config(self):
        image_tag = self.docker_runtime_config.image_tag
        if image_tag:
            dep = self.deployment_config
            image_path = "spec.template.spec.containers.0.image"
            # TODO: If more than one image deployed,
            #  we need to search for correct container
            image_base = glom.glom(dep, image_path)
            # Strip off existing image tag if any
            image_base = re.split(":", image_base)[0]
            full_image = f"{image_base}:{image_tag}"
            glom.assign(self._deployment_config, image_path, full_image)

    def _apply_deployment(self):
        """
        Create a namespaced deploy if the deployment does not already exist.
        If the namespaced deployment already exists then
        `self.run_job_config.update` will determine if the
        deployment will be updated or not.
        """
        dep = self.deployment_config
        namespace = dep["metadata"]["namespace"]
        deployment_name = dep["metadata"]["name"]
        if not self._deployment_exists():
            resp = self.kubernetes_client.create_namespaced_deployment(
                body=dep, namespace=namespace
            )
            deployment_name = resp.metadata.name
            logging.info(f"Deployment created for {deployment_name}")
        else:
            if self.run_job_config.update:
                self._update_deployment()
            else:
                logging.warning(
                    f"Cannot apply deployment for {deployment_name}."
                    f"If deployment already exists, set `update` to True."
                )

    def _setup_docker_image(self):
        super()._setup_docker_image()

        logging.info("Pushing worker image to GCR")
        docker_utils.push_image_to_gcr(
            self._full_image_name,
            self.docker_runtime_config.image_tag,
            self._docker_client,
        )

    def run(self, *args, **kwargs):
        # NOTE: Notice this job doesn't actually run docker locally, but we
        # still have to build and push the image before we can run kubectl

        # docker image setup
        self._check_gcp_credentials_exist()
        self._check_docker_setup()
        self._setup_docker_image()

        self._apply_image_to_deployment_config()
        self._apply_deployment(**kwargs)


class StopPipelineGKE(GKECommandMixin):
    def __init__(self, job_dir):
        super().__init__()
        self.job_dir = job_dir

    def _update_deployment(self, replica_count=None, image_tag=None):
        """
        This will update a deployment with a provided replica count or image tag
        :param int replica_count
            Number of replicas the deployment will be updated with
            If not provided then this will not be changed
        :param str image_tag
            The image tag that will be applied to the updated deployment
            If not provided then this will not be updated
        """
        dep = self.deployment_config
        deployment_name = glom.glom(dep, "metadata.name")
        namespace = glom.glom(dep, "metadata.namespace")
        if replica_count:
            glom.assign(dep, "spec.replicas", replica_count)
        if image_tag:
            image_path = "spec.template.spec.containers.0.image"
            image_base = glom.glom(dep, image_path)
            # Strip off existing image tag if present
            image_base = re.split(":", image_base)[0]
            full_image = image_base + f":{image_tag}"
            glom.assign(self._deployment_config, image_path, full_image)
        resp = self.kubernetes_client.patch_namespaced_deployment(
            name=deployment_name,
            namespace=namespace,
            body=dep,
        )
        logging.info(f"Scaled deployment {resp.metadata.name}")

    def stop(self):
        """
        Delete a namespaced deployment
        Expects existence of a kubernetes/deployment.yaml
        """
        self._update_deployment(replica_count=0)


class DeletePipelineGKE(GKECommandMixin):
    def __init__(self, job_dir):
        super().__init__()
        self.job_dir = job_dir

    def _delete_deployment(self):
        dep = self.deployment_config
        deployment_name = glom.glom(dep, "metadata.name")
        namespace = glom.glom(dep, "metadata.namespace")
        if self._deployment_exists():
            resp = self.kubernetes_client.delete_namespaced_deployment(
                name=deployment_name,
                namespace=namespace,
                body=client.V1DeleteOptions(
                    propagation_policy="Foreground", grace_period_seconds=5
                ),
            )
            logging.info(f"Deployment deleted: {resp}.")
        else:
            logging.error(
                f"Deployment {namespace}:{deployment_name}" f"does not exist."
            )

    def delete(self):
        """
        Delete a namespaced deployment
        Expects existence of a kubernetes/deployment.yaml
        """
        self._delete_deployment()
