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

import logging
import re

import click
import emoji

from klio_cli.commands.job.utils import create_args
from klio_cli.commands.job.utils import gcp_setup
from klio_cli.commands.job.utils import rendering


# Valid project IDs: https://cloud.google.com/resource-manager/reference
#                    /rest/v1/projects#Project
# Valid topic names: https://cloud.google.com/pubsub/docs/admin#resource_names
# Will save matches into symbolic groups named "project" and "topic"
TOPIC_REGEX = re.compile(
    r"^projects/(?P<project>[a-z0-9-]{6,30})/"
    r"topics/(?P<topic>[a-zA-Z0-9-_.~+%]{3,255})"
)
# TODO: include all dataflow-supported GCP regions
GCP_REGIONS = ["europe-west1", "us-central1", "asia-east1"]
VALID_BEAM_PY_VERSIONS = ["3.5", "3.6", "3.7"]
VALID_BEAM_PY_VERSIONS_SHORT = [
    "".join(p.split(".")) for p in VALID_BEAM_PY_VERSIONS
]
DEFAULTS = {
    "region": "europe-west1",
    "experiments": "beam_fn_api",
    "num_workers": 2,
    "max_num_workers": 2,
    "autoscaling_algorithm": "NONE",
    "disk_size_gb": 32,
    "worker_machine_type": "n1-standard-2",
    "python_version": "3.6",
    "use_fnapi": True,
    "create_resources": False,
}


class CreateJob(object):

    BASE_TOPIC_TPL = "projects/{gcp_project}/topics/{job_name}"
    WORKER_IMAGE_TPL = "gcr.io/{gcp_project}/{job_name}-worker"

    def _validate_worker_image(self, value):
        # TODO: add validation that the image string is in the format of
        #       gcr.io/<project>/<name>(:<version>)  (@lynn)
        return

    def _validate_region(self, region):
        if region not in GCP_REGIONS:
            regions = ", ".join(GCP_REGIONS)
            msg = '"{0}" is not a valid region. Available: {1}'.format(
                region, regions
            )
            raise click.BadParameter(msg)
        return region

    def _parse_python_version(self, python_version):
        if python_version.startswith("2"):
            msg = (
                "Klio no longer supports Python 2.7. "
                "Supported versions: {}".format(
                    ", ".join(VALID_BEAM_PY_VERSIONS)
                )
            )
            raise click.BadParameter(msg)

        invalid_err_msg = (
            "Invalid Python version given: '{}'. Valid Python versions: "
            "{}".format(python_version, ", ".join(VALID_BEAM_PY_VERSIONS))
        )

        # valid examples: 35, 3.5, 3.5.6
        if len(python_version) < 2 or len(python_version) > 5:
            raise click.BadParameter(invalid_err_msg)

        # 3.x -> 3x; 3.x.y -> 3x
        if "." in python_version:
            python_version = "".join(python_version.split(".")[:2])

        if python_version not in VALID_BEAM_PY_VERSIONS_SHORT:
            raise click.BadParameter(invalid_err_msg)

        # Dataflow's 3.5 image is just "python3" while 3.6 and 3.7 are
        # "python36" and "python37"
        if python_version == "35":
            python_version = "3"
        return python_version

    def _get_context_from_defaults(self, kwargs):
        def _get_worker_image():
            create_dockerfile = True
            image = self.WORKER_IMAGE_TPL.format(**kwargs)
            if kwargs.get("worker_image"):
                create_dockerfile = False
                image = kwargs["worker_image"]
            return image, create_dockerfile

        # TODO: increase abstraction - we generate some defaults more than once
        #       throughout this `job create` flow. We can probably abstract
        #       it better and write more DRY code (@lynn)
        tmp_default = "gs://{gcp_project}-dataflow-tmp/{job_name}".format(
            **kwargs
        )
        staging_default = tmp_default + "/staging"
        temp_default = tmp_default + "/temp"
        worker_image, create_dockerfile = _get_worker_image()

        if "use_fnapi" in kwargs:
            use_fnapi = kwargs.get("use_fnapi")
            if use_fnapi:
                use_fnapi = str(use_fnapi).lower() in ("y", "true", "yes")
            else:  # then it was used as a flag
                use_fnapi = True
        else:
            use_fnapi = DEFAULTS["use_fnapi"]

        create_resources = self._get_create_resources(kwargs)
        default_exps = DEFAULTS["experiments"].split(",")
        if not use_fnapi:
            default_exps = [e for e in default_exps if e != "beam_fn_api"]
        experiments = kwargs.get("experiments")
        if not experiments:
            experiments = default_exps
        else:
            experiments = experiments.split(",")

        pipeline_context = {
            "worker_harness_container_image": worker_image,
            "experiments": experiments,
            "project": kwargs["gcp_project"],
            "region": kwargs.get("region", DEFAULTS["region"]),
            "staging_location": kwargs.get(
                "staging_location", staging_default
            ),
            "temp_location": kwargs.get("temp_location", temp_default),
            "num_workers": kwargs.get("num_workers", DEFAULTS["num_workers"]),
            "max_num_workers": kwargs.get(
                "max_num_workers", DEFAULTS["max_num_workers"]
            ),
            "autoscaling_algorithm": kwargs.get(
                "autoscaling_algorithm", DEFAULTS["autoscaling_algorithm"]
            ),
            "disk_size_gb": kwargs.get(
                "disk_size_gb", DEFAULTS["disk_size_gb"]
            ),
            "worker_machine_type": kwargs.get(
                "worker_machine_type", DEFAULTS["worker_machine_type"]
            ),
        }

        base_topic = self.BASE_TOPIC_TPL.format(**kwargs)
        output_topic = kwargs.get("output_topic")
        if not output_topic:
            output_topic = base_topic + "-output"
        output_location = kwargs.get("output_data_location")
        if not output_location:
            output_location = "gs://{gcp_project}-output/{job_name}".format(
                **kwargs
            )
        input_topic = kwargs.get("input_topic")
        if not input_topic:
            input_topic = base_topic + "-input"

        input_data_location = kwargs.get("input_data_location")
        if not input_data_location:
            input_data_location = "gs://{gcp_project}-input/{job_name}".format(
                **kwargs
            )

        match = TOPIC_REGEX.match(input_topic)
        topic_name = match.group("topic")
        default_sub = (
            "projects/{gcp_project}/subscriptions/{topic_name}-{job_name}"
        )
        default_sub = default_sub.format(topic_name=topic_name, **kwargs)
        inputs = [
            {
                "topic": input_topic,
                "subscription": default_sub,
                "data_location": input_data_location,
            }
        ]

        outputs = [{"topic": output_topic, "data_location": output_location}]

        job_context = {
            "inputs": inputs,
            "outputs": outputs,
        }

        python_version = kwargs.get(
            "python_version", DEFAULTS["python_version"]
        )
        python_version = self._parse_python_version(python_version)

        context = {
            "pipeline_options": pipeline_context,
            "job_options": job_context,
            "python_version": python_version,
            "use_fnapi": use_fnapi,
            "create_resources": create_resources,
        }

        return context, create_dockerfile

    def _get_create_resources(self, kwargs, user_input=False):
        if "create_resources" in kwargs:
            create_resources = kwargs.get("create_resources")
            if create_resources:
                create_resources = str(create_resources).lower() in (
                    "y",
                    "true",
                    "yes",
                )
            else:  # then it was used as a flag
                create_resources = True
        else:
            if user_input:
                create_resources = click.prompt(
                    "Create topics, buckets, and dashboards? [Y/n]",
                    type=click.Choice(["y", "Y", "n", "N"]),
                    default="n"
                    if DEFAULTS["create_resources"] is False
                    else "y",
                    show_choices=False,
                    show_default=False,  # shown in prompt
                )
                create_resources = create_resources.lower() == "y"
            else:
                create_resources = DEFAULTS["create_resources"]

        return create_resources

    def _create_args_from_user_prompt(self, command_line_args):
        """Prompts for anything not provided on the command line

        Args:
            command_line_args(dict): Command line args
        Returns:
             A CreateJobArgs instance containing for each field
             the command line argument, if provided
             the user prompted input, if provided
             the default value, otherwise
        """
        create_job_args = create_args.CreateJobArgs(
            gcp_project=command_line_args.get("gcp_project"),
            job_name=command_line_args.get("job_name"),
        )

        create_job_args.region = command_line_args.get(
            "region"
        ) or click.prompt("Desired GCP Region", default=create_job_args.region)

        create_job_args.use_fnapi = command_line_args.get(
            "use_fnapi"
        ) or click.prompt(
            "Use Apache Beam's FnAPI (experimental) [Y/n]",
            type=click.Choice(["y", "Y", "n", "N"]),
            default="y" if create_job_args.use_fnapi is True else "n",
            show_choices=False,
            show_default=False,  # shown in prompt
        )

        create_job_args.create_resources = command_line_args.get(
            "create_resources"
        ) or click.prompt(
            "Create topics, buckets, and dashboards? [Y/n]",
            type=click.Choice(["y", "Y", "n", "N"]),
            default="n" if create_job_args.create_resources is False else "y",
            show_choices=False,
            show_default=False,  # shown in prompt
        )
        create_job_args.experiments = command_line_args.get(
            "experiments"
        ) or click.prompt(
            "Beam experiments to enable",
            default=",".join(create_job_args.experiments),
        )
        create_job_args.num_workers = command_line_args.get(
            "num_workers"
        ) or click.prompt(
            "Number of workers to run",
            type=int,
            default=create_job_args.num_workers,
        )
        create_job_args.max_num_workers = command_line_args.get(
            "max_num_workers"
        ) or click.prompt(
            "Maximum number of workers to run",
            type=int,
            default=create_job_args.max_num_workers,
        )
        create_job_args.autoscaling_algorithm = command_line_args.get(
            "autoscaling_algorithm"
        ) or click.prompt(
            "Autoscaling algorithm to use. "
            "Can be NONE (default) or THROUGHPUT_BASED",
            type=str,
            default=create_job_args.autoscaling_algorithm,
        )
        create_job_args.disk_size_gb = command_line_args.get(
            "disk_size_gb"
        ) or click.prompt(
            "Size of a worker disk (GB)",
            type=int,
            default=create_job_args.disk_size_gb,
        )
        create_job_args.worker_machine_type = command_line_args.get(
            "machine_type"
        ) or click.prompt(
            "Type of GCP instance for the worker machine(s)",
            default=create_job_args.worker_machine_type,
        )
        worker_image = command_line_args.get("worker_image") or click.prompt(
            (
                "Docker image to use for the worker."
                "If none, a Dockerfile will"
                " be created for you"
            ),
            default="",
        )
        if not worker_image:
            create_job_args.python_version = command_line_args.get(
                "python_version"
            ) or click.prompt(
                "Python major version ({})".format(
                    ", ".join(VALID_BEAM_PY_VERSIONS)
                ),
                default=create_job_args.python_version,
            )
        # post-refactor change in behavior: worker_image
        # is now set to the empty string if the user does not supply it
        # rather than being filled in with the default worker image value
        # this makes it possible to use this value later to decide
        # if a Dockerfile is created or not
        create_job_args.worker_image = worker_image

        create_job_args.staging_location = command_line_args.get(
            "staging_location"
        ) or click.prompt(
            "Staging environment location",
            default=create_job_args.staging_location,
        )
        create_job_args.temp_location = command_line_args.get(
            "temp_location"
        ) or click.prompt(
            "Temporary environment location",
            default=create_job_args.temp_location,
        )

        create_job_args.input_topic = command_line_args.get(
            "input_topic"
        ) or click.prompt(
            "Input topic (usually your dependency's output topic)",
            default=create_job_args.input_topic,
        )

        create_job_args.output_topic = command_line_args.get(
            "output_topic"
        ) or click.prompt("Output topic", default=create_job_args.output_topic)

        create_job_args.input_data_location = command_line_args.get(
            "input_data_location"
        ) or click.prompt(
            (
                "Location of job's input data (usually the location of your "
                "dependency's output data)"
            ),
            default=create_job_args.input_data_location,
        )
        create_job_args.output_data_location = command_line_args.get(
            "output_data_location"
        ) or click.prompt(
            "Location of job's output",
            default=create_job_args.output_data_location,
        )
        match = TOPIC_REGEX.match(create_job_args.input_topic)
        topic_name = match.group("topic")
        SUB_TPL = (
            "projects/{gcp_project}/subscriptions/{topic_name}-{job_name}"
        )
        create_job_args.subscription = SUB_TPL.format(
            topic_name=topic_name,
            gcp_project=create_job_args.gcp_project,
            job_name=create_job_args.job_name,
        )

        return create_job_args

    def _get_context_from_user_inputs(self, kwargs):
        region = kwargs.get("region") or click.prompt(
            "Desired GCP Region", default=DEFAULTS["region"]
        )
        self._validate_region(region)

        if "use_fnapi" in kwargs:
            use_fnapi = kwargs.get("use_fnapi")
            if use_fnapi:
                use_fnapi = str(use_fnapi).lower() in ("y", "true", "yes")
            else:  # then it was used as a flag
                use_fnapi = True

        else:
            use_fnapi = click.prompt(
                "Use Apache Beam's FnAPI (experimental) [Y/n]",
                type=click.Choice(["y", "Y", "n", "N"]),
                default="y" if DEFAULTS["use_fnapi"] is True else "n",
                show_choices=False,
                show_default=False,  # shown in prompt
            )
            use_fnapi = use_fnapi.lower() == "y"

        create_resources = self._get_create_resources(kwargs, user_input=True)

        # TODO should this even be an option? run-job will break if so.
        # TODO: figure out if we should expose `experiments` to the user, or
        #       if it's okay to always assume `beam_fn_api` is the only
        #       experiment someone would ever use.
        default_exps = DEFAULTS["experiments"].split(",")
        if not use_fnapi:
            default_exps = [e for e in default_exps if e != "beam_fn_api"]

        experiments = kwargs.get("experiments")
        if experiments:
            experiments = experiments.split(",")
        else:
            experiments = click.prompt(
                "Beam experiments to enable", default=default_exps,
            )
        num_workers = kwargs.get("num_workers") or click.prompt(
            "Number of workers to run",
            type=int,
            default=DEFAULTS["num_workers"],
        )
        max_workers = kwargs.get("max_num_workers") or click.prompt(
            "Maximum number of workers to run",
            type=int,
            default=DEFAULTS["max_num_workers"],
        )
        autoscaling_algorithm = kwargs.get(
            "autoscaling_algorithm"
        ) or click.prompt(
            "Autoscaling algorithm to use. "
            "Can be NONE (default) or THROUGHPUT_BASED",
            type=str,
            default=DEFAULTS["autoscaling_algorithm"],
        )
        disk_size = kwargs.get("disk_size_gb") or click.prompt(
            "Size of a worker disk (GB)",
            type=int,
            default=DEFAULTS["disk_size_gb"],
        )
        machine_type = kwargs.get("machine_type") or click.prompt(
            "Type of GCP instance for the worker machine(s)",
            default=DEFAULTS["worker_machine_type"],
        )

        # TODO: remove support for providing a dockerfile and docker image
        #       specifically for a job. But be able to provide the ability to
        #       choose between py2 or py3 base fnapi image. (@lynn)
        create_dockerfile = False
        worker_image = kwargs.get("worker_image")
        python_version = DEFAULTS["python_version"]
        if not worker_image:
            worker_image = click.prompt(
                (
                    "Docker image to use for the worker."
                    "If none, a Dockerfile will"
                    " be created for you"
                ),
                default="",
            )
            # FYI: for some reason, coverage doesn't detect that this branch
            #      is indeed covered; force skipping for now
            if not worker_image:  # pragma: no cover
                worker_image = self.WORKER_IMAGE_TPL.format(**kwargs)
                python_version = kwargs.get("python_version") or click.prompt(
                    "Python major version ({})".format(
                        ", ".join(VALID_BEAM_PY_VERSIONS)
                    ),
                    default=DEFAULTS["python_version"],
                )
                create_dockerfile = True

        python_version = self._parse_python_version(python_version)
        self._validate_worker_image(worker_image)

        tmp_default = "gs://{gcp_project}-dataflow-tmp/{job_name}".format(
            **kwargs
        )
        staging_default = tmp_default + "/staging"
        temp_default = tmp_default + "/temp"
        staging = kwargs.get("staging_location") or click.prompt(
            "Staging environment location", default=staging_default
        )
        temp = kwargs.get("temp_location") or click.prompt(
            "Temporary environment location", default=temp_default
        )

        pipeline_context = {
            "worker_harness_container_image": worker_image,
            "experiments": experiments,
            "region": region,
            "staging_location": staging,
            "temp_location": temp,
            "num_workers": num_workers,
            "max_num_workers": max_workers,
            "autoscaling_algorithm": autoscaling_algorithm,
            "disk_size_gb": disk_size,
            "worker_machine_type": machine_type,
        }

        base_topic = self.BASE_TOPIC_TPL.format(**kwargs)
        default_input_topic = base_topic + "-input"
        default_output_topic = base_topic + "-output"

        input_topic = kwargs.get("input_topic") or click.prompt(
            "Input topic (usually your dependency's output topic)",
            default=default_input_topic,
        )

        output_topic = kwargs.get("output_topic") or click.prompt(
            "Output topic", default=default_output_topic
        )

        default_input_loc = "gs://{gcp_project}-input/{job_name}".format(
            **kwargs
        )
        input_location = kwargs.get("input_data_location") or click.prompt(
            (
                "Location of job's input data (usually the location of your "
                "dependency's output data)"
            ),
            default=default_input_loc,
        )

        default_output_loc = "gs://{gcp_project}-output/{job_name}".format(
            **kwargs
        )
        output_location = kwargs.get("output_data_location") or click.prompt(
            "Location of job's output", default=default_output_loc
        )

        match = TOPIC_REGEX.match(input_topic)
        topic_name = match.group("topic")
        default_sub = (
            "projects/{gcp_project}/subscriptions/{topic_name}-{job_name}"
        )
        default_sub = default_sub.format(topic_name=topic_name, **kwargs)
        inputs = [
            {
                "topic": input_topic,
                "subscription": default_sub,
                "data_location": input_location,
            }
        ]
        outputs = [{"topic": output_topic, "data_location": output_location}]

        job_context = {
            "inputs": inputs,
            "outputs": outputs,
        }

        context = {
            "pipeline_options": pipeline_context,
            "job_options": job_context,
            "python_version": python_version,
            "use_fnapi": use_fnapi,
            "create_resources": create_resources,
        }
        return context, create_dockerfile

    def _get_user_input(self, kwargs):
        accept_defaults = kwargs["use_defaults"]
        if accept_defaults:
            context, create_dockerfile = self._get_context_from_defaults(
                kwargs
            )
        else:
            context, create_dockerfile = self._get_context_from_user_inputs(
                kwargs
            )

        context["job_name"] = kwargs["job_name"]
        context["pipeline_options"]["project"] = kwargs["gcp_project"]
        return context, create_dockerfile

    def _parse_unknown_args(self, user_args):
        # ('--foo', 'bar', '--baz', 'bla', 'qaz')
        parsed_keys_index = []  # (0, 2)
        parsed_args = {}
        for index, item in enumerate(user_args):
            if item.startswith("--"):
                parsed_keys_index.append(index)

        for index, item in enumerate(parsed_keys_index):
            key = user_args[item].lstrip("--").replace("-", "_")
            try:
                next_key = parsed_keys_index[index + 1]
            except IndexError:
                next_key = len(user_args)

            values = user_args[item + 1 : next_key]
            if len(values) == 1:
                values = values[0]

            parsed_args[key] = values

        return parsed_args

    def _create_external_resources(self, context):
        if context["create_resources"]:
            gcp_setup.create_topics_and_buckets(context)
            gcp_setup.create_stackdriver_dashboard(context)

    def create(self, unknown_args, known_kwargs, output_dir):
        unknown_args = self._parse_unknown_args(unknown_args)

        def merge_two_dicts(x, y):
            """Given two dicts, merge them into a new dict as a shallow copy."""
            z = x.copy()
            z.update(y)
            return z

        all_kwargs = merge_two_dicts(unknown_args, known_kwargs)

        context, create_dockerfile = self._get_user_input(all_kwargs)

        self._create_external_resources(context)

        template_renderer = rendering.CreateJobTemplateRenderer()
        env = template_renderer.get_environment()
        job_name = context["job_name"]
        package_name = job_name.replace("-", "_")
        template_renderer.create_job_directory(output_dir)
        template_renderer.create_job_config(env, context, output_dir)

        template_renderer.create_python_files(env, package_name, output_dir)
        if not context["use_fnapi"]:
            context["package_name"] = package_name
            template_renderer.create_no_fnapi_files(env, context, output_dir)
        template_renderer.create_reqs_file(env, context, output_dir)
        if create_dockerfile:
            template_renderer.create_dockerfile(env, context, output_dir)
        template_renderer.create_readme(env, context, output_dir)

        msg = "Klio job {} created successfully! :beer:".format(job_name)
        logging.info(emoji.emojize(msg, use_aliases=True))
