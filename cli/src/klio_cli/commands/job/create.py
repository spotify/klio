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
    def _create_context_from_create_job_args(self, create_job_args):
        """Create Jinja template context object from CreateJobArgs

        TODO remove this method after switching Jimja templates to using
        dict from create_job_args.asdict()
        """
        pipeline_context = {
            "project": create_job_args.gcp_project,
            "worker_harness_container_image": create_job_args.worker_image,
            "experiments": create_job_args.experiments,
            "region": create_job_args.region,
            "staging_location": create_job_args.staging_location,
            "temp_location": create_job_args.temp_location,
            "num_workers": create_job_args.num_workers,
            "max_num_workers": create_job_args.max_num_workers,
            "autoscaling_algorithm": create_job_args.autoscaling_algorithm,
            "disk_size_gb": create_job_args.disk_size_gb,
            "worker_machine_type": create_job_args.worker_machine_type,
        }
        inputs = [
            {
                "topic": create_job_args.input_topic,
                "subscription": create_job_args.subscription,
                "data_location": create_job_args.input_data_location,
            }
        ]

        outputs = [
            {
                "topic": create_job_args.output_topic,
                "data_location": create_job_args.output_data_location,
            }
        ]

        job_context = {
            "inputs": inputs,
            "outputs": outputs,
        }

        context = {
            "pipeline_options": pipeline_context,
            "job_options": job_context,
            "python_version": create_job_args.python_version,
            "use_fnapi": create_job_args.use_fnapi,
            "create_resources": create_job_args.create_resources,
            "job_name": create_job_args.job_name,
        }
        return context

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

    def _get_user_input(self, command_line_args):
        """Consolidates user input

        Args:
              command_line_args(dict): All CLI args
        Returns:
            Tuple of context, create_dockerfile
        """
        create_dockerfile = False
        if not command_line_args.get("use_defaults"):
            create_job_args = self._create_args_from_user_prompt(
                command_line_args
            )
            if not create_job_args.worker_image:
                create_dockerfile = True
                create_job_args.worker_image = (
                    create_job_args._default_worker_image()
                )
        else:
            create_job_args = create_args.CreateJobArgs.from_dict(
                command_line_args
            )
            if not command_line_args.get("worker_image"):
                create_dockerfile = True

        context = self._create_context_from_create_job_args(create_job_args)

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
