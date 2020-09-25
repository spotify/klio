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

import datetime
import io
import logging
import os
import re

import attr
import click
import emoji
import jinja2

from klio_cli.commands.job.utils import create_args
from klio_cli.commands.job.utils import gcp_setup


# Valid project IDs: https://cloud.google.com/resource-manager/reference
#                    /rest/v1/projects#Project
# Valid topic names: https://cloud.google.com/pubsub/docs/admin#resource_names
# Will save matches into symbolic groups named "project" and "topic"
TOPIC_REGEX = re.compile(
    r"^projects/(?P<project>[a-z0-9-]{6,30})/"
    r"topics/(?P<topic>[a-zA-Z0-9-_.~+%]{3,255})"
)

DEFAULTS = {
    "region": "europe-west1",
    "experiments": "beam_fn_api",
    "num_workers": 2,
    "max_num_workers": 2,
    "autoscaling_algorithm": "NONE",
    "disk_size_gb": 32,
    "worker_machine_type": "n1-standard-2",
    "python_version": "3.6",
    "use_fnapi": False,
    "create_resources": False,
}


class CreateJob(object):
    WORKER_IMAGE_TPL = "gcr.io/{gcp_project}/{job_name}-worker"

    def __init__(self):
        self.create_args_dict = {
            f: None for f in attr.fields_dict(create_args.CreateJobArgs)
        }

    def create(self, unknown_args, known_kwargs):
        # Build single source of truth for job creation arguments
        # (stored in self.create_args) by
        # consolidating information from command line, user
        # inputs and predetermined default values.
        # Values will be normalized and validated.
        self.build_create_job_args(known_kwargs, unknown_args)

        # build jinja context
        self.context = self._get_context()
        env = self._get_environment()

        # create klio job files
        self._create_job_directory(self.create_args.job_dir)
        self._create_job_config(env, self.context, self.create_args.job_dir)
        self._create_python_files(env, self.create_args.job_dir)
        if not self.create_args.use_fnapi:
            self._create_no_fnapi_files(
                env, self.context, self.create_args.job_dir
            )
        self._create_reqs_file(env, self.context, self.create_args.job_dir)
        if self.create_args.create_dockerfile:
            self._create_dockerfile(
                env, self.context, self.create_args.job_dir
            )
        self._create_readme(env, self.context, self.create_args.job_dir)

    def _get_context(self):
        pipeline_context = {
            "project": self.create_args.gcp_project,
            "worker_harness_container_image": self.create_args.worker_image,
            "experiments": self.create_args.experiments,
            "region": self.create_args.region,
            "staging_location": self.create_args.staging_location,
            "temp_location": self.create_args.temp_location,
            "num_workers": self.create_args.num_workers,
            "max_num_workers": self.create_args.max_num_workers,
            "autoscaling_algorithm": self.create_args.autoscaling_algorithm,
            "disk_size_gb": self.create_args.disk_size_gb,
            "worker_machine_type": self.create_args.worker_machine_type,
        }

        context = {
            "pipeline_options": pipeline_context,
            "python_version": self.create_args.python_version,
            "use_fnapi": self.create_args.use_fnapi,
            "create_resources": self.create_args.create_resources,
            "job_name": self.create_args.job_name,
        }
        return context

    def _get_environment(self):
        here = os.path.abspath(__file__)
        here_dir = os.path.join(here, os.path.pardir)
        here_dir = os.path.abspath(here_dir)
        template_dir = os.path.join(here_dir, "utils", "templates")
        return jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir), autoescape=False
        )

    # TODO: add a boolean flag to cli.create_job command to force-recreate
    #       if the particular job directory already exists. This would
    #       essentially overwrite all the klio-generated files. (@lynn)
    def _create_job_directory(self, output_dir):
        try:
            os.mkdir(output_dir)
        except OSError as e:
            if e.errno != 17:  # file already exists
                raise

    def _write_template(self, output_dir, output_file, data):
        output = os.path.abspath(os.path.join(output_dir, output_file))
        # TODO: wrap the writing of a file in a `try`/`except`. If there is
        #       an error in writing a file, raise a user-friendly exception.
        #       This might leave the job-generation in a weird state (i.e.
        #       some files created). We should decide what we want to do, i.e.
        #       leave our partially-created mess there, or clean up after
        #       ourselves. (@lynn)
        with io.open(output, "w", encoding="utf-8") as f:
            f.write(data)

        logging.debug("Created {0}".format(output))

    def _create_job_config(self, env, context, job_dir):
        config_tpl = env.get_template(
            "klio-job-{}.yaml.tpl".format(self.create_args.job_type)
        )

        template_context = {"klio": context}
        rendered_file = config_tpl.render(template_context)
        self._write_template(job_dir, "klio-job.yaml", rendered_file)

    def _create_python_files(self, env, output_dir):
        current_year = datetime.datetime.now().year
        template_context = {"klio": {"year": current_year}}

        init_tpl = env.get_template("init.py.tpl")
        init_rendered = init_tpl.render(template_context)
        self._write_template(output_dir, "__init__.py", init_rendered)

        run_tpl = env.get_template("run.py.tpl")
        run_rendered = run_tpl.render(template_context)
        self._write_template(output_dir, "run.py", run_rendered)

        transforms_tpl = env.get_template("transforms.py.tpl")
        transforms_rendered = transforms_tpl.render(template_context)
        self._write_template(output_dir, "transforms.py", transforms_rendered)

        test_transforms_tpl = env.get_template("test_transforms.py.tpl")
        test_transforms_rendered = test_transforms_tpl.render(template_context)
        self._write_template(
            output_dir, "test_transforms.py", test_transforms_rendered
        )

    def _create_no_fnapi_files(self, env, context, output_dir):
        package_name = context["job_name"].replace("-", "_")
        context["package_name"] = package_name

        template_context = {"klio": context}

        setup_tpl = env.get_template("setup.py.tpl")
        setup_rendered = setup_tpl.render(template_context)
        self._write_template(output_dir, "setup.py", setup_rendered)

        manifest_tpl = env.get_template("MANIFEST.in.tpl")
        manifest_rendered = manifest_tpl.render(template_context)
        self._write_template(output_dir, "MANIFEST.in", manifest_rendered)

    def _create_reqs_file(self, env, context, output_dir):
        # the reqs file no longer has template context needed to be filled in,
        # but leaving this here in case we do change our minds (i.e. explicitly
        # including a klio package)
        template_context = {"klio": context}
        reqs_tpl = env.get_template("job-requirements.txt.tpl")
        reqs_rendered = reqs_tpl.render(template_context)
        self._write_template(output_dir, "job-requirements.txt", reqs_rendered)

    def _create_dockerfile(self, env, context, output_dir):
        template_context = {"klio": context}

        dockerfile_tpl = env.get_template("dockerfile.tpl")
        dockerfile_rendered = dockerfile_tpl.render(template_context)
        self._write_template(output_dir, "Dockerfile", dockerfile_rendered)

    def _create_readme(self, env, context, output_dir):
        template_context = {"klio": context}

        setup_inst_tpl = env.get_template("README.md.tpl")
        setup_inst_rendered = setup_inst_tpl.render(template_context)
        self._write_template(output_dir, "README.md", setup_inst_rendered)

    def _parse_cli_args(self, known_args, addl_job_args):
        updated_fields = {}

        for create_arg in self.create_args_dict:
            if create_arg in known_args:
                updated_fields[create_arg] = known_args.get(create_arg)
            if create_arg in addl_job_args:
                updated_fields[create_arg] = addl_job_args.get(create_arg)

        self.create_args_dict.update(updated_fields)

    def __parse_use_fnapi_user_input(self, updated_fields):
        use_fnapi = self.create_args_dict.get("use_fnapi")
        if isinstance(use_fnapi, str):
            updated_fields["use_fnapi"] = str(use_fnapi).lower() in (
                "y",
                "true",
                "yes",
            )
        if use_fnapi is None:
            use_fnapi = click.prompt(
                "Use Apache Beam's FnAPI (experimental) [Y/n]",
                type=click.Choice(["y", "Y", "n", "N"]),
                default="y" if DEFAULTS["use_fnapi"] is True else "n",
                show_choices=False,
                show_default=False,  # shown in prompt
            )
            updated_fields["use_fnapi"] = use_fnapi.lower() == "y"

    def __parse_created_resources_user_input(self, updated_fields):
        if self.create_args_dict.get("create_resources"):
            create_resources = self.create_args_dict.get("create_resources")
            if create_resources:
                updated_fields["create_resources"] = str(
                    create_resources
                ).lower() in ("y", "true", "yes",)
            else:  # then it was used as a flag
                updated_fields["create_resources"] = True
        else:
            create_resources = click.prompt(
                "Create topics, buckets, and dashboards? [Y/n]",
                type=click.Choice(["y", "Y", "n", "N"]),
                default="n" if DEFAULTS["create_resources"] is False else "y",
                show_choices=False,
                show_default=False,  # shown in prompt
            )
            updated_fields["create_resources"] = (
                create_resources.lower() == "y"
            )

    def __parse_python_version_user_input(self, updated_fields):
        if not self.create_args_dict.get("worker_image"):  # pragma: no cover
            python_version = self.create_args_dict.get(
                "python_version"
            ) or click.prompt(
                "Python major version ({})".format(
                    ", ".join(create_args.VALID_BEAM_PY_VERSIONS)
                ),
                default=DEFAULTS["python_version"],
            )
            updated_fields["python_version"] = python_version

    def __parse_worker_image_user_input(self, updated_fields):
        # TODO: remove support for providing a dockerfile and docker image
        #       specifically for a job. But be able to provide the ability to
        #       choose between py2 or py3 base fnapi image. (@lynn)
        if not self.create_args_dict.get("worker_image"):
            worker_image = click.prompt(
                (
                    "Docker image to use for the worker."
                    "If none, a Dockerfile will"
                    " be created for you"
                ),
                default="",
            )

            if worker_image:
                updated_fields["worker_image"] = worker_image

    def _parse_user_input_args(self):
        if not self.create_args_dict.get("use_defaults"):
            updated_fields = {}
            self.__parse_use_fnapi_user_input(updated_fields)
            self.__parse_created_resources_user_input(updated_fields)
            self.__parse_worker_image_user_input(updated_fields)
            self.__parse_python_version_user_input(updated_fields)

            self.create_args_dict.update(updated_fields)

    def _apply_default_experiments(self):
        if not self.create_args_dict.get("experiments"):
            if not self.create_args_dict.get("use_fnapi"):
                self.create_args_dict["experiments"] = [
                    e for e in self.default_exps if e != "beam_fn_api"
                ]
        else:
            if isinstance(self.create_args_dict.get("experiments"), str):
                self.create_args_dict[
                    "experiments"
                ] = self.create_args_dict.get("experiments").split(",")

    def _apply_defaults(self):
        for k in self.create_args_dict:
            if self.create_args_dict.get(k) is None and k in DEFAULTS:
                self.create_args_dict[k] = DEFAULTS[k]

        if not self.create_args_dict.get("job_dir"):
            self.create_args_dict["job_dir"] = self.default_job_dir

        self.create_args_dict["create_dockerfile"] = False
        if not self.create_args_dict.get("worker_image"):
            # worker image name not supplied by either CLI flags
            # or through user input, so we will have to
            # create one
            self.create_args_dict["create_dockerfile"] = True
            image = self.WORKER_IMAGE_TPL.format(
                gcp_project=self.create_args_dict.get("gcp_project"),
                job_name=self.create_args_dict.get("job_name"),
            )
            self.create_args_dict["worker_image"] = image

        self._apply_default_experiments()

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

    def _build_defaults(self):
        self.default_job_dir = os.getcwd()
        self.default_exps = DEFAULTS["experiments"].split(",")

    def build_create_job_args(self, known_args, addl_job_args):
        unknown_args = self._parse_unknown_args(addl_job_args)
        self._parse_cli_args(known_args, unknown_args)
        self._build_defaults()
        self._parse_user_input_args()
        self._apply_defaults()

        self.create_args = create_args.CreateJobArgs.from_dict(
            self.create_args_dict
        )


class CreateStreamingJob(CreateJob):
    BASE_TOPIC_TPL = "projects/{gcp_project}/topics/{job_name}"

    def _build_defaults(self):
        super()._build_defaults()

        default_bucket = "gs://{gcp_project}-dataflow-tmp/{job_name}".format(
            gcp_project=self.create_args_dict.get("gcp_project"),
            job_name=self.create_args_dict.get("job_name"),
        )
        self.default_staging_location = default_bucket + "/staging"
        self.default_temp_location = default_bucket + "/temp"


        base_topic = self.BASE_TOPIC_TPL.format(
            gcp_project=self.create_args_dict.get("gcp_project"),
            job_name=self.create_args_dict.get("job_name"),
        )
        self.default_input_topic = base_topic + "-input"
        self.default_output_topic = base_topic + "-output"
        self.default_input_loc = "gs://{gcp_project}-input/{job_name}".format(
            gcp_project=self.create_args_dict.get("gcp_project"),
            job_name=self.create_args_dict.get("job_name"),
        )
        self.default_output_loc = "gs://{gcp_project}-output/{job_name}".format(
            gcp_project=self.create_args_dict.get("gcp_project"),
            job_name=self.create_args_dict.get("job_name"),
        )

    def _apply_default_gcs_bucket(self):

        if not self.create_args_dict.get("staging_location"):
            self.create_args_dict[
                "staging_location"
            ] = self.default_staging_location
        if not self.create_args_dict.get("temp_location"):
            self.create_args_dict["temp_location"] = self.default_temp_location



    def _apply_default_topics_and_subscriptions(self):
        if not self.create_args_dict.get("output_topic"):
            self.create_args_dict["output_topic"] = self.default_output_topic
        if not self.create_args_dict.get("output_data_location"):
            self.create_args_dict[
                "output_data_location"
            ] = self.default_output_loc

        if not self.create_args_dict.get("input_topic"):
            self.create_args_dict["input_topic"] = self.default_input_topic
        if not self.create_args_dict.get("input_data_location"):
            self.create_args_dict[
                "input_data_location"
            ] = self.default_input_loc

        match = TOPIC_REGEX.match(self.create_args_dict.get("input_topic"))
        topic_name = match.group("topic")
        default_sub = (
            "projects/{gcp_project}/subscriptions/{topic_name}-{job_name}"
        )
        default_sub = default_sub.format(
            topic_name=topic_name,
            gcp_project=self.create_args_dict.get("gcp_project"),
            job_name=self.create_args_dict.get("job_name"),
        )
        self.create_args_dict["subscription"] = default_sub

    def _apply_defaults(self):
        super()._apply_defaults()

        self._apply_default_gcs_bucket()
        self._apply_default_topics_and_subscriptions()

    def _parse_experiments_user_input(self, updated_fields):
        # TODO should this even be an option? run-job will break if so.
        # TODO: figure out if we should expose `experiments` to the user, or
        #       if it's okay to always assume `beam_fn_api` is the only
        #       experiment someone would ever use.
        experiments = self.create_args_dict.get("experiments")
        if experiments:
            updated_fields["experiments"] = experiments.split(",")
        else:
            updated_fields["experiments"] = click.prompt(
                "Beam experiments to enable", default=self.default_exps
            )

    def _parse_worker_config_user_input(self, updated_fields):
        updated_fields["region"] = self.create_args_dict.get(
            "region"
        ) or click.prompt("Desired GCP Region", default=DEFAULTS["region"])

        updated_fields["num_workers"] = self.create_args_dict.get(
            "num_workers"
        ) or click.prompt(
            "Number of workers to run",
            type=int,
            default=DEFAULTS["num_workers"],
        )
        updated_fields["max_workers"] = self.create_args_dict.get(
            "max_num_workers"
        ) or click.prompt(
            "Maximum number of workers to run",
            type=int,
            default=DEFAULTS["max_num_workers"],
        )
        updated_fields["autoscaling_algorithm"] = self.create_args_dict.get(
            "autoscaling_algorithm"
        ) or click.prompt(
            "Autoscaling algorithm to use. "
            "Can be NONE (default) or THROUGHPUT_BASED",
            type=str,
            default=DEFAULTS["autoscaling_algorithm"],
        )
        updated_fields["disk_size"] = self.create_args_dict.get(
            "disk_size_gb"
        ) or click.prompt(
            "Size of a worker disk (GB)",
            type=int,
            default=DEFAULTS["disk_size_gb"],
        )
        updated_fields["machine_type"] = self.create_args_dict.get(
            "worker_machine_type"
        ) or click.prompt(
            "Type of GCP instance for the worker machine(s)",
            default=DEFAULTS["worker_machine_type"],
        )

    def _parse_user_input_topics_and_subs(self, updated_fields):
        updated_fields["input_topic"] = self.create_args_dict.get(
            "input_topic"
        ) or click.prompt(
            "Input topic (usually your dependency's output topic)",
            default=self.default_input_topic,
        )
        updated_fields["output_topic"] = self.create_args_dict.get(
            "output_topic"
        ) or click.prompt("Output topic", default=self.default_output_topic)

        updated_fields["input_data_location"] = self.create_args_dict.get(
            "input_data_location"
        ) or click.prompt(
            (
                "Location of job's input data (usually the location of your "
                "dependency's output data)"
            ),
            default=self.default_input_loc,
        )
        updated_fields["output_data_location"] = self.create_args_dict.get(
            "output_data_location"
        ) or click.prompt(
            "Location of job's output", default=self.default_output_loc
        )

    def _parse_default_data_locations_user_input(self, updated_fields):
        updated_fields["staging_location"] = self.create_args_dict.get(
            "staging_location"
        ) or click.prompt(
            "Staging environment location",
            default=self.default_staging_location,
        )
        updated_fields["temp_location"] = self.create_args_dict.get(
            "temp_location"
        ) or click.prompt(
            "Temporary environment location",
            default=self.default_temp_location,
        )

    def _parse_user_input_args(self):
        if not self.create_args_dict.get("use_defaults"):
            super()._parse_user_input_args()

            updated_fields = {}
            self._parse_experiments_user_input(updated_fields)
            self._parse_worker_config_user_input(updated_fields)
            self._parse_default_data_locations_user_input(updated_fields)
            self._parse_user_input_topics_and_subs(updated_fields)

            self.create_args_dict.update(updated_fields)

    def _get_context(self):
        context = super()._get_context()

        inputs = [
            {
                "topic": self.create_args.input_topic,
                "subscription": self.create_args.subscription,
                "data_location": self.create_args.input_data_location,
            }
        ]

        outputs = [
            {
                "topic": self.create_args.output_topic,
                "data_location": self.create_args.output_data_location,
            }
        ]

        context["job_options"] = {
            "inputs": inputs,
            "outputs": outputs,
        }

        return context

    def _create_external_resources(self, context):
        if context["create_resources"]:
            gcp_setup.create_topics_and_buckets(context)
            gcp_setup.create_stackdriver_dashboard(context)

    def create(self, unknown_args, known_kwargs):
        super().create(unknown_args, known_kwargs)
        self._create_external_resources(self.context)

        msg = "Streaming Klio job {} created successfully! :beer:".format(
            self.create_args.job_name
        )
        logging.info(emoji.emojize(msg, use_aliases=True))


class CreateBatchJob(CreateJob):
    def create(self, unknown_args, known_kwargs):
        super().create(unknown_args, known_kwargs)

        msg = "Batch Klio job {} created successfully! :beer:".format(
            self.create_args.job_name
        )
        logging.info(emoji.emojize(msg, use_aliases=True))
