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

import click
import emoji
import jinja2

from klio_core import variables

from klio_cli.commands.job.utils import gcp_setup


# Valid project IDs: https://cloud.google.com/resource-manager/reference
#                    /rest/v1/projects#Project
# Valid topic names: https://cloud.google.com/pubsub/docs/admin#resource_names
# Will save matches into symbolic groups named "project" and "topic"
TOPIC_REGEX = re.compile(
    r"^projects/(?P<project>[a-z0-9-]{6,30})/"
    r"topics/(?P<topic>[a-zA-Z0-9-_.~+%]{3,255})"
)
VALID_BEAM_PY_VERSIONS = ["3.6", "3.7", "3.8"]

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
    "job_type": "streaming",
}


class CreateJob(object):

    BASE_TOPIC_TPL = "projects/{gcp_project}/topics/{job_name}"
    WORKER_IMAGE_TPL = "gcr.io/{gcp_project}/{job_name}-worker"

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
        if context.get("job_type") == "batch":
            config_tpl = env.get_template("klio-job-batch.yaml.tpl")
        else:
            config_tpl = env.get_template("klio-job.yaml.tpl")
        template_context = {"klio": context}
        rendered_file = config_tpl.render(template_context)
        self._write_template(job_dir, "klio-job.yaml", rendered_file)

    def _create_python_files(self, env, package_name, job_type, output_dir):
        current_year = datetime.datetime.now().year
        template_context = {
            "klio": {"year": current_year, "package_name": package_name}
        }

        init_tpl = env.get_template("init.py.tpl")
        init_rendered = init_tpl.render(template_context)
        self._write_template(output_dir, "__init__.py", init_rendered)

        run_tpl = env.get_template("run.py.tpl")
        run_rendered = run_tpl.render(template_context)
        self._write_template(output_dir, "run.py", run_rendered)

        if job_type == "batch":
            transforms_tpl = env.get_template("transforms-batch.py.tpl")
        else:
            transforms_tpl = env.get_template("transforms.py.tpl")
        transforms_rendered = transforms_tpl.render(template_context)
        self._write_template(output_dir, "transforms.py", transforms_rendered)

        test_transforms_tpl = env.get_template("test_transforms.py.tpl")
        test_transforms_rendered = test_transforms_tpl.render(template_context)
        self._write_template(
            output_dir, "test_transforms.py", test_transforms_rendered
        )

    def _create_no_fnapi_files(self, env, context, output_dir):
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

    def _validate_worker_image(self, value):
        # TODO: add validation that the image string is in the format of
        #       gcr.io/<project>/<name>(:<version>)  (@lynn)
        return

    def _validate_region(self, region):
        if region not in variables.DATAFLOW_REGIONS:
            regions = ", ".join(variables.DATAFLOW_REGIONS)
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

        # valid examples: 36, 3.6, 3.6.6
        if len(python_version) < 2 or len(python_version) > 5:
            raise click.BadParameter(invalid_err_msg)

        # keep only the major and minor version information
        python_version = ".".join(python_version.split(".")[:2])

        if python_version not in VALID_BEAM_PY_VERSIONS:
            raise click.BadParameter(invalid_err_msg)

        return python_version

    def _get_default_streaming_job_context(self, kwargs):
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
        dependencies = kwargs.get("dependencies", [])

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
            "dependencies": dependencies,
        }

        return job_context

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

        job_type = kwargs.get("job_type", DEFAULTS["job_type"])
        if job_type == "batch":
            job_context = self._get_default_batch_job_context(kwargs)
        else:
            job_context = self._get_default_streaming_job_context(kwargs)

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
            "job_type": job_type,
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
                    "Create relevant resources in GCP? [Y/n]",
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

    def _get_dependencies_from_user_inputs(self):
        dependencies = []
        while True:
            job = {}
            job_name = click.prompt("Dependency job name")
            job["job_name"] = job_name
            job["gcp_project"] = click.prompt(
                "GCP project where '{}' is located".format(job_name)
            )
            input_topic = click.prompt(
                "Input topic of '{}'".format(job_name), default=""
            )
            if input_topic:
                job["input_topics"] = [input_topic]

            region = click.prompt(
                "GCP region where '{}' is located".format(job_name), default=""
            )
            if region:
                self._validate_region(region)
                job["region"] = region

            dependencies.append(job)

            if not click.confirm("Do you have another dependency?"):
                break

        return dependencies

    def _get_context_from_user_inputs(self, kwargs):
        job_type = kwargs.get("job_type") or click.prompt(
            "Job Type",
            type=click.Choice(["batch", "streaming"]),
            default=DEFAULTS["job_type"],
        )
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

        if job_type == "batch":
            job_context = self._get_batch_user_input_job_context(kwargs)
        else:
            job_context = self._get_streaming_user_input(kwargs)

        context = {
            "pipeline_options": pipeline_context,
            "job_options": job_context,
            "python_version": python_version,
            "use_fnapi": use_fnapi,
            "create_resources": create_resources,
            "job_type": job_type,
        }
        return context, create_dockerfile

    def _get_default_batch_job_context(self, kwargs):
        default_batch_event_input = "{}_input_elements.txt".format(
            kwargs.get("job_name")
        )
        default_batch_data_input = "{}-input".format(kwargs.get("job_name"))
        default_batch_event_output = "{}_output_elements".format(
            kwargs.get("job_name")
        )
        default_batch_data_output = "{}-output".format(kwargs.get("job_name"))

        job_context = {
            "inputs": [
                {
                    "event_location": default_batch_event_input,
                    "data_location": default_batch_data_input,
                }
            ],
            "outputs": [
                {
                    "event_location": default_batch_event_output,
                    "data_location": default_batch_data_output,
                }
            ],
        }
        return job_context

    def _get_batch_user_input_job_context(self, kwargs):
        default_batch_event_input = "{}_input_ids.txt".format(
            kwargs.get("job_name")
        )
        batch_event_input = kwargs.get("batch_event_input")
        if not batch_event_input:
            batch_event_input = click.prompt(
                "Batch event input file", default=default_batch_event_input
            )

        default_batch_data_input = "{}-input".format(kwargs.get("job_name"))
        batch_data_input = kwargs.get("batch_data_input")
        if not batch_data_input:
            batch_data_input = click.prompt(
                "Batch data input directory", default=default_batch_data_input
            )

        default_batch_event_output = "{}_output_elements".format(
            kwargs.get("job_name")
        )
        batch_event_output = kwargs.get("batch_event_output")
        if not batch_event_output:
            batch_event_output = click.prompt(
                "Batch event output file", default=default_batch_event_output
            )

        default_batch_data_output = "{}-output".format(kwargs.get("job_name"))
        batch_data_output = kwargs.get("batch_data_output")
        if not batch_data_output:
            batch_data_output = click.prompt(
                "Batch data output directory",
                default=default_batch_data_output,
            )

        job_context = {
            "inputs": [
                {
                    "event_location": batch_event_input,
                    "data_location": batch_data_input,
                }
            ],
            "outputs": [
                {
                    "event_location": batch_event_output,
                    "data_location": batch_data_output,
                }
            ],
        }
        return job_context

    def _get_streaming_user_input(self, kwargs):
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
            "dependencies": [],
        }

        dependencies = kwargs.get("dependencies")
        if not dependencies:
            if click.confirm("Does your job have dependencies on other jobs?"):
                dependencies = self._get_dependencies_from_user_inputs()

        if dependencies is not None:
            job_context["dependencies"] = dependencies

        return job_context

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

    def _parse_dependency_args(self, dependency):
        # both "_" and "-" are valid
        valid_keys = (
            "job_name",
            "gcp_project",
            "region",
            "input_topics",
            "input_topic",
        )
        job_dependency = {}

        for item in dependency:
            key, value = item.split("=")
            clean_key = key.replace("-", "_")
            if clean_key in valid_keys:
                if clean_key == "input_topics":
                    value = value.split(",")
                job_dependency[clean_key] = value
            else:
                logging.warning(
                    "Skipping unrecognized job dependency key: '{}'.".format(
                        key
                    )
                )
        return job_dependency

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
            if len(values) == 1 and key != "dependency":
                values = values[0]

            if key == "dependency":
                dependency = self._parse_dependency_args(values)
                if len(dependency) == 0:
                    continue
                parsed_args.setdefault("dependencies", []).append(dependency)
            else:
                parsed_args[key] = values

        return parsed_args

    def _create_external_resources(self, context):
        if context["create_resources"]:
            if context["job_type"] == "streaming":
                gcp_setup.create_topics(context)
            gcp_setup.create_buckets(context)
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

        env = self._get_environment()
        job_name = context["job_name"]
        package_name = job_name.replace("-", "_")
        self._create_job_directory(output_dir)
        self._create_job_config(env, context, output_dir)

        job_type = context["job_type"]
        self._create_python_files(env, package_name, job_type, output_dir)
        if not context["use_fnapi"]:
            context["package_name"] = package_name
            self._create_no_fnapi_files(env, context, output_dir)
        self._create_reqs_file(env, context, output_dir)
        if create_dockerfile:
            self._create_dockerfile(env, context, output_dir)
        self._create_readme(env, context, output_dir)

        msg = "Klio job {} created successfully! :tada:".format(job_name)
        logging.info(emoji.emojize(msg, use_aliases=True))
