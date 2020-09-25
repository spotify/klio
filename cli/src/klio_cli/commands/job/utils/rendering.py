# Copyright 2020 Spotify AB
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

import jinja2


class CreateJobTemplateRenderer(object):
    # TODO: add a boolean flag to cli.create_job command to force-recreate
    #       if the particular job directory already exists. This would
    #       essentially overwrite all the klio-generated files. (@lynn)
    def create_job_directory(self, output_dir):
        try:
            os.mkdir(output_dir)
        except OSError as e:
            if e.errno != 17:  # file already exists
                raise

    def get_environment(self):
        here = os.path.abspath(__file__)
        here_dir = os.path.join(here, os.path.pardir)
        here_dir = os.path.abspath(here_dir)
        template_dir = os.path.join(here_dir, "templates")
        return jinja2.Environment(
            loader=jinja2.FileSystemLoader(template_dir), autoescape=False
        )

    def write_template(self, output_dir, output_file, data):
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

    def create_job_config(self, env, context, job_dir):
        config_tpl = env.get_template("klio-job.yaml.tpl")

        template_context = {"klio": context}
        rendered_file = config_tpl.render(template_context)
        self.write_template(job_dir, "klio-job.yaml", rendered_file)

    def create_python_files(self, env, package_name, output_dir):
        current_year = datetime.datetime.now().year
        template_context = {
            "klio": {"year": current_year, "package_name": package_name}
        }

        init_tpl = env.get_template("init.py.tpl")
        init_rendered = init_tpl.render(template_context)
        self.write_template(output_dir, "__init__.py", init_rendered)

        run_tpl = env.get_template("run.py.tpl")
        run_rendered = run_tpl.render(template_context)
        self.write_template(output_dir, "run.py", run_rendered)

        transforms_tpl = env.get_template("transforms.py.tpl")
        transforms_rendered = transforms_tpl.render(template_context)
        self.write_template(output_dir, "transforms.py", transforms_rendered)

        test_transforms_tpl = env.get_template("test_transforms.py.tpl")
        test_transforms_rendered = test_transforms_tpl.render(template_context)
        self.write_template(
            output_dir, "test_transforms.py", test_transforms_rendered
        )

    def create_no_fnapi_files(self, env, context, output_dir):
        template_context = {"klio": context}

        setup_tpl = env.get_template("setup.py.tpl")
        setup_rendered = setup_tpl.render(template_context)
        self.write_template(output_dir, "setup.py", setup_rendered)

        manifest_tpl = env.get_template("MANIFEST.in.tpl")
        manifest_rendered = manifest_tpl.render(template_context)
        self.write_template(output_dir, "MANIFEST.in", manifest_rendered)

    def create_reqs_file(self, env, context, output_dir):
        # the reqs file no longer has template context needed to be filled in,
        # but leaving this here in case we do change our minds (i.e. explicitly
        # including a klio package)
        template_context = {"klio": context}
        reqs_tpl = env.get_template("job-requirements.txt.tpl")
        reqs_rendered = reqs_tpl.render(template_context)
        self.write_template(output_dir, "job-requirements.txt", reqs_rendered)

    def create_dockerfile(self, env, context, output_dir):
        template_context = {"klio": context}

        dockerfile_tpl = env.get_template("dockerfile.tpl")
        dockerfile_rendered = dockerfile_tpl.render(template_context)
        self.write_template(output_dir, "Dockerfile", dockerfile_rendered)

    def create_readme(self, env, context, output_dir):
        template_context = {"klio": context}

        setup_inst_tpl = env.get_template("README.md.tpl")
        setup_inst_rendered = setup_inst_tpl.render(template_context)
        self.write_template(output_dir, "README.md", setup_inst_rendered)
