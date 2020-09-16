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

import logging
import time

import pytest

from py import io

from klio_exec.commands.utils import plugin_utils


PLUGIN_NAMESPACE = "klio.plugins.audit"


def _run_pytest(tw):
    tw.write("Running tests for audit validation...\n")
    # Test-style verification
    exit_code = pytest.main(["-qq"])
    pytest_failed = exit_code != 0

    if pytest_failed:
        tw.write("PyTest failed!\n", yellow=True)
    else:
        tw.write("PyTest passed!\n")

    return pytest_failed


def _get_audit_steps(job_dir, config_obj, tw):
    loaded_steps = plugin_utils.load_plugins_by_namespace(PLUGIN_NAMESPACE)
    return [step(job_dir, config_obj, tw) for step in loaded_steps]


def list_audit_steps(ctx, param, value):
    # this is a click.option callback where it automatically passes in
    # context, param name, and param value
    if not value or ctx.resilient_parsing:
        return
    tw = io.TerminalWriter()
    tw.hasmarkup = True
    tw.sep("=", "Installed audit steps")
    plugin_utils.print_plugins(PLUGIN_NAMESPACE, tw)
    ctx.exit()


def audit(job_dir, config_obj):
    tw = io.TerminalWriter()
    tw.hasmarkup = True
    tw.write("Auditing your Klio job...\n")

    try:
        audit_steps = _get_audit_steps(job_dir, config_obj, tw)
        for step in audit_steps:
            step.before_tests()

        pytest_failed = _run_pytest(tw)

        tw.sep("=", "audit session starts", bold=True)

        start = time.time()

        # TODO: Currently, we're running all of the audit steps in
        # the same Python process and there's no isolation. Do we want
        # to run the tests once per step instead?
        for step in audit_steps:
            step.after_tests()

            if not any([step.errored, step.warned]):
                msg = "[{}]: audit step passed!\n".format(step.AUDIT_STEP_NAME)
                tw.write(msg, green=True)

        end = time.time()

        failed_steps = [step for step in audit_steps if step.errored]
        warned_steps = [step for step in audit_steps if step.warned]

        error_count = len(failed_steps)
        warning_count = len(warned_steps)

        finished_msg = "{0} errors, {1} warnings in {2:.3f} seconds".format(
            error_count, warning_count, end - start
        )
        color = "green"
        if error_count:
            color = "red"
        elif warning_count:
            color = "yellow"
        kwargs = {
            color: True,
            "bold": True,
        }
        tw.sep("=", finished_msg, **kwargs)

    except Exception:
        # use logging instead of tw to easily get traceback info
        logging.error(
            "Unable to run the audit command due to:\n", exc_info=True
        )

        raise SystemExit(1)

    if not pytest_failed and not error_count:
        if not warning_count:
            tw.write("Good job! Your job does not exhibit any known issues.\n")
        else:
            tw.write(
                "Cool! Your job has warnings, but no errors. "
                "Please check the warnings.\n",
                yellow=True,
            )
    else:
        tw.sep(
            "You have errors in your job. Please fix and try again.\n",
            red=True,
        )
        raise SystemExit(1)
