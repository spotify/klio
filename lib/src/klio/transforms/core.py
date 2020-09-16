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

import glob
import logging
import os
import threading

import yaml

from klio_core import config
from klio_core.proto import klio_pb2

from klio.metrics import client as metrics_client
from klio.metrics import logger as metrics_logger
from klio.metrics import stackdriver


class KlioContext(object):
    """Context related to the currently running job.

    Available to transforms via one of the :ref:`KlioContext decorators
    <klio-context-decorators>`.
    """

    # TODO: is this still needed on dataflow?
    _thread_local = threading.local()

    def __init__(self):
        self.__transform_name = None

    def _load_config_from_file(self):
        # [Klio v2] this may get expensive, to always be reading config
        # from a file. Can this be replaced by something in memory
        # that's also globally accessible?
        klio_job_file = "/usr/src/config/.effective-klio-job.yaml"
        # for backwards compatibility, and user is using setup.py and we
        # have to find it somewhere...
        if not os.path.exists(klio_job_file):
            # use iterator so we don't waste time searching everywhere upfront
            files = glob.iglob("/usr/**/klio-job.yaml", recursive=True)
            for f in files:
                klio_job_file = f
                # only grab the first one
                break
        with open(klio_job_file, "r") as f:
            all_config_data = yaml.safe_load(f)
        return config.KlioConfig(all_config_data)

    def _create_klio_job_obj(self):
        klio_job = klio_pb2.KlioJob()
        klio_job.job_name = self.config.job_name
        klio_job.gcp_project = self.config.pipeline_options.project
        klio_job_str = klio_job.SerializeToString()
        return klio_job_str

    def _get_metrics_registry(self):
        clients = []
        use_logger, use_stackdriver = None, None
        metrics_config = self.config.job_config.metrics

        # use_logger and use_stackdriver could be False (turn off),
        # None (use default config), or a dict of configured values
        use_logger = metrics_config.get("logger")
        use_stackdriver = metrics_config.get("stackdriver_logger")

        # TODO: set runner in OS environment (via klio-exec), since
        #       the runner defined in config could be overwritten via
        #       `--direct-runner`.
        #       i.e.: runner = os.getenv("BEAM_RUNNER", "").lower()
        runner = self.config.pipeline_options.runner
        if "dataflow" in runner.lower():
            # Must explicitly compare to `False` since `None` could be
            # the user accepting default config.
            # If explicitly false, then just disable logger underneath SD
            if use_stackdriver is not False:
                sd_client = stackdriver.StackdriverLogMetricsClient(
                    self.config
                )
                clients.append(sd_client)
            else:
                # if use_stackdriver is explicitly false, then make sure
                # logger client is disabled since the stackdriver client
                # inherits the logger client
                use_logger = False

        if not len(clients):  # setup default client
            disabled = False
            # User might disable the logger, but we still need a relay
            # client if all other relay clients are disabled. This allows
            # folks to silence metrics but not need to remove code that
            # interacts with `_klio.metrics`.
            # Must explicitly compare to `False` since `None` could be
            # the user accepting default config
            if use_logger is False:
                disabled = True
            logger_client = metrics_logger.MetricsLoggerClient(
                self.config, disabled=disabled
            )
            clients.append(logger_client)

        return metrics_client.MetricsRegistry(
            clients, transform_name=self._transform_name
        )

    @property
    def config(self):
        """A ``KlioConfig`` instance representing the job's configuration."""
        klio_config = getattr(self._thread_local, "klio_config", None)
        if not klio_config:
            self._thread_local.klio_config = self._load_config_from_file()
        return self._thread_local.klio_config

    @property
    def job(self):
        """An instance of :ref:`kliojob` of the current job."""
        klio_job = getattr(self._thread_local, "klio_job", None)
        if not klio_job:
            self._thread_local.klio_job = self._create_klio_job_obj()
        return self._thread_local.klio_job

    @property
    def logger(self):
        """A namespaced logger.

        Equivalent to ``logging.getLogger("klio")``.
        """
        klio_logger = getattr(self._thread_local, "klio_logger", None)
        if not klio_logger:
            self._thread_local.klio_logger = logging.getLogger("klio")
        return self._thread_local.klio_logger

    @property
    def metrics(self):
        """A metrics registry instance.

        See :ref:`metrics <metrics>` for more information."""
        metrics_registry = getattr(self._thread_local, "klio_metrics", None)
        if not metrics_registry:
            self._thread_local.klio_metrics = self._get_metrics_registry()
        return self._thread_local.klio_metrics

    # <-- private/internal attributes -->
    @property
    def _transform_name(self):
        return self.__transform_name

    @_transform_name.setter
    def _transform_name(self, name):
        self.__transform_name = name
