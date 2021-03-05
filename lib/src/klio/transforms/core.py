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

import __main__
import glob
import logging
import os
import threading

import yaml

from klio_core import config
from klio_core.proto import klio_pb2

from klio.metrics import client as metrics_client
from klio.metrics import logger as metrics_logger
from klio.metrics import native as native_metrics
from klio.metrics import stackdriver


class RunConfig(object):

    _thread_local = threading.local()

    @classmethod
    def _load_config_from_file(cls):
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

    # NOTE: for now this approach is not being used (and may be removed in the
    # future)
    @classmethod
    def _get_via_main_session(cls):
        if hasattr(__main__, "run_config"):
            return __main__.run_config
        else:
            raise Exception(
                "Attempt to access RunConfig before it was set. This likely"
                " means something was imported before RunConfig was set."
            )

    @classmethod
    def _get_via_thread_local(cls):
        klio_config = getattr(cls._thread_local, "klio_config", None)
        if not klio_config:
            cls._thread_local.klio_config = cls._load_config_from_file()
        return cls._thread_local.klio_config

    @classmethod
    def get(cls):
        return cls._get_via_thread_local()

    @classmethod
    def set(cls, config):
        __main__.run_config = config


class KlioContext(object):
    """Context related to the currently running job.

    Available to transforms via one of the :ref:`KlioContext decorators
    <klio-context-decorators>`.
    """

    _thread_local = threading.local()

    def __init__(self):
        self.__transform_name = None

    def _create_klio_job_obj(self):
        klio_job = klio_pb2.KlioJob()
        klio_job.job_name = self.config.job_name
        klio_job.gcp_project = self.config.pipeline_options.project
        klio_job_str = klio_job.SerializeToString()
        return klio_job_str

    def _get_metrics_registry(self):
        native_metrics_client = native_metrics.NativeMetricsClient(self.config)
        clients = [native_metrics_client]
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
            if use_logger is None:
                use_logger = False

            if use_stackdriver is None:
                use_stackdriver = True
            # if use_stackdriver is explicitly False, then make sure
            # logger client is disabled since the stackdriver client
            # inherits the logger client
            elif use_stackdriver is False:
                use_logger = False

        else:
            if use_logger is None:
                use_logger = True
            if use_stackdriver is not False:
                # Explicitly turn it off when not running on Dataflow as SD
                # metrics reporting doesn't work when running locally
                use_stackdriver = False

        if use_stackdriver is not False:
            sd_client = stackdriver.StackdriverLogMetricsClient(self.config)
            clients.append(sd_client)

        if use_logger is not False:
            logger_client = metrics_logger.MetricsLoggerClient(self.config)
            clients.append(logger_client)

        return metrics_client.MetricsRegistry(
            clients, transform_name=self._transform_name
        )

    @property
    def config(self):
        """A ``KlioConfig`` instance representing the job's configuration."""
        return RunConfig.get()

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
