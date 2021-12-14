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
import logging
import threading

from klio_core import variables as kvars
from klio_core.proto import klio_pb2

from klio.metrics import client as metrics_client
from klio.metrics import logger as metrics_logger
from klio.metrics import native as native_metrics
from klio.metrics import shumway


class RunConfig(object):
    """
    Manages runtime storage of KlioConfig object used during pipeline
    execution.

    In order for the config to be available on workers (which may include
    values overridden from CLI arguments), it is stored to a main session
    variable before the Beam pipeline is started.  Beam will ensure that
    everything in ``__main__`` is pickled and made available on worker nodes.
    """

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
    def get(cls):
        return cls._get_via_main_session()

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
        use_logger, use_shumway = None, None
        metrics_config = self.config.job_config.metrics

        # use_logger/use_shumway could be False (turn off),
        # None (use default config), or a dict of configured values
        use_logger = metrics_config.get("logger")
        use_shumway = metrics_config.get("shumway")

        # TODO: set runner in OS environment (via klio-exec), since
        #       the runner defined in config could be overwritten via
        #       `--direct-runner`.
        #       i.e.: runner = os.getenv("BEAM_RUNNER", "").lower()
        runner = self.config.pipeline_options.runner

        if kvars.KlioRunner.DIRECT_RUNNER != runner:
            if use_logger is None:
                use_logger = False

        # use shumway when running on DirectGKERunner unless it's explicitly
        # turned off/set to False. Don't set it to True if it's set to False
        # or it's a dictionary (aka has some configuration)
        if kvars.KlioRunner.DIRECT_GKE_RUNNER == runner:
            if use_shumway is None:
                use_shumway = True
        # shumway only works on DirectGKERunner, so we explicitly set it
        # to False
        else:
            use_shumway = False

        if use_logger is not False:
            logger_client = metrics_logger.MetricsLoggerClient(self.config)
            clients.append(logger_client)

        if use_shumway is not False:
            shumway_client = shumway.ShumwayMetricsClient(self.config)
            clients.append(shumway_client)

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
