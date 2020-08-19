# Copyright 2020 Spotify AB

import glob
import logging
import os
import threading

from multiprocessing import pool

import apache_beam as beam
import six
import yaml

from klio_core import config
from klio_core import dataflow
from klio_core.proto import klio_pb2

from klio.message_handler import v1 as v1_msg_handler
from klio.message_handler import v2 as v2_msg_handler
from klio.metrics import client as metrics_client
from klio.metrics import logger as metrics_logger
from klio.metrics import stackdriver
from klio.transforms import _utils


class _KlioNamespace(object):
    """Namespace object to attach klio-related objects to a transform."""

    _thread_local = threading.local()

    def __init__(self):
        self.__transform_name = None

    def _load_config_from_file(self):
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

        inputs = self.config.job_config.events.inputs
        # [batch dev] we're essentially zipping together event inputs with
        #             data inputs. not sure if this is a good assumption
        # [batch dev] TODO: this should be updated once we update the proto
        #             definition of JobInput (prob separate Event and Data
        #             input definitions)
        for index, input_ in enumerate(inputs):
            job_input = klio_job.JobInput()
            if input_.name == "pubsub":
                job_input.topic = input_.topic
                job_input.subscription = input_.subscription

            data_input = self.config.job_config.data.inputs[index]
            if data_input and data_input.name == "gcs":
                job_input.data_location = data_input.location
            klio_job.inputs.extend([job_input])

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

    def _get_parent_jobs(self):
        """Get parent job(s) from current job's config and Dataflow API.

        Returns:
            list(klio_pb2.KlioJob)
        """
        # TODO: allow dataflow api version to be configurable @lynn
        dataflow_client = dataflow.get_dataflow_client()
        dependencies = self.config.job_config.dependencies

        parent_jobs = []
        for dep in dependencies:
            job_name = dep["job_name"]
            gcp_project = dep["gcp_project"]
            region = dep.get("region")

            # maybe nice folks have already added it to the configuration
            # TODO: support multiple topics @lynn
            input_topic = dep.get("input_topic")
            if not input_topic:
                try:
                    input_topic = dataflow_client.get_job_input_topic(
                        job_name, gcp_project, region
                    )
                except Exception as e:
                    self.logger.warning(
                        "No input topic found for job '%s' under project "
                        "'%s': %s" % (job_name, gcp_project, e)
                    )
                    continue

            if not input_topic:
                continue

            klio_job = klio_pb2.KlioJob()
            klio_job.job_name = job_name
            klio_job.gcp_project = gcp_project
            job_input = klio_job.JobInput()
            job_input.topic = input_topic
            klio_job.inputs.extend([job_input])
            parent_jobs.append(klio_job.SerializeToString())
            self.logger.debug(
                "Successfully found parent job '%s' under project '%s'"
                % (job_name, gcp_project)
            )

        if dependencies and not parent_jobs:
            self.logger.warning(
                "No input topics for configured parent jobs found."
            )

        return parent_jobs

    def _create_thread_pool(self):
        processes = self.config.job_config.thread_pool_processes
        return pool.ThreadPool(processes=processes)

    @property
    def config(self):
        klio_config = getattr(self._thread_local, "klio_config", None)
        if not klio_config:
            self._thread_local.klio_config = self._load_config_from_file()
        return self._thread_local.klio_config

    @property
    def job(self):
        klio_job = getattr(self._thread_local, "klio_job", None)
        if not klio_job:
            self._thread_local.klio_job = self._create_klio_job_obj()
        return self._thread_local.klio_job

    @property
    def logger(self):
        klio_logger = getattr(self._thread_local, "klio_logger", None)
        if not klio_logger:
            self._thread_local.klio_logger = logging.getLogger("klio")
        return self._thread_local.klio_logger

    @property
    def metrics(self):
        metrics_registry = getattr(self._thread_local, "klio_metrics", None)
        if not metrics_registry:
            self._thread_local.klio_metrics = self._get_metrics_registry()
        return self._thread_local.klio_metrics

    # TODO: is "dependencies" a better name?
    @property
    def parent_jobs(self):
        parents = getattr(self._thread_local, "parent_jobs", None)
        if not parents:
            self._thread_local.parent_jobs = self._get_parent_jobs()
        return self._thread_local.parent_jobs

    # <-- private/internal attributes -->
    @property
    def _thread_pool(self):
        klio_thread_pool = getattr(
            self._thread_local, "klio_thread_pool", None
        )
        if not klio_thread_pool:
            self._thread_local.klio_thread_pool = self._create_thread_pool()
        return self._thread_local.klio_thread_pool

    @property
    def _transform_name(self):
        return self.__transform_name

    @_transform_name.setter
    def _transform_name(self, name):
        self.__transform_name = name


class KlioDoFnMetaclass(type):
    """Enforce behavior upon subclasses of `KlioBaseDoFn`."""

    def __init__(cls, name, bases, clsdict):
        """Decorate a user's DoFn process method to parse Klio messages."""
        _utils.has_abstract_methods_implemented(cls, name, bases)

        if not getattr(cls, "_klio", None):
            setattr(cls, "_klio", _KlioNamespace())

        if os.getenv("KLIO_TEST_MODE", "").lower() in ("true", "1"):
            return

        if _utils.is_original_process_func(
            clsdict, bases, base_class="KlioBaseDoFn"
        ):
            if cls._klio.config.version == 1:
                setattr(
                    cls,
                    "process",
                    v1_msg_handler.parse_klio_message(clsdict["process"]),
                )

            elif cls._klio.config.version == 2:
                setattr(
                    cls,
                    "process",
                    v2_msg_handler.parse_klio_message(clsdict["process"]),
                )

            else:
                raise RuntimeError(
                    "Unsupported 'version' in 'klio-job.yaml': {}".format(
                        cls._klio.config.version
                    )
                )
            # "initialize" klio namespace once we have original process func
            cls._klio._transform_name = name

    def __new__(metaclass, name, bases, clsdict):
        """Get abstract methods for a base class."""
        clsdict["_klio_abstract_methods"] = _utils.get_abstract_methods(
            clsdict
        )
        clsdict["_klio_all_methods"] = _utils.get_all_methods(clsdict)
        cls = type.__new__(metaclass, name, bases, clsdict)
        return cls


class KlioBaseDoFn(six.with_metaclass(KlioDoFnMetaclass, beam.DoFn)):
    """Base DoFn class from which Klio jobs must inherit."""

    @_utils.abstract
    def input_data_exists(self, entity_id):
        """Check if input data for given entity ID exists.

        Args:
            entity_id (str): a referential identifier that points to an
                audio file found in the configured input data location.

        Returns:
            (bool) whether or not the expected input data exists for the
                given entity_id.
        """
        pass  # pragma: no cover

    @_utils.abstract
    def output_data_exists(self, entity_id):
        """Check if the supposed output data for a given entity ID exists.
        Args:
            entity_id (str): a referential identifier that points to an
                audio file found in the configured output data location.
        Returns:
            (bool) whether or not the output data exists for the given
                entity_id.
        """
        pass  # pragma: no cover


# [Klio v2]: _KlioNamespace should probably be deprecated and replaced
#           by (hopefully simpler) KlioContext
class KlioContext(object):
    # TODO: is this still needed on dataflow?
    _thread_local = threading.local()

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

        inputs = self.config.job_config.events.inputs
        # [batch dev] we're essentially zipping together event inputs with
        #             data inputs. not sure if this is a good assumption
        # [batch dev] TODO: this should be updated once we update the proto
        #             definition of JobInput (prob separate Event and Data
        #             input definitions)
        for index, input_ in enumerate(inputs):
            job_input = klio_job.JobInput()
            if input_.name == "pubsub":
                job_input.topic = input_.topic
                job_input.subscription = input_.subscription

            data_input = self.config.job_config.data.inputs[index]
            if data_input and data_input.name == "gcs":
                job_input.data_location = data_input.location
            klio_job.inputs.extend([job_input])

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
        klio_config = getattr(self._thread_local, "klio_config", None)
        if not klio_config:
            self._thread_local.klio_config = self._load_config_from_file()
        return self._thread_local.klio_config

    @property
    def job(self):
        klio_job = getattr(self._thread_local, "klio_job", None)
        if not klio_job:
            self._thread_local.klio_job = self._create_klio_job_obj()
        return self._thread_local.klio_job

    @property
    def logger(self):
        klio_logger = getattr(self._thread_local, "klio_logger", None)
        if not klio_logger:
            self._thread_local.klio_logger = logging.getLogger("klio")
        return self._thread_local.klio_logger

    @property
    def metrics(self):
        metrics_registry = getattr(self._thread_local, "klio_metrics", None)
        if not metrics_registry:
            self._thread_local.klio_metrics = self._get_metrics_registry()
        return self._thread_local.klio_metrics
