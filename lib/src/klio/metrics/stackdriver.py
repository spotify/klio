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
"""
Klio ships with a Stackdriver Log-based Metrics relay client. The client
creates metrics objects in Stackdriver Monitoring based off of filters in
Stackdriver Logging. For more information on Stackdriver's Log-based Metrics,
see `related documentation <https://cloud.google.com/logging/docs/
logs-based-metrics/>`_.

When running on Dataflow, the client is on by default with no additional
configuration needed. This must be actively turned off in ``klio-job.yaml``
if not wanted (see below).

The log-based metrics client is not available for direct runner.

To explicitly turn off log-based metrics, in ``klio-job.yaml``:

.. code-block:: yaml

    job_config:
      metrics:
        stackdriver_logger: false

.. deprecated:: 21.3.0
    Use :mod:`native <klio.metrics.native>` metrics instead. See
    :ref:`docs <stackdriver-log-metrics-deprecation-notice>` for more info.
"""
import logging

from googleapiclient import discovery
from googleapiclient import errors as gapi_errors

from klio.metrics import logger
from klio.transforms import _utils


BASE_WARN_MSG = " has been deprecated since `klio` version 21.3.0"


# FYI: marking the __init__ method as deprecated instead of the class since
#      (for now) the decorator doesn't work on classes.
class StackdriverLogMetricsClient(logger.MetricsLoggerClient):
    """Stackdriver client for transform metrics.

    Intended to be instantiated by :class:`klio.metrics.client.MetricsRegistry`
    and not by itself.

    Args:
        klio_config (klio_core.config.KlioConfig):  the job's configuration.

    .. deprecated:: 21.3.0
       Use :class:`NativeMetricsClient
       <klio.metrics.native.NativeMetricsClient>` instead. See
       :ref:`docs <stackdriver-log-metrics-deprecation-notice>` for more info.
    """

    RELAY_CLIENT_NAME = "stackdriver_logger"

    @_utils.deprecated(message="StackdriverLogMetricsClient" + BASE_WARN_MSG)
    def __init__(self, klio_config):
        super(StackdriverLogMetricsClient, self).__init__(klio_config)
        self.job_name = klio_config.job_name

    @property
    def _stackdriver_client(self):
        client = getattr(self._thread_local, "stackdriver_client", None)
        if not client:
            # FYI this does not make a network call
            client = discovery.build("logging", "v2")
            self._thread_local.stackdriver_client = client
        return self._thread_local.stackdriver_client

    # get a list of metrics first and use any already-available metrics
    # to create metrics objects
    def counter(self, name, transform=None, tags=None, **kwargs):
        """Create a :class:`StackdriverLogMetricsCounter` object.

        .. note::

            Stackdriver counts log lines so initializing a
            counter value is not supported .

        Args:
            name (str): name of counter
            transform (str): transform the counter is associated with
            tags (dict): any tags of additional contextual information
                to associate with the counter

        Returns:
            StackdriverLogMetricsCounter: a log-based counter
        """
        # Since stackdriver literally counts loglines, initializing a
        # counter value is not supported
        if kwargs.get("value", 0) > 0:
            self.logger.log(
                logging.WARNING,
                "Initializing Stackdriver log-based counters with a value "
                "other than 0 is not supported. Defaulting to 0.",
            )
        ctr = StackdriverLogMetricsCounter(
            name=name,
            job_name=self.job_name,
            project=self.klio_config.pipeline_options.project,
            transform=transform,
            tags=tags,
        )
        ctr._init_metric(self._stackdriver_client)
        return ctr

    def gauge(self, *args, **kwargs):
        """Create a :class:`StackdriverLogMetricsGauge` object.

        .. warning::

            Gauges for Stackdriver are not yet supported. This will
            default to standard logging.

        Args:
            name (str): name of gauge
            value (int): starting value of gauge; defaults to 0
            transform (str): transform the gauge is associated with
            tags (dict): any tags of additional contextual information
                to associate with the gauge

        Returns:
            StackdriverLogMetricsGauge: a log-based gauge
        """
        self.logger.log(
            logging.WARNING,
            "Gauge is not supported for Stackdriver log-based metrics, "
            "defaulting to standard logger.",
        )
        return StackdriverLogMetricsGauge(*args, **kwargs)

    def timer(self, *args, **kwargs):
        """Create a :class:`StackdriverLogMetricsTimer` object.

        .. warning::

            Timers for Stackdriver are not yet supported. This will
            default to standard logging.

        Args:
            name (str): name of gauge
            value (int): starting value of gauge; defaults to 0
            transform (str): transform the gauge is associated with
            tags (dict): any tags of additional contextual information
                to associate with the gauge

        Returns:
            StackdriverLogMetricsTimer: a log-based timer
        """
        self.logger.log(
            logging.WARNING,
            "Timer is not supported for Stackdriver log-based metrics, "
            "defaulting to standard logger.",
        )
        return StackdriverLogMetricsTimer(*args, **kwargs)


# FYI: marking the __init__ method as deprecated instead of the class since
#      (for now) the decorator doesn't work on classes.
class StackdriverLogMetricsCounter(logger.LoggerCounter):
    """Stackdriver log-based counter metric.

    .. note::

        Stackdriver counts log lines so initializing a counter value is
        not supported .

    Args:
        name (str): name of counter
        job_name (str): name of Dataflow job
        project (str): name of GCP project associated with Dataflow job
        transform (str): Name of transform associated with counter, if any.
        tags (dict): Tags to associate with counter. Note:
            ``{"metric_type": "counter"}`` will always be an included tag.

    .. deprecated:: 21.3.0
        Use :class:`NativeCounter <klio.metrics.native.NativeCounter>` instead.
        See :ref:`docs <stackdriver-log-metrics-deprecation-notice>` for more
        info.
    """

    # NOTE: The in-memory value (as kept track in the metric Dispatchers) may
    # be greater than 1, but it doesn't mean anything because:
    # 1. log-based counters in SD count the number log lines, and does not
    #    extract a value from the log line, and
    # 2. state is not maintained within a pipeline, so the in-memory
    #    value will eventually be wiped and re-initialized.
    # Therefore, we're hard-coding counters to the value of 1
    DEFAULT_LOG_FORMAT = (
        "[{name}] value: 1 transform: '{transform}' tags: {tags}"
    )
    KLIO_TRANSFORM_LABEL_KEY = "klio_transform"

    @_utils.deprecated(message="StackdriverLogMetricsCounter" + BASE_WARN_MSG)
    def __init__(self, name, job_name, project, transform=None, tags=None):
        # Since stackdriver literally counts loglines, initializing a
        # counter value is not supported; defaulting to 0
        super(StackdriverLogMetricsCounter, self).__init__(
            name, value=0, transform=transform, tags=tags
        )
        self.job_name = job_name
        self.project = project
        self.parent = "projects/{}".format(project)
        self.desc = "Klio counter '{}' ".format(self.name)
        self.body = self._get_body()

    def _get_filter(self):
        _filter = (
            'resource.type="dataflow_step" '
            'logName="projects/{project}/logs/'
            'dataflow.googleapis.com%2Fworker" '
            # since stackdriver counters do not support regexes, we're
            # including the [] around the name to avoid name collisions like
            # my-counter & my-counter2
            'jsonPayload.message:"[{name}]"'.format(
                project=self.project, name=self.name
            )
        )
        return _filter

    def _get_transform_label_extractor(self):
        # Grab the transform name from the log line, i.e.:
        # "[my-counter] value: 1 transform: 'HelloKlio' tags: {'metric_...";
        # Needs to be able to follow valid Python chars for class names:
        # https://stackoverflow.com/a/10120327/1579977
        label_extractor = "\"transform: '([a-zA-Z_][a-zA-Z0-9_]*)' tags:\""

        # docs: https://cloud.google.com/logging/docs/
        #              logs-based-metrics/labels#create-label
        label_regex = "REGEXP_EXTRACT(jsonPayload.message, {})".format(
            label_extractor
        )

        return {self.KLIO_TRANSFORM_LABEL_KEY: label_regex}

    def _get_body(self):
        labels = [
            {
                "key": self.KLIO_TRANSFORM_LABEL_KEY,
                "valueType": "STRING",
                "description": "Name of Klio-based transform",
            }
        ]
        body = {
            "name": self.name,
            "description": self.desc,
            "filter": self._get_filter(),
            "metricDescriptor": {
                "metricKind": "DELTA",
                "valueType": "INT64",
                "unit": "1",
                "labels": labels,
            },
            "labelExtractors": self._get_transform_label_extractor(),
        }
        return body

    def _init_metric(self, stackdriver_client):
        req = (
            stackdriver_client.projects()
            .metrics()
            .create(parent=self.parent, body=self.body)
        )
        try:
            req.execute()

        except gapi_errors.HttpError as e:
            # HTTP Conflict - metric already exists
            if e.resp.status == 409:
                logging.getLogger("klio.metrics").debug(
                    "Metric {} already exists".format(self.name)
                )
                return

            logging.getLogger("klio.metrics").error(
                "Error creating metric '{}': {}".format(self.name, e),
                exc_info=True,
            )

        except Exception as e:
            logging.getLogger("klio.metrics").error(
                "Error creating metric '{}': {}".format(self.name, e),
                exc_info=True,
            )

    # including method for documentation purposes


# FYI: marking the __init__ method as deprecated instead of the class since
#      (for now) the decorator doesn't work on classes.
class StackdriverLogMetricsGauge(logger.LoggerGauge):
    """Pass-thru object for naming only. Stackdriver log-based metrics
    does not support gauges.

    .. deprecated:: 21.3.0
        Use :class:`NativeGauge <klio.metrics.native.NativeGauge>` instead.
        See :ref:`docs <stackdriver-log-metrics-deprecation-notice>` for more
        info.
    """

    @_utils.deprecated(message="StackdriverLogMetricsGauge" + BASE_WARN_MSG)
    def __init__(self, *args, **kwargs):
        super(StackdriverLogMetricsGauge, self).__init__(*args, **kwargs)


# FYI: marking the __init__ method as deprecated instead of the class since
#      (for now) the decorator doesn't work on classes.
class StackdriverLogMetricsTimer(logger.LoggerTimer):
    """Pass-thru object for naming only. Stackdriver log-based metrics
    does not support timers.

    .. deprecated:: 21.3.0
        Use :class:`NativeTimer <klio.metrics.native.NativeTimer>` instead.
        See :ref:`docs <stackdriver-log-metrics-deprecation-notice>` for more
        info.
    """

    @_utils.deprecated(message="StackdriverLogMetricsTimer" + BASE_WARN_MSG)
    def __init__(self, *args, **kwargs):
        super(StackdriverLogMetricsTimer, self).__init__(*args, **kwargs)
