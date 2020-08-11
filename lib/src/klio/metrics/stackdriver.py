# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB
"""
Klio ships with a Stackdriver Log-based Metrics relay client. The client
creates metrics objects in Stackdriver Monitoring based off of filters in
Stackdriver Logging. For more information on Stackdriver's Log-based Metrics,
see https://cloud.google.com/logging/docs/logs-based-metrics/.

When running on Dataflow, the client is on by default with no additional
configuration needed. This must be actively turned off in `klio-job.yaml`
if not wanted (see below).

The log-based metrics client is not available for direct runner.

To explicitly turn off log-based metrics, in `klio-job.yaml`:

    job_config:
      metrics:
        stackdriver_logger: false

"""
import logging

from googleapiclient import discovery
from googleapiclient import errors as gapi_errors

from klio.metrics import logger


class StackdriverLogMetricsClient(logger.MetricsLoggerClient):
    RELAY_CLIENT_NAME = "stackdriver_logger"

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
        self.logger.log(
            logging.WARNING,
            "Gauge is not supported for Stackdriver log-based metrics, "
            "defaulting to standard logger.",
        )
        return StackdriverLogMetricsGauge(*args, **kwargs)

    def timer(self, *args, **kwargs):
        self.logger.log(
            logging.WARNING,
            "Timer is not supported for Stackdriver log-based metrics, "
            "defaulting to standard logger.",
        )
        return StackdriverLogMetricsTimer(*args, **kwargs)


class StackdriverLogMetricsCounter(logger.LoggerCounter):
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


class StackdriverLogMetricsGauge(logger.LoggerGauge):
    """Pass-thru object for naming only. Stackdriver log-based metrics
    does not support gauges.
    """


class StackdriverLogMetricsTimer(logger.LoggerTimer):
    """Pass-thru object for naming only. Stackdriver log-based metrics
    does not support timers.
    """
