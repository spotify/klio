# Copyright 2021 Spotify AB
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

"""Tests for GkeDirectRunner.

Adapted from apache_beam/runners/direct/direct_runner_test.py
"""
import logging
import threading

import apache_beam as beam
import hamcrest as hc
from apache_beam import pipeline
from apache_beam.metrics import cells
from apache_beam.metrics import execution
from apache_beam.metrics import metric
from apache_beam.metrics import metricbase
from apache_beam.testing import test_pipeline
from apache_beam.testing import util

from klio_exec.runners import gke_direct


def test_gke_direct_runs(caplog):
    runner = "klio_exec.runners.gke_direct.GkeDirectRunner"
    pipeline = test_pipeline.TestPipeline(runner=runner)
    _ = pipeline | beam.Create([{"foo": "bar"}])
    result = pipeline.run()
    result.wait_until_finish()

    gke_logger_records = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and r.name == "klio.gke_direct_runner"
    ]
    assert 1 == len(gke_logger_records)


def test_waiting_on_result_stops_executor_threads():
    pre_test_threads = set(t.ident for t in threading.enumerate())

    runner = "klio_exec.runners.gke_direct.GkeDirectRunner"
    pipeline = test_pipeline.TestPipeline(runner=runner)
    _ = pipeline | beam.Create([{"foo": "bar"}])
    result = pipeline.run()
    result.wait_until_finish()

    post_test_threads = set(t.ident for t in threading.enumerate())
    new_threads = post_test_threads - pre_test_threads
    assert 0 == len(new_threads)


def test_direct_runner_metrics():
    class MyDoFn(beam.DoFn):
        def start_bundle(self):
            count = metric.Metrics.counter(self.__class__, "bundles")
            count.inc()

        def finish_bundle(self):
            count = metric.Metrics.counter(self.__class__, "finished_bundles")
            count.inc()

        def process(self, element):
            gauge = metric.Metrics.gauge(self.__class__, "latest_element")
            gauge.set(element)
            count = metric.Metrics.counter(self.__class__, "elements")
            count.inc()
            distro = metric.Metrics.distribution(
                self.__class__, "element_dist"
            )
            distro.update(element)
            return [element]

    p = pipeline.Pipeline(gke_direct.GkeDirectRunner())
    pcoll = (
        p
        | beam.Create([1, 2, 3, 4, 5], reshuffle=False)
        | "Do" >> beam.ParDo(MyDoFn())
    )
    util.assert_that(pcoll, util.equal_to([1, 2, 3, 4, 5]))
    result = p.run()
    result.wait_until_finish()
    metrics = result.metrics().query()
    namespace = "{}.{}".format(MyDoFn.__module__, MyDoFn.__name__)

    hc.assert_that(
        metrics["counters"],
        hc.contains_inanyorder(
            execution.MetricResult(
                execution.MetricKey(
                    "Do", metricbase.MetricName(namespace, "elements")
                ),
                5,
                5,
            ),
            execution.MetricResult(
                execution.MetricKey(
                    "Do", metricbase.MetricName(namespace, "bundles")
                ),
                1,
                1,
            ),
            execution.MetricResult(
                execution.MetricKey(
                    "Do", metricbase.MetricName(namespace, "finished_bundles")
                ),
                1,
                1,
            ),
        ),
    )

    hc.assert_that(
        metrics["distributions"],
        hc.contains_inanyorder(
            execution.MetricResult(
                execution.MetricKey(
                    "Do", metricbase.MetricName(namespace, "element_dist")
                ),
                cells.DistributionResult(cells.DistributionData(15, 5, 1, 5)),
                cells.DistributionResult(cells.DistributionData(15, 5, 1, 5)),
            )
        ),
    )

    gauge_result = metrics["gauges"][0]
    hc.assert_that(
        gauge_result.key,
        hc.equal_to(
            execution.MetricKey(
                "Do", metricbase.MetricName(namespace, "latest_element")
            )
        ),
    )
    hc.assert_that(gauge_result.committed.value, hc.equal_to(5))
    hc.assert_that(gauge_result.attempted.value, hc.equal_to(5))
