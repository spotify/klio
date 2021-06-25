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

import logging
import warnings

from apache_beam import pipeline as beam_pipeline
from apache_beam.options import pipeline_options
from apache_beam.options import value_provider
from apache_beam.runners.direct import bundle_factory
from apache_beam.runners.direct import clock as beam_clock
from apache_beam.runners.direct import (
    consumer_tracking_pipeline_visitor as ctpv,
)
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.direct import evaluation_context as eval_ctx
from apache_beam.runners.direct import executor as beam_exec
from apache_beam.testing import test_stream

from klio_exec.runners import evaluators


# without this, users would get flooded with warnings of "your application has
# authenticated using end user credentials from Google Cloud SDK ..."
warnings.filterwarnings(
    "ignore", category=UserWarning, module="google.auth._default",
)
_LOGGER = logging.getLogger("klio.gke_direct_runner")


class GkeDirectRunner(direct_runner.BundleBasedDirectRunner):
    """Custom DirectRunner class for running on GKE.

    Acknowledges PubsubMessages after they are finished processing rather than
    before they start processing, but otherwise is meant to behave identically
    to the BundleBasedDirectRunner.
    """

    def run_pipeline(self, pipeline, options):
        """Execute the entire pipeline and returns an DirectPipelineResult."""

        # Klio maintainer note: This code is the eact same logic in
        # direct_runner.BundleBasedDirectRunner.run_pipeline with the
        # following changes:
        # 1. Import statements that were originally inside this method
        #    was moved to the top of this module.
        # 2. Import statements adjusted to import module and not objects
        #    according to the google style guide.
        # 3. The functionalty we needed to override, which is invoking
        #    our own TransformEvaluatorRegistry when instantiating the
        #    Executor class (called out below).

        # If the TestStream I/O is used, use a mock test clock.
        class TestStreamUsageVisitor(beam_pipeline.PipelineVisitor):
            """Visitor determining whether a Pipeline uses a TestStream."""

            def __init__(self):
                self.uses_test_stream = False

            def visit_transform(self, applied_ptransform):
                if isinstance(
                    applied_ptransform.transform, test_stream.TestStream
                ):
                    self.uses_test_stream = True

        visitor = TestStreamUsageVisitor()
        pipeline.visit(visitor)
        clock = (
            beam_clock.TestClock()
            if visitor.uses_test_stream
            else beam_clock.RealClock()
        )

        # Performing configured PTransform overrides.
        pipeline.replace_all(direct_runner._get_transform_overrides(options))

        _LOGGER.info("Running pipeline with Klio's GkeDirectRunner.")
        self.consumer_tracking_visitor = ctpv.ConsumerTrackingPipelineVisitor()
        pipeline.visit(self.consumer_tracking_visitor)

        bndl_factory = bundle_factory.BundleFactory(
            stacked=options.view_as(
                pipeline_options.DirectOptions
            ).direct_runner_use_stacked_bundle
        )
        evaluation_context = eval_ctx.EvaluationContext(
            options,
            bndl_factory,
            self.consumer_tracking_visitor.root_transforms,
            self.consumer_tracking_visitor.value_to_consumers,
            self.consumer_tracking_visitor.step_names,
            self.consumer_tracking_visitor.views,
            clock,
        )

        # Klio maintainer note: this is where the change in logic is:
        # using our own `KlioTransformEvaluatorRegistry`.
        executor = beam_exec.Executor(
            self.consumer_tracking_visitor.value_to_consumers,
            evaluators.KlioTransformEvaluatorRegistry(evaluation_context),
            evaluation_context,
        )
        # DirectRunner does not support injecting
        # PipelineOptions values at runtime
        value_provider.RuntimeValueProvider.set_runtime_options({})
        # Start the executor. This is a non-blocking call, it will start the
        # execution in background threads and return.
        executor.start(self.consumer_tracking_visitor.root_transforms)
        result = direct_runner.DirectPipelineResult(
            executor, evaluation_context
        )

        return result
