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

import collections
import contextlib
import functools
import logging
import os
import subprocess
import sys
import tempfile
import time

import apache_beam as beam

try:
    import memory_profiler
except ImportError:  # pragma: no cover
    logging.error(
        "Failed to import profiling dependencies. Did you install "
        "`klio-exec[debug]` in your job's Docker image?"
    )
    raise SystemExit(1)

from klio.transforms import decorators
from klio_core.proto import klio_pb2

from klio_exec.commands.utils import cpu_utils
from klio_exec.commands.utils import memory_utils
from klio_exec.commands.utils import profile_utils


@contextlib.contextmanager
def smart_open(filename=None, fmode=None):
    """Handle both stdout and files in the same manner."""
    if filename and filename != "-":
        fh = open(filename, fmode)
    else:
        fh = sys.stdout

    try:
        yield fh
    finally:
        if fh is not sys.stdout:
            fh.close()


class StubIOSubMapper(object):
    def __init__(self, input_pcol):
        def fake_constructor(*args, **kwargs):
            return input_pcol

        # normally this is a map of io-name -> transform class.  Instead we'll
        # just have every possible name return our pretend constructor that
        # returns our pre-constructed transform
        self.input = collections.defaultdict(lambda: fake_constructor)
        self.output = {}  # no outputs


class StubIOMapper(object):
    def __init__(self, input_pcol, iterations):

        repeated_input = input_pcol | beam.FlatMap(lambda x: [x] * iterations)

        self.batch = StubIOSubMapper(repeated_input)
        self.streaming = StubIOSubMapper(repeated_input)

    @staticmethod
    def from_input_file(file_path, iterations):
        transform = beam.io.ReadFromText(file_path)
        return StubIOMapper(transform, iterations)

    @staticmethod
    def from_entity_ids(id_list, iterations):
        transform = beam.Create(id_list)
        return StubIOMapper(transform, iterations)


class KlioPipeline(object):
    DEFAULT_FILE_PREFIX = "klio_profile_{what}_{ts}"
    TRANSFORMS_PATH = "./transforms.py"

    def __init__(
        self, klio_config, input_file=None, output_file=None, entity_ids=None
    ):
        self.input_file = input_file
        self.output_file = output_file
        self.entity_ids = entity_ids
        self._stream = None
        self._now_str = time.strftime("%Y%m%d%H%M%S", time.localtime())
        self.klio_config = klio_config

    def _get_output_png_file(self, what, temp_output):
        output_file_base = self.output_file
        prefix = KlioPipeline.DEFAULT_FILE_PREFIX.format(
            what=what, ts=self._now_str
        )
        if temp_output:
            output_file_base = prefix

        elif "." in self.output_file:
            # reuse a user's output file name, just replace existing extension
            output_file_base = os.path.splitext(self.output_file)[0]

        return "{}.png".format(output_file_base)

    @contextlib.contextmanager
    def _smart_temp_create(self, what, plot_graph):
        # For plotting a graph, an output file of the data collected is
        # needed, but the user shouldn't be required to provide an output
        # file if they don't want. This creates a tempfile to write data
        # to for generating the plot graph off of.
        # A context manager needed so that temp file can be cleaned up after.
        temp_output = False
        prefix = KlioPipeline.DEFAULT_FILE_PREFIX.format(
            what=what, ts=self._now_str
        )
        if plot_graph and not self.output_file:
            temp_output_file = tempfile.NamedTemporaryFile(
                dir=".", prefix=prefix
            )
            self.output_file = temp_output_file.name
            temp_output = True
        yield temp_output

    def _get_subproc(self, **kwargs):
        cmd = ["klioexec", "profile", "run-pipeline"]

        if kwargs.get("show_logs"):
            cmd.append("--show-logs")

        if self.input_file:
            cmd.extend(["--input-file", self.input_file])
        else:
            cmd.extend(self.entity_ids)

        return subprocess.Popen(cmd)

    def _get_cpu_line_profiler(self):
        return cpu_utils.KLineProfiler()

    def _profile_wall_time_per_line(self, iterations, **_):
        profiler = self._get_cpu_line_profiler()
        decorators.ACTIVE_PROFILER = profiler

        self._run_pipeline(iterations=iterations)

        if self.output_file:
            return profiler.print_stats(self.output_file, output_unit=1)

        # output_unit = 1 second, meaning the numbers in "Time" and
        # "Per Hit" columns are in seconds
        profiler.print_stats(output_unit=1)

    def _get_memory_line_profiler(self):
        return memory_utils.KMemoryLineProfiler(backend="psutil")

    def _get_memory_line_wrapper(self, profiler, get_maximum):
        wrapper = memory_utils.KMemoryLineProfiler.wrap_per_element
        if get_maximum:
            wrapper = functools.partial(
                memory_utils.KMemoryLineProfiler.wrap_maximum, profiler
            )
        return wrapper

    def _profile_memory_per_line(self, get_maximum=False):
        profiler = self._get_memory_line_profiler()
        decorators.ACTIVE_PROFILER = self._get_memory_line_wrapper(
            profiler, get_maximum
        )

        # "a"ppend if output per element; "w"rite (once) for maximum.
        # append will append a file with potentially already-existing data
        # (i.e. from a previous run), which may be confusing; but with how
        # memory_profiler treats streams, there's no simple way to prevent
        # appending data for per-element without re-implementing parts of
        # memory_profiler (maybe someday?) @lynn
        fmode = "w" if get_maximum else "a"

        with smart_open(self.output_file, fmode=fmode) as f:
            self._stream = f
            self._run_pipeline()

            if get_maximum:
                memory_profiler.show_results(profiler, stream=self._stream)

    def _profile_memory(self, **kwargs):
        # Profile the memory while the pipeline runs in another process
        p = self._get_subproc(**kwargs)

        plot_graph = kwargs.get("plot_graph")
        with self._smart_temp_create("memory", plot_graph) as temp_output:
            with smart_open(self.output_file, fmode="w") as f:
                memory_profiler.memory_usage(
                    proc=p,
                    interval=kwargs.get("interval"),
                    timestamps=True,
                    include_children=kwargs.get("include_children"),
                    multiprocess=kwargs.get("multiprocess"),
                    stream=f,
                )

            if not plot_graph:
                return

            output_png = self._get_output_png_file("memory", temp_output)
            profile_utils.plot(
                input_file=self.output_file,
                output_file=output_png,
                x_label="Time (in seconds)",
                y_label="Memory used (in MiB)",
                title="Memory Used While Running Klio-based Transforms",
            )
            return output_png

    def _profile_cpu(self, **kwargs):
        # Profile the CPU while the pipeline runs in another process
        p = self._get_subproc(**kwargs)

        plot_graph = kwargs.get("plot_graph")
        with self._smart_temp_create("cpu", plot_graph) as temp_output:
            with smart_open(self.output_file, fmode="w") as f:
                cpu_utils.get_cpu_usage(
                    proc=p, interval=kwargs.get("interval"), stream=f,
                )

            if not plot_graph:
                return

            output_png = self._get_output_png_file("cpu", temp_output)
            profile_utils.plot(
                input_file=self.output_file,
                output_file=output_png,
                x_label="Time (in seconds)",
                y_label="CPU%",
                title="CPU Usage of All Klio-based Transforms",
            )
            return output_png

    def _get_user_pipeline(self, config, io_mapper):
        runtime_config = collections.namedtuple(
            "RuntimeConfig",
            ["image_tag", "direct_runner", "update", "blocking"],
        )(None, True, False, True)

        from klio_exec.commands.run import KlioPipeline as KP

        return KP("profile_job", config, runtime_config, io_mapper)

    def _get_user_config(self):
        self.klio_config.pipeline_options.runner = "direct"
        self.klio_config.job_config.events.outputs = {}
        return self.klio_config

    @staticmethod
    def _entity_id_to_message(entity_id):
        message = klio_pb2.KlioMessage()
        message.data.element = bytes(entity_id, "UTF-8")
        message.metadata.intended_recipients.anyone.SetInParent()
        message.version = klio_pb2.Version.V2
        return message

    def _get_io_mapper(self, iterations):
        if self.input_file:
            return StubIOMapper.from_input_file(self.input_file, iterations)
        else:
            messages = []
            for entity_id in self.entity_ids:
                message = self._entity_id_to_message(entity_id)
                messages.append(message.SerializeToString())
            return StubIOMapper.from_entity_ids(messages, iterations)

    def _run_pipeline(self, iterations=None, **_):
        if not iterations:
            iterations = 1

        io_mapper = self._get_io_mapper(iterations)
        config = self._get_user_config()

        pipeline = self._get_user_pipeline(config, io_mapper)
        pipeline.run()

    def profile(self, what, **kwargs):

        if what == "run":
            return self._run_pipeline(**kwargs)
        elif what == "cpu":
            return self._profile_cpu(**kwargs)
        elif what == "memory":
            return self._profile_memory(**kwargs)
        elif what == "memory_per_line":
            return self._profile_memory_per_line(**kwargs)
        elif what == "timeit":
            return self._profile_wall_time_per_line(**kwargs)
