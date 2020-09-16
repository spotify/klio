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

import contextlib
import functools
import imp
import inspect
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

from apache_beam.options import pipeline_options

from klio import transforms as klio_transforms

from klio_exec.commands.utils import cpu_utils
from klio_exec.commands.utils import memory_utils
from klio_exec.commands.utils import profile_utils
from klio_exec.commands.utils import wrappers


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


class KlioPipeline(object):
    DEFAULT_FILE_PREFIX = "klio_profile_{what}_{ts}"
    TRANSFORMS_PATH = "./transforms.py"

    def __init__(self, input_file=None, output_file=None, entity_ids=None):
        self.input_file = input_file
        self.output_file = output_file
        self.entity_ids = entity_ids
        self._prof = None
        self._stream = None
        self._now_str = time.strftime("%Y%m%d%H%M%S", time.localtime())

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

    def _wrap_memory_per_line(self, transforms, get_maximum):
        wrapper = memory_utils.KMemoryLineProfiler.wrap_per_element
        if get_maximum:
            wrapper = functools.partial(
                memory_utils.KMemoryLineProfiler.wrap_maximum, self._prof
            )

        for txf in transforms:
            process_method = getattr(txf, "process")
            process_method = wrapper(process_method, stream=self._stream)
            setattr(txf, "process", process_method)
            yield txf

    def _wrap_wall_time(self, transforms):
        for txf in transforms:
            process_method = getattr(txf, "process")
            process_method = self._prof(process_method)
            setattr(txf, "process", process_method)
            yield txf

    def _profile_wall_time_per_line(self, transforms, iterations, **_):
        self._prof = cpu_utils.KLineProfiler()
        wrapped_transforms = self._wrap_wall_time(transforms)

        self._run_pipeline(wrapped_transforms, iterations=iterations)

        if self.output_file:
            return self._prof.print_stats(self.output_file, output_unit=1)

        # output_unit = 1 second, meaning the numbers in "Time" and
        # "Per Hit" columns are in seconds
        self._prof.print_stats(output_unit=1)

    def _profile_memory_per_line(self, transforms, get_maximum=False):
        self._prof = memory_utils.KMemoryLineProfiler(backend="psutil")
        wrapped_transforms = self._wrap_memory_per_line(
            transforms, get_maximum
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
            self._run_pipeline(wrapped_transforms)

            if get_maximum:
                memory_profiler.show_results(self._prof, stream=self._stream)

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

    def _run_pipeline(self, transforms, iterations=None, **_):
        if not iterations:
            iterations = 1
        transforms = wrappers.print_user_exceptions(transforms)
        options = pipeline_options.PipelineOptions()
        options.view_as(pipeline_options.StandardOptions).runner = "direct"
        options.view_as(pipeline_options.SetupOptions).save_main_session = True

        pipeline = beam.Pipeline(options=options)

        if self.input_file:
            entity_ids = pipeline.apply(
                beam.io.ReadFromText(self.input_file), pipeline
            )
        else:
            entity_ids = pipeline.apply(beam.Create(self.entity_ids), pipeline)

        scaled_entity_ids = entity_ids.apply(
            beam.FlatMap(lambda x: [x] * iterations)
        )

        for transform in transforms:
            scaled_entity_ids.apply(beam.ParDo(transform()))

        result = pipeline.run()
        # since on direct runner
        result.wait_until_finish()

    def _get_transforms(self):
        transforms_module = imp.load_source(
            "transforms", KlioPipeline.TRANSFORMS_PATH
        )
        # TODO: temporary! remove me once profiling is supported for v2
        if not hasattr(klio_transforms, "KlioBaseDoFn"):
            logging.error("Profiling V2 klio jobs is not yet available.")
            raise SystemExit(1)

        for attribute in dir(transforms_module):
            obj = getattr(transforms_module, attribute)
            if inspect.isclass(obj) and issubclass(
                obj, klio_transforms.KlioBaseDoFn
            ):
                yield obj

    def profile(self, what, **kwargs):
        transforms = self._get_transforms()

        if what == "run":
            return self._run_pipeline(transforms, **kwargs)
        elif what == "cpu":
            return self._profile_cpu(**kwargs)
        elif what == "memory":
            return self._profile_memory(**kwargs)
        elif what == "memory_per_line":
            return self._profile_memory_per_line(transforms, **kwargs)
        elif what == "timeit":
            return self._profile_wall_time_per_line(transforms, **kwargs)
