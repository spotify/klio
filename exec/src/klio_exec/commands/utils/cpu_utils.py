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

import sys
import time

import line_profiler as lp
import psutil

from klio_exec.commands.utils import wrappers


class KLineProfiler(wrappers.KLineProfilerMixin, lp.LineProfiler):
    pass


# adapted from memory_profiler's approach:
# github.com/pythonprofilers/memory_profiler/blob/master/memory_profiler.py
def get_cpu_usage(proc, interval=None, stream=None):
    """Measure the cpu % usage at an interval."""
    if not interval:
        interval = 0.1
    if not stream:
        stream = sys.stdout

    line_count = 0
    while True:
        cpu_usage = psutil.cpu_percent()
        timestamp = time.time()
        # Same format as memory_profiler
        stream.write("CPU {:.1f} {:.4f}\n".format(cpu_usage, timestamp))

        time.sleep(interval)
        line_count += 1
        # flush every 50 lines. Make 'tail -f' usable on profile file
        if line_count > 50:
            line_count = 0
            stream.flush()

        if proc.poll() is not None:
            break
