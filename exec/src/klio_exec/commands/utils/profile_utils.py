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

import logging

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    # can be a problem on MacOS w certain Python & OS versions
    # https://stackoverflow.com/a/34583958/1579977
    import matplotlib

    matplotlib.use("agg")
    import matplotlib.pyplot as plt
import numpy as np


def _get_profiling_data(filename):
    """Read a given file and parse its content for profiling data."""
    data, timestamps = [], []

    try:
        with open(filename, "r") as f:
            file_data = f.readlines()
    except Exception:
        logging.error("Could not read profiling data.", exc_info=True)
        raise SystemExit(1)

    for line in file_data:
        if line == "\n":
            continue

        line = line.strip()
        line_data = line.split(" ")
        if len(line_data) != 3:
            continue
        _, mem_usage, timestamp = line.split(" ")
        data.append(float(mem_usage))
        timestamps.append(float(timestamp))

    if not data:
        logging.error("No samples to parse in {}.".format(filename))
        raise SystemExit(1)

    return {"data": data, "timestamp": timestamps}


def plot(input_file, output_file, x_label, y_label, title):
    """Plot profiling data."""
    profile_data = _get_profiling_data(input_file)

    data = np.asarray(profile_data["data"])
    timestamp = np.asarray(profile_data["timestamp"])

    global_start = float(timestamp[0])
    t = timestamp - global_start  # start at 0 rather than a specific time

    plt.figure(figsize=(14, 6), dpi=90)
    plt.plot(t, data, "+-c")  # c is for `cyan`
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid()
    plt.savefig(output_file)
