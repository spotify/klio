# Copyright 2020 Spotify AB

import enum
import inspect
import logging
import warnings

# black conflicts with flake8 here
# fmt: off
import apache_beam as beam
import attr
try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    # can be a problem on MacOS w certain Python & OS versions
    # https://stackoverflow.com/a/34583958/1579977
    import matplotlib
    matplotlib.use("TkAgg")
    import matplotlib.pyplot as plt
import numpy as np
# fmt: on

from klio.transforms import decorators


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


class ProfileObjectType(enum.Enum):
    DOFN = "dofn"
    FUNCTION = "function"
    METHOD = "method"


@attr.attrs
class ProfileObject(object):
    """Wraps classes, methods, functions for profiling.  Takes care of putting
    them in a proper beam transform, if necessary."""

    obj_type = attr.attrib()
    obj = attr.attrib()

    @classmethod
    def from_object(cls, obj):
        if inspect.isclass(obj) and issubclass(obj, beam.DoFn):
            return ProfileObject(ProfileObjectType.DOFN, obj)
        elif callable(obj):
            args = inspect.getfullargspec(obj).args
            if len(args) == 1:
                return ProfileObject(ProfileObjectType.FUNCTION, obj)
            elif len(args) == 2:
                if args[0] == "self":
                    warnings.warn(
                        "profiling methods may not work, self will be None!"
                    )
                    return ProfileObject(ProfileObjectType.METHOD, obj)
                else:
                    warnings.warn(
                        "profiling 2-argument function, assuming first argument"
                        "is klio context, which will be None when profiling"
                    )
                    # technically not a method, but works the same way
                    return ProfileObject(ProfileObjectType.METHOD, obj)
            else:
                raise Exception(
                    "cannot profile function with {} arguments".format(
                        len(args)
                    )
                )
        else:
            raise Exception(
                (
                    "Attempting to profile unrecognized object {}"
                    ", only DoFn classes and single-argument"
                    "methods/functions supported"
                ).format(obj)
            )

    def _name_entity_from_args(self, args):
        if self.obj_type == ProfileObjectType.DOFN:
            transform_name = args[0].__class__.__name__
            entity_id = args[1]
        elif self.obj_type == ProfileObjectType.FUNCTION:
            transform_name = "function"
            entity_id = args[0]
        elif self.obj_type == ProfileObjectType.METHOD:
            transform_name = "method"
            entity_id = args[1]
        return {"transform_name": transform_name, "entity_id": entity_id}

    def to_wrapped_transform(self, wrapper_fn):
        if self.obj_type == ProfileObjectType.DOFN:
            process_method = getattr(self.obj, "process")
            process_method = wrapper_fn(process_method)
            setattr(self.obj, "process", process_method)
            return beam.ParDo(self.obj())
        elif self.obj_type == ProfileObjectType.FUNCTION:
            wrapped = wrapper_fn(self.obj)
            return beam.Map(wrapped)
        else:
            wrapped = wrapper_fn(self.obj)
            return beam.Map(lambda x: wrapped(None, x))


def load_profile_objects():
    # NOTE: this function assumes user code has already been loaded, so that
    # anything using the `profile` decorator is already registered
    for raw_obj in decorators.PROFILE_OBJECTS:
        yield ProfileObject.from_object(raw_obj)
