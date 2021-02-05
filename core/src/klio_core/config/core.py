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

import logging
import os

import attr
import yaml

from klio_core.config import _io as io
from klio_core.config import _preprocessing as preprocessing
from klio_core.config import _utils as utils

logger = logging.getLogger("klio")

WORKER_DISK_TYPE_URL = (
    "compute.googleapis.com/projects/{project}/regions/{region}/"
    "diskTypes/{disk_type}"
)


@utils.config_object(key_prefix=None, allow_unknown_keys=False)
class BaseKlioConfig(object):
    """Base Klio config representation of ``klio-job.yaml``.

    Args:
        config_dict (dict): dictionary representation of configuration
            as parsed from ``klio-job.yaml``.
    """

    job_name = attr.attrib(type=str)

    job_config = attr.attrib(repr=False)
    pipeline_options = attr.attrib(repr=False, default={})

    version = attr.attrib(type=int, default=2, repr=False)

    def __config_post_init__(self, config_dict):
        self._raw = config_dict

        self.job_config = KlioJobConfig(
            self.job_config, job_name=self.job_name, version=self.version,
        )
        self.pipeline_options = KlioPipelineConfig(
            self.pipeline_options,
            job_name=self.job_name,
            version=self.version,
        )

    def __repr__(self):
        return "KlioConfig(job_name=%s)" % self.job_name

    def as_dict(self):
        job_config_dict = self.job_config.as_dict()
        pipeline_opts_dict = self.pipeline_options.as_dict()
        base_config_dict = attr.asdict(self)
        base_config_dict["job_config"] = job_config_dict
        base_config_dict["pipeline_options"] = pipeline_opts_dict
        return base_config_dict


class KlioConfig(BaseKlioConfig):
    """Klio config object representation of ``klio-job.yaml``.

    Args:
        config_dict (dict): dictionary representation of configuration
            as parsed from ``klio-job.yaml``.

    Attributes:
        job_name (str): Name of Klio job.
        version (int): Version of Klio job.
        pipeline_options (KlioPipelineConfig): Apache Beam pipeline-related
            configuration.
        job_config (KlioJobConfig): Job-related configuration.
    """

    def __init__(
        self,
        raw_config_data,
        raw_overrides=None,
        raw_templates=None,
        config_skip_preprocessing=False,
        **kwargs
    ):
        if config_skip_preprocessing:
            processed_config = raw_config_data
        else:
            processed_config = preprocessing.KlioConfigPreprocessor.process(
                raw_config_data=raw_config_data,
                raw_template_list=raw_templates or [],
                raw_override_list=raw_overrides or [],
            )
        return super().__init__(processed_config, **kwargs)

    # re-defining as_dict just to have its docstring.
    def as_dict(self):
        """Return a dictionary representation of the :class:`KlioConfig`
        object.
        """
        return super().as_dict()

    def write_to_file(self, path_or_stream):
        """write the config object as a yaml file

        Args:
            path_or_stream: either a string of a file path or a writable stream
        """

        def _write(stream):
            yaml.dump(
                self.as_dict(),
                stream=stream,
                Dumper=utils.IndentListDumper,
                default_flow_style=False,
                sort_keys=False,
            )

        if isinstance(path_or_stream, str):
            dirname = os.path.dirname(path_or_stream)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(path_or_stream, "w+") as stream:
                _write(stream)
        else:
            _write(path_or_stream)


@attr.attrs
class KlioIOConfigContainer(object):
    inputs = attr.attrib()
    outputs = attr.attrib()


@utils.config_object(key_prefix="job_config", allow_unknown_keys=True)
class KlioJobConfig(object):
    """Job-specific config representing the ``job_config`` key of
    ``klio-job.yaml``.

    ``job_config`` is both for any user-specific job configuration needed,
    as well as klio-related configuration (i.e. timeouts, metrics).

    See :ref:`documentation <job-config>` for information on available
    configuration.

    Attributes:
        job_name (str): Name of Klio job.
        version (int): Version of Klio job.
        allow_non_klio_messages (bool): Allow this job to process free-form,
            non-KlioMessage messages.
        blocking (bool): Wait for Dataflow job to finish before exiting.
        metrics (dict): Dictionary representing desired metrics configuration.
        events (``KlioIOConfigContainer``): Job event I/O configuration.
        data (``KlioIOConfigContainer``): Job data I/O configuration.

    Args:
        config_dict (dict): dictionary representation of ``job_config``
            as parsed from ``klio-job.yaml``.
    """

    ATTRIBS_TO_SKIP = ["version", "job_name"]

    # required attributes
    job_name = utils.field(type=str, repr=True)
    version = utils.field(type=int)

    # optional attributes
    allow_non_klio_messages = utils.field(type=bool, default=False)
    metrics = utils.field(default={})
    blocking = utils.field(type=bool, default=False)

    def __config_post_init__(self, config_dict):
        self._raw = config_dict
        self._scanned_io_subclasses = None
        self.USER_ATTRIBS = []

        self._parse_io(config_dict)

        declared_config = self._as_dict()
        for key, value in self._raw.items():
            if key not in declared_config:
                # set user attributes to job_config - but only the first
                # level, for example, from this
                # job_config:
                #    foo:
                #      key1: value1
                #      list1:
                #        - one
                #        - two
                # gets parsed to:
                # job_config.foo -> {"key1": "value1", "alist": ["one", "two"]}
                setattr(self, key, value)
                # key track of user-set attributes so that when as_dict()
                # is called, we re-add it to what _as_dict() returns
                self.USER_ATTRIBS.append({key: value})

    def _parse_io(self, config_dict):
        event_inputs = self._create_config_objects(
            config_dict.get("events", {}).get("inputs", {}),
            io.KlioIOType.EVENT,
            io.KlioIODirection.INPUT,
        )
        event_outputs = self._create_config_objects(
            config_dict.get("events", {}).get("outputs", {}),
            io.KlioIOType.EVENT,
            io.KlioIODirection.OUTPUT,
        )
        self.events = KlioIOConfigContainer(
            inputs=event_inputs, outputs=event_outputs
        )

        data_inputs = self._create_config_objects(
            config_dict.get("data", {}).get("inputs", {}),
            io.KlioIOType.DATA,
            io.KlioIODirection.INPUT,
        )
        data_outputs = self._create_config_objects(
            config_dict.get("data", {}).get("outputs", {}),
            io.KlioIOType.DATA,
            io.KlioIODirection.OUTPUT,
        )
        self.data = KlioIOConfigContainer(
            inputs=data_inputs, outputs=data_outputs
        )

    def _get_all_config_subclasses(self):
        if self._scanned_io_subclasses is not None:
            return self._scanned_io_subclasses

        # notice this will end up including intermediate classes but that
        # shouldn't matter since they shouldn't "support" any valid
        # combinations of type/direction
        all_subclasses = []

        def traverse(cls):
            for subclass in cls.__subclasses__():
                all_subclasses.append(subclass)
                traverse(subclass)

        traverse(io.KlioIOConfig)
        self._scanned_io_subclasses = all_subclasses
        return all_subclasses

    def _create_config_objects(self, configs, io_type, io_direction):
        options = dict(
            [
                (x.name, x)
                for x in self._get_all_config_subclasses()
                if x.supports_type(io_type)
                and x.supports_direction(io_direction)
            ]
        )
        objs = []
        for name, config in configs.items():
            type_name = config["type"].lower()
            if type_name not in options:
                raise Exception(
                    "{} is not a valid {} {}".format(
                        config["type"], io_type.name, io_direction.name
                    )
                )
            subclass = options[type_name]
            objs.append(subclass.from_dict(config, io_type, io_direction))
        return objs

    def _as_dict(self):
        config_dict = attr.asdict(
            self, filter=lambda x, _: x.name not in self.ATTRIBS_TO_SKIP
        )
        config_dict["events"] = {}
        config_dict["events"]["inputs"] = [
            ei.as_dict() for ei in self.events.inputs
        ]
        config_dict["events"]["outputs"] = [
            eo.as_dict() for eo in self.events.outputs
        ]

        config_dict["data"] = {}
        config_dict["data"]["inputs"] = [
            di.as_dict() for di in self.data.inputs
        ]
        config_dict["data"]["outputs"] = [
            do.as_dict() for do in self.data.outputs
        ]
        return config_dict

    def as_dict(self):
        """Return a dictionary representation of the :class:`KlioJobConfig`
        object.

        .. tip::

            Use this method to access any custom config key/value pairs
            defined under ``klio-job.yaml::job_config``.
        """

        config_dict = self._as_dict()
        for attrib in self.USER_ATTRIBS:
            for key, value in attrib.items():
                config_dict[key] = value

        return config_dict

    def __repr__(self):
        return "KlioJobConfig(job_name=%s)" % self.job_name


@utils.config_object(key_prefix="pipeline_options", allow_unknown_keys=True)
class KlioPipelineConfig(object):
    """Pipeline-specific config representing the ``pipeline_options`` key
    of ``klio-job.yaml``.

    .. note::

        ``pipeline_options`` map 1:1 to options supported in Apache Beam and
        its runners (i.e. Dataflow). See `all supported pipeline options
        <https://github.com/apache/beam/blob/master/sdks/python/apache_beam/
        options/pipeline_options.py>`_ for available options.

    Any instance attribute not defined in this class but is available in Apache
    Beam or its runners will still be passed through when running the
    pipeline.

    See :ref:`documentation <kliopipelineconfig>` for information on available
    configuration.

    Args:
        config_dict (dict): dictionary representation of ``pipeline_options``
            as parsed from ``klio-job.yaml``.

    Attributes:
        job_name (str): Name of Klio job.
        version (int): Version of Klio job.
        (remaining attributes): See `all supported pipeline options
            <https://github.com/apache/beam/blob/master/sdks/python/
            apache_beam/options/pipeline_options.py>`_ for all available
            remaining attributes.
    """

    ATTRIBS_TO_SKIP = ["version"]

    job_name = utils.field(repr=True, type=str)
    version = utils.field(type=int)

    runner = utils.field(type=str, default="DataflowRunner")
    streaming = utils.field(type=bool, default=True)

    # debug options
    # TODO: add validation - if job is instreaming mode and no setup.py,
    # then config must use the "beam_fn_api" experiment (we could also just
    # automatically put it in there)
    experiments = utils.field(default=[])

    # setup options
    sdk_location = utils.field(type=str, default=None)
    setup_file = utils.field(type=str, default=None)
    requirements_file = utils.field(type=str, default=None)

    # GCP options
    project = utils.field(type=str, default=None)
    staging_location = utils.field(type=str, default=None)
    temp_location = utils.field(type=str, default=None)
    region = utils.field(type=str, default="europe-west1")
    subnetwork = utils.field(type=str, default=None)
    update = utils.field(type=bool, default=False)
    dataflow_endpoint = utils.field(type=str, default=None)
    service_account_email = utils.field(type=str, default=None)
    no_auth = utils.field(type=bool, default=False)
    template_location = utils.field(type=str, default=None)
    labels = utils.field(default=[])
    label = utils.field(type=str, default=None)
    transform_name_mapping = utils.field(type=str, default=None)
    enable_streaming_engine = utils.field(type=bool, default=False)
    dataflow_kms_key = utils.field(type=str, default=None)
    flexrs_goal = utils.field(
        type=str,
        default=None,
        validator=attr.validators.optional(
            attr.validators.in_(["COST_OPTIMIZED", "SPEED_OPTIMIZED"])
        ),
    )

    # worker options
    autoscaling_algorithm = utils.field(
        type=str,
        default="NONE",
        validator=attr.validators.optional(
            attr.validators.in_(["THROUGHPUT_BASED", "NONE"])
        ),
    )
    num_workers = utils.field(type=int, default=2)
    disk_size_gb = utils.field(type=int, default=32)
    worker_machine_type = utils.field(type=str, default="n1-standard-2")
    worker_harness_container_image = utils.field(type=str, default=None)
    worker_disk_type = utils.field(type=str, default=None,)
    use_public_ips = utils.field(type=bool, default=None)
    min_cpu_platform = utils.field(type=str, default=None)
    dataflow_worker_jar = utils.field(type=str, default=None)

    # profiling options
    profile_location = utils.field(type=str, default=None)
    profile_cpu = utils.field(type=bool, default=None)
    profile_memory = utils.field(type=bool, default=None)
    profile_sample_rate = utils.field(type=float, default=None)

    def __config_post_init__(self, config_dict):
        if self.label:
            self.labels.append(self.label)

        self.max_num_workers = max(2, self.num_workers)
        self.USER_ATTRIBS = []

        valid_disk_types = ["local-ssd", "pd-ssd", "pd-standard"]

        def format_disk_type(simple_type):
            return WORKER_DISK_TYPE_URL.format(
                project=self.project,
                region=self.region,
                disk_type=simple_type,
            )

        # worker_disk_type may or may not already be formatted as a URL
        if self.worker_disk_type is not None:
            if self.worker_disk_type in valid_disk_types:
                self.worker_disk_type = format_disk_type(self.worker_disk_type)
            elif self.worker_disk_type != format_disk_type(
                self.worker_disk_type.split("/")[-1]
            ):
                raise ValueError(
                    "Invalid pipeline_options.worker_disk_type: '{}'".format(
                        self.worker_disk_type
                    )
                )

        declared_config = self._as_dict()
        for key, value in config_dict.items():
            if key not in declared_config:
                # set user attributes to job_config - but only the first
                # level, for example, from this
                # pipeline_options:
                #    foo:
                #      key1: value1
                #      list1:
                #        - one
                #        - two
                # gets parsed to:
                # pipeline_options.foo ->
                #   {"key1": "value1", "alist": ["one", "two"]}
                setattr(self, key, value)
                # key track of user-set attributes so that when as_dict()
                # is called, we re-add it to what _as_dict() returns
                self.USER_ATTRIBS.append({key: value})

    def _as_dict(self):
        return attr.asdict(
            self, filter=lambda x, _: x.name not in self.ATTRIBS_TO_SKIP
        )

    def as_dict(self):
        """Return a dictionary representation of the
        :class:`KlioPipelineConfig` object.
        """
        config_dict = self._as_dict()
        for attrib in self.USER_ATTRIBS:
            for key, value in attrib.items():
                config_dict[key] = value

        return config_dict

    def __repr__(self):
        return "KlioPipelineConfig(job_name=%s)" % self.job_name
