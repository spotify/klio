.. _kliopipelineconfig:

Beam Pipeline Options
=====================

.. important::

    These are just some commonly-used options, but any option that Beam & Dataflow support are also supported here. More information can be found in the `official Beam docs`_.


.. option:: pipeline_options.autoscaling_algorithm STR

    Algorithm in which to scale your job. See `Dataflow's autoscaling documentation`_ for more
    info.

    | **Options**: ``NONE``, ``THROUGHPUT_BASED``
    | **Default**: ``NONE``
    | **Runner**: Dataflow


.. option:: pipeline_options.disk_size_gb INT

    Configure the amount of available storage for a worker running on Dataflow.

    | **Default**: ``32``
    | **Runner**: Dataflow


.. _experiments:
.. option:: pipeline_options.experiments[] LIST(STR)

    Flags that enable Beam to run experimental features.

    Set this value to an empty list if the default values are not needed.

    | **Default**: ``enable_stackdriver_agent_metrics``, ``beam_fn_api``.
    | **Runner**: Dataflow

    .. note::

        Experiments are correlated to specific runners. Valid experiment values for the Dataflow
        runner can be found in their `documentation <https://cloud.google.com/dataflow/docs/guides/
        specifying-exec-params>`_ (not limited to that linked page).


.. option:: pipeline_options.max_num_workers INT

    Configure the maximum number of workers that will try to run your job at any given time on
    Dataflow.

    | **Default**: ``2``
    | **Runner**: Dataflow

    .. note::

        Only relevant when ``pipeline_options.autoscaling_algorithm`` is not ``NONE``.


.. option:: pipeline_options.num_workers INT

    The number of workers that your job will run on Dataflow.

    | **Default**: ``2``
    | **Runner**: Dataflow


.. option:: pipeline_options.project STR

    GCP project within which this job will be run.

    **Runner**: Dataflow *(Required)*


.. option:: pipeline_options.region STR

    GCP region where this job will be run on Dataflow (`supported regions`_).

    | **Default**: ``europe-west1``.
    | **Runner**: Dataflow
    | *Required*


.. option:: pipeline_options.runner STR

    Specify which runner should be used to execute the job.

    | **Options**: ``DirectRunner``, ``DataflowRunner``
    | **Default**: ``DataflowRunner``
    | *Required*


.. option:: pipeline_options.setup_file STR

    Path to ``setup.py`` file relative to a Klio job's ``run.py`` file.

    | **Runner**: Dataflow

    .. note::

        This configuration attribute is mutually exclusive with the ``beam_fn_api``
        :ref:`experiment <experiments>`.


.. option:: pipeline_options.staging_location STR

    A Cloud Storage path for Cloud Dataflow to stage code packages needed by workers executing the
    job. Must be a valid Cloud Storage URL, beginning with ``gs://``.

    If not set, defaults to a staging directory within ``temp_location``. At least one of
    ``temp_location`` or ``staging_location`` must be specified.

    **Runner**: Dataflow

    .. note::

        The commands ``klio job create`` and ``klio job verify --create-resources`` will create
        this bucket for you.

.. _kliopipelineconfig-streaming:

.. option:: pipeline_options.streaming BOOL

    If ``True``, the pipeline reads from an unbounded source (a.k.a. Pub/Sub) and will always be
    "up"; a streaming job will process data from a source and will continue working until it is
    shut down. If ``False``, it designates the job as a batch job.

    | **Default**: ``True``.


.. option:: pipeline_options.temp_location STR

    A Cloud Storage path for Cloud Dataflow to stage temporary job files created during the
    execution of the pipeline. Must be a valid Cloud Storage URL, beginning with ``gs://``.

    If not set, defaults to a staging directory within ``staging_location``. At least one of
    ``temp_location`` or ``staging_location`` must be specified.

    **Runner**: Dataflow

    .. note::

        The commands ``klio job create`` and ``klio job verify --create-resources`` will create
        this bucket for you.


.. option:: pipeline_options.worker_harness_container_image STR

    Regardless of the configured runner, Klio will build an image and tag it with the configured
    value set here.

    For all runners, Klio uses this image as the "`driver`_" of a pipeline (i.e. to start/launch
    the pipeline).

    When running a pipeline with ``DirectRunner``, the entire execution model (the
    "`runner`_" and the "`worker`_") runs within this image as well. Essentially the driver,
    runner, and worker all run on one container.

    With Dataflow, the workers (synonymous with "instance" or "host") will download the image from
    `Google Container Registry`_ (GCR) to then use as the runtime environment for the pipeline's
    transforms. Each worker will then run as many containers as configured (default is equal to
    the number of the workers's CPUs).

    | **Runner**: Dataflow, Direct (*Required*)

    .. note::

        When using Dataflow, the name of the image must be a URI for `Google Container Registry`_.

        For example: ``gcr.io/my-project/my-klio-job-image``.

    .. note::

        Image version tags may be included here (e.g. ``my-klio-job-image:v1``) but by default,
        Klio takes care of these image tags via the ``klio-cli`` when uploading and deploying.


.. option:: pipeline_options.worker_disk_type STR

    Configure the disk type for a worker running on Dataflow.

    | **Options**: ``pd-standard``, ``pd-ssd``, ``local-ssd``
    | **Default**: ``pd-standard``
    | **Runner**: Dataflow


.. option:: pipeline_options.worker_machine_type STR

    Configure the worker type for a worker running on Dataflow. See `available machine types`_.

    | **Default**: ``n1-standard-2``
    | **Runner**: Dataflow




.. _official Beam docs: https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
.. _Dataflow's autoscaling documentation: https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#autoscaling
.. _available machine types: https://cloud.google.com/compute/docs/machine-types
.. _Pipeline: https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline
.. _PCollection: https://beam.apache.org/documentation/programming-guide/#pcollections
.. _driver: https://beam.apache.org/documentation/programming-guide/#overview
.. _runner: https://beam.apache.org/documentation/runtime/model/
.. _worker: https://beam.apache.org/documentation/runtime/model/
.. _Google Container Registry: https://cloud.google.com/container-registry
.. _supported regions: https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#locations
