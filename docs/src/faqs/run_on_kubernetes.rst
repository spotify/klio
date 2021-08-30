.. _run-on-k8s:

How do I run a Klio job on Kubernetes?
======================================

As of version ``21.8.0``, Klio supports running a streaming job directly on Kubernetes with a new runner called ``DirectGKERunner``.
So far, this has only been tested using `Google Kubernetes Engine`_ (GKE), but *the support should work on any Kubernetes cluster*.

.. caution::

    The new ``DirectGKERunner`` should be considered **beta**.
    However, it has been found to be extremely performant, and particularly helps jobs processing large/long media files.

Under the hood, when using the new ``DirectGKERunner``, Klio creates a Kubernetes `deployment`_.
When Kubernetes starts the containers for the deployment,
it runs the Klio job in ``DirectRunner`` mode (just like you would locally with ``klio job run --direct-runner``).

The difference between ``DirectGKERunner`` and ``DirectRunner`` is that the latter automatically acknowledges messages it reads from the Pub/Sub queue before running it through the defined pipeline.
The logic in the direct runner has been adapted to acknowledge the message once it’s considered done running through the pipeline
(i.e. either it successfully completed the pipeline, was filtered out, or was dropped due to an error in processing).
Without this ability, if a container gets OOM-killed or the job gets otherwise interrupted,
then the failing or in-progress messages do not return to the Pub/Sub queue for re-delivery.
Those messages would be lost.

Limitations
-----------

Using the ``DirectGKERunner`` for a Klio job comes with inherent limitations:

**The Klio job must:**

* be a streaming job (and therefore read event input from a Pub/Sub subscription or topic)
* not need any aggregation transforms (e.g. ``GroupByKey``, ``CombinePerKey``, ``Distinct``, etc). ``Reshuffle`` & ``Flatten`` are also not supported.
* not process :ref:`non-KlioMessages <allow-non-klio>` - if this is a need, please let file an `issue`_.

**Other caveats:**

* Unlike Dataflow, there is no nice UI like Dataflow’s job page. Logging and metrics should be relied upon for observability into the job's progress.
* Since it's built off of the direct runner, ``DirectGKERunner`` includes the same `additional checks`_ at the cost of performance. However, our benchmarks have shown that the ``DirectGKERunner`` is more performant than when running on Dataflow.
* Processing items concurrently is not supported with the ``DirectGKERunner``; it will process one element at a time per replica/pod before consuming another. This allows replicas to be as small as needed to process a single element, and relies on the scalability of Kubernetes for concurrency. Users are still able to use multithreading, multiprocessing, and subprocesses within their job, though.

Job Setup
---------

.. note::
    The following instructions assumes your local environment is setup for deploying workloads to your Kubernetes cluster(s).

Pre-requisites
^^^^^^^^^^^^^^

* ``kubectl`` installed (`instructions`_)
* A Kubernetes cluster to deploy to
* If needed, a Kubernetes namespace

Step 1: Update Klio CLI package versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Update your ``klio-cli`` installation to (at least) ``21.8.0`` with the ``kubernetes`` `extras installation`_:

.. code-block:: sh

    pip install "klio-cli[kubernetes]>=21.8.0"

Step 2: Update Klio package versions in job
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similarly to the Klio CLI update, you will need to update your job's dependency on ``klio-exec`` in ``job-requirements.txt``:

.. code-block::

    klio-exec>=21.8.0

Step 3: Update Dockerfile
^^^^^^^^^^^^^^^^^^^^^^^^^

1. At the end of the job's ``Dockerfile``, add the following two lines:

.. code-block:: Docker

    ENTRYPOINT ["klioexec"]
    CMD ["run"]

2. Be sure to also have these two lines somewhere in the ``Dockerfile`` (if they don't exist already):

.. code-block:: Docker

    ARG KLIO_CONFIG=klio-job.yaml
    COPY $KLIO_CONFIG klio-job-run-effective.yaml


Step 4: Update ``klio-job.yaml``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Under ``pipeline_options``:

.. code-block:: yaml

    pipeline_options:
      runner: DirectGKERunner
      ...


Optionally, you can also remove the following keys if they're set (they're otherwise ignored):

.. code-block:: yaml

    pipeline_options:
      disk_size_gb: ...
      experiments: ...
      max_num_workers: ...
      subnetwork: ...
      region: ...
      worker_disk_type: ...
      worker_machine_type: ...

Step 5: Create a Service Account
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::
    This step is required when using `GKE`_.
    If not using GKE, then you may need to setup authentication between your job and the other resources it uses (Pub/Sub, logging, etc.).

A service account is needed for your GKE job to be able to access other GCP resources (Pub/Sub, Logs, etc).

You may choose to create one service account for all jobs running on GKE,
or create an individual service account for each GKE job.

Step 5.1: Creating a Service Account JSON Key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Within your GCP project, go to `IAM & Admin > Service Accounts`_.
2. Click "+ Create Service Account" at the top.
3. Fill in the Service Account Details, and then click "Create and Continue".
4. Add the following roles, then click "Continue" (you can also add these roles later):
    a. Pub/Sub Subscriber
    b. Logs Writer
    c. Service Account User
    d. If reading input data from GCS: Storage Object Viewer
    e. If reading input data from GCS: Storage Admin (needed to read buckets)
    f. If writing output data to GCS: Storage Object Creator
    g. If publishing event output to a Pub/Sub topic: Pub/Sub Publisher

5. Click "Done". Once successfully created, you'll be redirected to the Service Accounts list page.
6. Search for the newly created Service Account and click on the email address to open the Details page.
7. At the top, click on the "Keys" tab.
8. Click the "Add Key" drop-down, and select "Create New Key". Then select JSON for key type.
9. A JSON file should be automatically downloaded to your computer.

Step 5.2: Generate a Kubernetes Secret with the Service Account
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the following command, filling in the variables where needed:

.. code-block:: sh

    kubectl create secret generic $KEY_NAME \
    --from-file=$PROD_FILE_NAME=$PATH_TO_LOCAL_FILE \
    --namespace $YOUR_NAMESPACE

* The ``$KEY_NAME`` is used in ``kubernetes/deployment.yaml`` so the service account will get pulled into the deployment (see towards the bottom of the ``deployment.yaml`` file :ref:`below <k8s-step-6>` for more info). This can be something like ``my-klio-jobs-service-account``.
* The ``$PROD_FILE_NAME`` is the name of the file that will get mounted as into the job’s container. This can be something like ``key.json``.
* The ``$PATH_TO_LOCAL_FILE`` refers to - you guessed it - the file that was downloaded when creating the JSON key for the service account.
* The ``$YOUR_NAMESPACE`` is your Kubernetes namespace, if the cluster requires.

Step 5.3: Delete local key
~~~~~~~~~~~~~~~~~~~~~~~~~~

Now that the key has been encrypted & uploaded for use, delete the local JSON key of the service account.

.. _k8s-step-6:

Step 6: Create a ``kubernetes/deployment.yaml`` file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In your job's directory, create a ``kubernetes`` directory with a ``deployment.yaml`` file inside.

Copy & paste the following into that new ``kubernetes/deployment.yaml`` file, and fill in the ``$VARIABLES``:

.. code-block:: yaml

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      namespace: $YOUR_NAMESPACE  # if cluster requires
      name: $JOB_NAME # name of job as defined in klio-job.yaml::job_name
      labels:
        app: $JOB_NAME # name of job as defined in klio-job.yaml::job_name
        # Add any more labels needed.
    spec:
      # Set the number of replicas/workers your job requires.
      # Replicas can be considered equivalent to `pipeline_options.num_workers` in
      # `klio-job.yaml`.
      replicas: $NUM_OF_REPLICAS # this is equivalent to `pipeline_options.num_workers`
      strategy:
        # `Recreate` will tear down all pods before redeploying. This is useful when
        # you don't want a mix of old and new deployments (e.g. two different versions
        # of an image).
        # `RollingUpdate` is the other option.
        # More information can be found here:
        # https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy
        type: Recreate
      selector:
        matchLabels:
          app: $JOB_NAME # name of job as defined in klio-job.yaml::job_name
          # any other labels to match the deployment to the pod
      template:
        metadata:
          labels:
            app: $JOB_NAME # name of job as defined in klio-job.yaml::job_name
        spec:
          containers:
          # custom name of container - helpful for using `kubectl` to
          # observe the deployment. This may be the same “base name” in $GCR_IMAGE
          # without the “gcr.io/<project>/” prefix.
          - name: $IMAGE_NAME
            # `image` must match `pipeline_options.worker_harness_container_image`
            # in `klio-job.yaml`.
            # Coming soon: Klio dynamically filling this in automatically.
            image: $GCR_IMAGE # GCR URL but make sure there is no image tag
              resources:
                requests:
                  cpu: $CPU_REQ # CPU that the container is guaranteed to get
                  memory: $MEM_REQ # Memory that the container is guaranteed to get
                limits:
                  cpu: $CPU_LIM # Limit where your container starts getting throttled
                  # May want to increase the limits of memory if the job will be
                  # handling the occasional really long audio.
                  memory: $MEM_LIM # Limit where container gets OOM-killed & restarted
            volumeMounts:
            # Mount job's service account
            - name: $SECRET_NAME # must match below in volumes.name
              mountPath: /var/secrets/google
            env:
            # ENVVAR needed so that Klio picks up the service account
            - name: GOOGLE_APPLICATION_CREDENTIALS
              value: /var/secrets/google/$PROD_FILE_NAME # prod file name from Step 5.2.
          volumes:
            # Include job's service account in the deployment
            # See Step 5 for instructions on setting up a service account.
          - name: $SECRET_NAME # must match above in `volumeMounts.name`
            secret:
              secretName: $KEY_NAME # key name given in Step 5.2.

Depending on your Kubernetes setup, you may want to add more containers such utility/sidecar containers,
other environment variables, mounts, probes, etc.
The above is what's considered the minimum for a deployment of a Klio job.

If your job needs to be highly available, read :ref:`below <limiting-disruption>` on how to limit the amount of concurrent disruptions.

.. todo::
    Once written, add link somewhere here to info on how to setup autoscaling.

Step 7: Run the job
^^^^^^^^^^^^^^^^^^^

After completing all the above, you can deploy the job via ``klio job run [OPTIONS]``.

Some suggestions to test out the deployment:

* Start with a small number of replicas in ``kubernetes/deployment.yaml`` to make sure the job runs smoothly first.
* You may want to test the job with large files to see if you need to request more memory.
* If you’re running this Klio job in production right now, and don't want to affect traffic before you're ready to cut over, create a new subscription to the Pub/Sub topic for the Kubernetes-based job. This will allow the Kubernetes job to get the same traffic as the production job. You may want to update the event output and/or the data output location if you don't want to overwrite the production outputs.
* Once it looks all good, you can update the ``kubernetes/deployment.yaml`` file to the number of replicas needed and/or the resources (memory, CPU) needed. Run ``klio job run --update`` to update the existing job without taking it down.


Helpful Tips
------------

``klio`` Commands
^^^^^^^^^^^^^^^^^

* ``klio job run ...`` will run the job on Kubernetes when ``DirectGKERunner`` is set as the runner in ``klio-job.yaml``. It is similar to running ``kubectl apply -f kubernetes/deployment.yaml``.
* ``klio job run --update ...`` will update the deployment in Kubernetes (for example with new image tag, or an otherwise updated ``kubernetes/deployment.yaml``, etc).
* ``klio job stop`` will bring the number of replicas to 0, but does not delete the deployment. This allows you to still see the job's deployment on GKE.
* ``klio job deploy ...`` just runs klio job stop and then ``klio job run ...``.
* ``klio job delete`` will delete the entire deployment (equivalent to ``kubectl delete -f kubernetes/deployment.yaml``).

``kubectl`` Commands
^^^^^^^^^^^^^^^^^^^^

For working with ``kubectl`` commands, you'll need some specifics from your ``kubernetes/deployment.yaml`` file:

* ``$APP_LABEL`` can be found in ``spec.template.metadata.labels.app``
* ``$IMAGE_NAME`` can be found in ``spec.template.spec.containers[0].name`` (not the ``GCR_IMAGE``)
* ``$NUM_OF_REPLICAS`` can be found in ``spec.replicas``

.. note::
    If your cluster uses namespaces, be sure to include ``--namespace $YOUR_NAMESPACE`` to any ``kubectl`` command.

Status of deployment
~~~~~~~~~~~~~~~~~~~~

From within the job's directory:

.. code-block:: sh

    kubectl describe -f kubernetes/deployment.yaml

As well, getting the events may be helpful:

.. code-block:: sh

    kubectl get events --sort-by='.lastTimestamp'

This should include all events, including any autoscaling setup.

View a job's logs
~~~~~~~~~~~~~~~~~
You can view logs locally:

.. code-block:: sh

    kubectl logs -l app=$APP_LABEL --container=$IMAGE_NAME

Add ``--follow=true`` to tail the logs, and ``--timestamps=true`` to include logs' timestamps.

Drop ``--container=$IMAGE_NAME`` and replace it with ``--all-containers`` if you want to follow the logs for other containers on the pods,
like any sidecars you may have.

If you have more than 10 replicas/pods, you'll want to add ``--max-log-requests=$NUM_OF_REPLICAS`` to be able to grab the logs of all pods.

Job's CPU and memory usage
~~~~~~~~~~~~~~~~~~~~~~~~~~

To view a snapshot of each container's CPU and memory usage:

.. code-block:: sh

    kubectl top pod -l app=$APP_LABEL --containers | grep $IMAGE_NAME

Omit the ``--containers | grep $IMAGE_NAME`` to include other containers on your pods
(e.g. any sidecars for your deployment).

"ssh"/exec into a container or run a one-off command
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, find the names of the pods:

.. code-block:: sh

    kubectl get pods \
      -l app=$APP_LABEL \
      --no-headers \
      -o custom-columns=":metadata.name"

Then, you can either exec into the container directly (replacing ``$POD_NAME`` with one of the pod names from the previous command):

.. code-block:: sh

    kubectl exec $POD_NAME --container $IMAGE_NAME -it -- bash

Or, run a one-off command, like ``ps aux`` (replacing ``$POD_NAME`` with one of the pod names from the previous command):

.. code-block:: sh

    kubectl exec $POD_NAME --container $IMAGE_NAME -- ps aux


Code Suggestions
^^^^^^^^^^^^^^^^

Additional Loggers
~~~~~~~~~~~~~~~~~~

A couple of loggers were added with the ``DirectGKERunner`` support.
Some of them are noisy, but can be helpful when debugging or trying to deploy a job for the first time.
The following loggers will give some insight into the progress of each consumed Pub/Sub message:

* ``klio.gke_direct_runner.heartbeat``
* ``klio.gke_direct_runner.message_manager``

Examples of these logs:

.. code-block::

    DEBUG:klio.gke_direct_runner.message_manager:Received d34db33f from Pub/Sub.
    DEBUG:klio.gke_direct_runner.message_manager:Extended Pub/Sub ack deadline for PubSubKlioMessage(kmsg_id=d34db33f) by 30s
    DEBUG:klio.gke_direct_runner.message_manager:Skipping extending Pub/Sub ack deadline for PubSubKlioMessage(kmsg_id=d34db33f)
    INFO:klio.gke_direct_runner.heartbeat:Job is still processing d34db33f…
    INFO:klio.gke_direct_runner.message_manager:Acknowledged d34db33f. Job is no longer processing this message.

To make sure those logs are actually seen, add the following to your ``run.py``:

.. code-block:: py

    import logging
    import apache_beam as beam
    import transforms

    logging.getLogger("klio.gke_direct_runner.heartbeat").setLevel(logging.DEBUG)
    logging.getLogger("klio.gke_direct_runner.message_manager").setLevel(logging.DEBUG)


    def run(input_pcol, job_config):
        ...


Without the above, only warning and error messages will show.
You may also choose to set the level to ``logging.INFO`` to ignore the debug-level logs.

Metrics
~~~~~~~

Since running a job on GKE does not have the nice Dataflow Job UI with the job's graph,
Klio now emits some :ref:`metrics by default <metrics>`, but you may wish to add your own metrics too with custom metrics.

For example, this Downsample transform keeps track of successful downloads, successful uploads,
the time it takes to download, and a gauge on the memory footprint of the loaded file:

.. code-block:: py

    import tempfile

    import apache_beam as beam
    import librosa
    import numpy as np

    from klio.transforms import decorators


    class DownsampleFn(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
            self.output_dir = self._klio.config.job_config.data.outputs[0].location
            self.client = self._setup_client()
            self.dnl_success_ctr = self._klio.metrics.counter(
                "download-success", transform="DownsampleFn"
            )
            self.upl_success_ctr = self._klio.metrics.counter(
                "upload-success", transform="DownsampleFn"
            )
            self.dnl_timer = self._klio.metrics.timer(
                "download-timer", transform="DownsampleFn", timer_unit="seconds"
            )
            self.entity_memory_gauge = self._klio.metrics.gauge(
                "entity-memory", transform="DownsampleFn"
            )

        @decorators.set_klio_context
        def _setup_client(self):
            # snip

        @decorators.handle_klio
        def process(self, data):
            entity_id = data.element.decode("utf-8")
            self._klio.logger.info(f"DownsampleFn processing {entity_id}")

            with tempfile.TemporaryDirectory() as tmp_dirname:
                with self.dnl_timer:
                    audio_file = self.client.download(
                        entity_id=entity_id, output_directory=tmp_dirname,
                    )
                self.dnl_success_ctr.inc()

                y, sr = librosa.load(audio_file)

                downsampled_y = y[::2]
                downsampled_rate = sr / 2

                memory_footprint = downsampled_y.nbytes
                self.entity_memory_gauge.set(memory_footprint)

                tmp_out_path = f"{tmp_dirname}/output.npz"
                np.savez(tmp_out_path, y=downsampled_y, sr=downsampled_rate)

                output_file_path = f"{self.output_dir}/{entity_id}.npz"
                self.client.upload_from_filename(tmp_out_path, output_file_path)
                self.upl_success_ctr.inc()

            yield data

This is just an example of what can be done in a job.
Please refer to the :ref:`Klio docs on metrics <metrics>` for more info.


.. _limiting-disruption:

Limiting Disruption
-------------------

If you want your job to be highly available with a limited amount of downtime,
it's advisable to set up a `budget for pod disruptions`_.

One can configure the amount of concurrent "disruptions" that a deployment experiences.
Disruptions can be:

* When vertical autoscaling tears down (evicts) pods to bring up new pods with new resource requirements & limitations;
* Involuntary disruptions that out of our control, like hardware failure, cluster maintenance gone wrong, node being out of resources, etc
* New Docker image for deployments

A "Pod Disruption Budget" (a separate YAML file) can then configure the following to minimize disruptions
(further docs on how to configure a `budget for pod disruptions`_):

* minimum number of pods available
* maximum number of pods unavailable

Separately, a Deployment can have configuration for:

* `Replacement strategy`_, e.g. when deploying an updated Docker image;
* `Progress deadline seconds`_, e.g. when to mark a deployment has failed progressing;
* `Termination grace period`_, e.g. when a pod is requested to terminate gracefully, how much time should k8s gives before forcefully terminating.

.. _Google Kubernetes Engine: https://cloud.google.com/kubernetes-engine
.. _GKE: https://cloud.google.com/kubernetes-engine
.. _deployment: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/
.. _issue: https://github.com/spotify/klio/issues
.. _additional checks: https://beam.apache.org/documentation/runners/direct/
.. _instructions: https://kubernetes.io/docs/tasks/tools/
.. _extras installation: https://packaging.python.org/tutorials/installing-packages/#installing-setuptools-extras
.. _IAM & Admin > Service Accounts: https://console.cloud.google.com/iam-admin/serviceaccounts
.. _budget for pod disruptions: https://kubernetes.io/docs/tasks/run-application/configure-pdb/
.. _Replacement strategy: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#strategy
.. _Progress deadline seconds: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#progress-deadline-seconds
.. _Termination grace period: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination
