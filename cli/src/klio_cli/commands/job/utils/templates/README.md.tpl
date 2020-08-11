# {{ klio.job_name }}

#### _Notice!_

This `README.md` has been automatically generated via `klio job create`. You may edit/delete the contents of this `README.md`.

Please see the [full Klio documentation][klio_docs.todo] for more detail and a quick-start example.

## System Requirements

If not already, please install the following system requirements:

* Python 3.5, 3.6, or 3.7
* Docker ([macOS][docker_mac], [Windows][docker_win], [Linux][docker_linux])
* [gcloud SDK][gcloud]
    * required for authentication when creating [Google Pub/Sub][pubsub] resources, regardless of using the [Dataflow][dataflow] or Apache Beam's [Direct Runner][direct_runner]
    * if using Dataflow, then required for also creating [Cloud Storage][gcs] (GCS) resources, uploading Docker images to [Cloud Container Registry][gcr] (GCR), and deploying jobs to Dataflow)
* `klio-cli` (suggested to be installed via [`pipx`][pipx])

{% if not klio.use_fnapi -%}
## Notice
This job uses `setup.py` for dependency management. Please familiarize yourself with the requirements and limitations stated in the [documentation][setup-py.todo].
{% endif -%}

## Run Klio Locally

Your new Klio job can be run locally with Apache Beam's [direct runner][direct_runner]:

```sh
# in the root of the project directory
$ klio job run --direct-runner
```

Running this command for the first time will build a Docker image of your Klio job, then run the job via Direct Runner within the built image.

Once the job is running, the job will be waiting for input to trigger any work. In order to provide the job with input, you'll need to publish a message to the job (via the Google Cloud Pub/Sub topic/subscription created during `klio job create` and defined in [`klio-job.yaml`](./klio-job.yaml)).

In a new terminal window:

```sh
# in the root of the project directory
$ klio message publish cats-rule-the-internet
```

This will create a single Klio message with the data "cats-rule-the-internet" to be processed in  [`transforms.py`](./transforms.py).

In the terminal running the job, you should see the following log messages:

```log
INFO:klio:Received 'cats-rule-the-internet' from Pub/Sub topic 'projects/sigint/topics/foss-templates-test2-input'
```

## Run Klio on Google Cloud Dataflow

To run on Dataflow:

```sh
# in the root of the project directory
$ klio job run
```

The command will first upload the Docker image to Cloud Registry, then upload the job to Dataflow. Unlike running with Direct Runner, the command will exit out with a link to the Dataflow console where to view the job.

Give the job a few minutes to spin up the required resources. Then you're able to publish Klio messages to it:

```sh
# in the root of the project directory
$ klio message publish cats-rule-the-internet
```

Logs for the job can be seen in the Dataflow console page, and in Stackdriver Logging directly.

<!-- TODO: update `klio_docs` link with the proper docs page (i.e. read the docs, or spotify.github.io/klio) once FOSS'ed (@lynn) -->

[klio_docs.todo]: https://github.com/spotify/klio
[docker_mac]: https://docs.docker.com/docker-for-mac/install/
[docker_win]: https://docs.docker.com/docker-for-windows/install/
[docker_linux]: https://docs.docker.com/install/
[gcloud]: https://cloud.google.com/sdk/install
[pubsub]: https://cloud.google.com/pubsub/docs
[dataflow]: https://cloud.google.com/dataflow/docs
[pipx]: https://pypi.org/project/pipx/
[direct_runner]: https://beam.apache.org/documentation/runners/direct/
[gcs]: https://cloud.google.com/storage/docs
[gcr]: https://cloud.google.com/container-registry/docs
{% if not klio.use_fnapi -%}
[setup-py.todo]: http://point-to-public-docs
{% endif -%}
