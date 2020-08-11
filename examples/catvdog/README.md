# Cat and Dog Image Classifier

## Requirements

* Python 3.5+ (make sure the minor version matches with the base Docker image stated on the `FROM` line in the [`Dockerfile`](./Dockerfile#L2))
* `klio-cli` (`pipx install klio-cli`)
* GCP project with the following APIs enabled
    - [GCR](https://console.cloud.google.com/apis/api/containerregistry.googleapis.com/overview)
    - [Dataflow](https://console.developers.google.com/apis/api/dataflow.googleapis.com/overview)
* GCS bucket (as configured for `job_config.inputs[0].data_location` in your `klio-job.yaml` below) with unclassified input data; see [Resources](#resources) for dataset

## Run

```sh
# Clone example
$ git clone https://github.com/spotify/klio
$ cd klio/examples/catvdog

# Create a klio-job.yaml based off of the example config
$ export LDAP_USER=<your team name> GCP_PROJECT=<your gcp project>
$ sed -e "s/\$LDAP_USER/${LDAP_USER}/" \
      -e "s/\$GCP_PROJECT/${GCP_PROJECT}/" \
      klio-job.yaml.example > klio-job.yaml

# Setup GCP-related resources
$ klio job verify --create-resources

# Run locally
$ klio job run --direct-runner

# Or on dataflow
$ klio job run
```

Then to publish messages:

```
# note to not include the file extension, just the name/ID
$ klio message publish --top-down 400
```

## Resources

Take a look at `run.py` and `transforms.py` (TODO: include link to Golden Path once written) for further understanding of the logic applied and what's required for klio.

* Model based off of [github.com/gsurma/image_classifier](https://github.com/gsurma/image_classifier) ([corresponding writeup](https://towardsdatascience.com/image-classifier-cats-vs-dogs-with-convolutional-neural-networks-cnns-and-google-colabs-4e9af21ae7a8))
* Dataset from [Kaggle](https://www.kaggle.com/c/dogs-vs-cats)
