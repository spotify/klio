Cat and Dog Image Classifier
============================

Requirements
------------

* Follow the documented `installation instructions <https://docs.klio.io/en/latest/quickstart/installation.html>`_.
* Python 3.6+

Run
---

.. code-block:: sh

    # Clone example
    $ git clone https://github.com/spotify/klio
    $ cd klio/examples/catvdog

    # Create a klio-job.yaml based off of the example config
    $ export GCP_PROJECT=<your gcp project>
    $ sed -e "s/\$GCP_PROJECT/${GCP_PROJECT}/" \
          klio-job.yaml.example > klio-job.yaml

    # Setup GCP-related resources
    $ klio job verify --create-resources

    # Run tests
    $ klio job test

    # Run locally
    $ klio job run --direct-runner

Then to publish messages:

.. code-block:: sh

    # Do not include the file extension, just the name/ID
    $ klio message publish --top-down 400


Resources
---------

Take a look at ``run.py`` and ``transforms.py`` for further understanding of the logic applied and what's required for Klio.

* Model based off of `github.com/gsurma/image_classifier <https://github.com/gsurma/image_classifier>`_ (`corresponding writeup <https://towardsdatascience.com/image-classifier-cats-vs-dogs-with-convolutional-neural-networks-cnns-and-google-colabs-4e9af21ae7a8>`_)
* Dataset from `Kaggle <https://www.kaggle.com/c/dogs-vs-cats>`_
