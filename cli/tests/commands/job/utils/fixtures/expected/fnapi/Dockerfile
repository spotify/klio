## -*- docker-image-name: "gcr.io/foo/bar" -*-
FROM apache/beam_python3.6_sdk:2.24.0

WORKDIR /usr/src/app
RUN mkdir -p /usr/src/config

ENV GOOGLE_CLOUD_PROJECT=test-gcp-project \
    PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

###############################################################
# DO NOT EDIT ABOVE THIS LINE. Or you may break klio.         #
# pip packages are automatically installed for you.           #
# klio-exec must be installed before all other packages.      #
# Add extra installation and config needed by your job BELOW. #
###############################################################



###############################################################
# DO NOT EDIT BELOW THIS LINE. Or you may break klio.         #
# pip packages are automatically installed for you.           #
# Add extra installation and config needed by your job ABOVE. #
###############################################################

COPY job-requirements.txt job-requirements.txt
RUN pip install -r job-requirements.txt --use-feature=2020-resolver

COPY __init__.py \
     run.py \
     transforms.py \
     /usr/src/app/

ARG KLIO_CONFIG=klio-job.yaml
COPY $KLIO_CONFIG /usr/src/config/.effective-klio-job.yaml
