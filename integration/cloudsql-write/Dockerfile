## -*- docker-image-name: "gcr.io/fluffy-zelda-glitch-toki-kobe/cloudsql-write-worker" -*-
FROM apache/beam_python3.6_sdk:2.26.0

WORKDIR /usr/src/app

ENV GOOGLE_CLOUD_PROJECT=fluffy-zelda-glitch-toki-kobe \
    PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

COPY __init__.py \
     run.py \
     transforms.py \
     job-requirements.txt \
     setup.py \
     MANIFEST.in \
     klio-job.yaml \
     # Include any other non-Python files your job needs
     /usr/src/app/

ARG KLIO_CONFIG=klio-job.yaml
COPY $KLIO_CONFIG klio-job-run-effective.yaml

RUN pip install .