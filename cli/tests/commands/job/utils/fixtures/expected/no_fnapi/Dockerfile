## -*- docker-image-name: "gcr.io/foo/bar" -*-
FROM apache/beam_python3.6_sdk:2.24.0

WORKDIR /usr/src/app

ENV GOOGLE_CLOUD_PROJECT=test-gcp-project \
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

RUN pip install .  --use-feature=2020-resolver
