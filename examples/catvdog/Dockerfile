FROM apache/beam_python3.6_sdk:2.24.0

WORKDIR /usr/src/app

ENV PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

COPY job-requirements.txt job-requirements.txt
RUN pip install -r job-requirements.txt

COPY __init__.py \
     run.py \
     transforms.py \
     /usr/src/app/

ARG KLIO_CONFIG=klio-job.yaml
COPY $KLIO_CONFIG /usr/src/config/.effective-klio-job.yaml
