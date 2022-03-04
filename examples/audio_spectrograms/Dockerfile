FROM dataflow.gcr.io/v1beta3/python38-fnapi:2.35.0

WORKDIR /usr/src/app
RUN mkdir -p /usr/src/config

ENV PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

COPY __init__.py \
     run.py \
     setup.py \
     transforms.py \
     audio_ids.txt \
     klio-job.yaml \
     MANIFEST.in \
     job-requirements.txt \
     /usr/src/app/

RUN pip install .
