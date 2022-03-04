FROM dataflow.gcr.io/v1beta3/python38-fnapi:2.35.0

WORKDIR /usr/src/app

ENV PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

COPY job-requirements.txt job-requirements.txt
RUN pip install -r job-requirements.txt

COPY __init__.py \
     run.py \
     transforms.py \
     /usr/src/app/

