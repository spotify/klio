FROM dataflow.gcr.io/v1beta3/python36-fnapi:2.27.0

WORKDIR /usr/src/app
RUN mkdir -p /usr/src/config

ENV PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

COPY core core
COPY lib lib
COPY exec exec
RUN pip install ./core
RUN pip install ./lib
RUN pip install ./exec

COPY job-requirements.txt job-requirements.txt
RUN pip install -r job-requirements.txt

COPY __init__.py \
     run.py \
     transforms.py \
     MANIFEST.in \
     setup.py \
     /usr/src/app/

ARG KLIO_CONFIG=klio-job.yaml
COPY $KLIO_CONFIG klio-job-run-effective.yaml
RUN pip install .
