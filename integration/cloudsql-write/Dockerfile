## -*- docker-image-name: "gcr.io/fluffy-zelda-glitch-toki-kobe/cloudsql-write-worker" -*-
FROM apache/beam_python3.6_sdk:2.26.0

WORKDIR /usr/src/app

ENV GOOGLE_CLOUD_PROJECT=fluffy-zelda-glitch-toki-kobe \
    PYTHONPATH=/usr/src/app

RUN pip install --upgrade pip setuptools

COPY core core
COPY lib lib
COPY exec exec
RUN pip install ./core
RUN pip install ./lib
RUN pip install ./exec

COPY __init__.py \
     run.py \
     transforms.py \
     job-requirements.txt \
     setup.py \
     MANIFEST.in \
     klio-job.yaml \
     cloud_sql_proxy.linux.amd64 \
#     klio-core-21.2.0.tar.gz \
#     klio-21.2.0.tar.gz \
#     klio-exec-21.2.0.tar.gz \
     # Include any other non-Python files your job needs
     /usr/src/app/

ARG KLIO_CONFIG=klio-job.yaml
COPY $KLIO_CONFIG klio-job-run-effective.yaml
# TODO understand why the following line was needed
COPY $KLIO_CONFIG /usr/src/config/.effective-klio-job.yaml

RUN pip install .