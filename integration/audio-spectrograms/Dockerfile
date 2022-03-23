FROM dataflow.gcr.io/v1beta3/python38-fnapi:2.35.0

WORKDIR /usr/src/app
RUN mkdir -p /usr/src/config

ENV PYTHONPATH=/usr/src/app

RUN apt-get update && apt-get install -y libsndfile1

RUN pip install --upgrade pip setuptools

COPY core core
COPY lib lib
COPY exec exec
COPY audio audio
RUN pip install ./core
RUN pip install ./lib
RUN pip install ./exec
RUN pip install ./audio

COPY job-requirements.txt job-requirements.txt
RUN pip install -r job-requirements.txt

COPY __init__.py \
     run.py \
     transforms.py \
     MANIFEST.in \
     setup.py \
     batch_track_ids.txt \
     /usr/src/app/

RUN pip install .
