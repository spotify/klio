## -*- docker-image-name: "{{ klio.pipeline_options.worker_harness_container_image }}" -*-
FROM apache/beam_python{{ klio.python_version }}_sdk:2.35.0

WORKDIR /usr/src/app
{%- if klio.use_fnapi %}
RUN mkdir -p /usr/src/config
{%- endif %}

ENV GOOGLE_CLOUD_PROJECT={{klio.pipeline_options.project}} \
    PYTHONPATH=/usr/src/app

{% if klio.use_fnapi -%}
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
RUN pip install -r job-requirements.txt
{%- else -%}
RUN pip install --upgrade pip setuptools
{%- endif %}

COPY __init__.py \
     run.py \
     transforms.py \
     {%- if not klio.use_fnapi %}
     job-requirements.txt \
     setup.py \
     MANIFEST.in \
     # Include any other non-Python files your job needs
     {%- endif %}
     /usr/src/app/

{% if not klio.use_fnapi -%}
RUN pip install .
{% endif -%}
