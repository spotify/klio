job_name: {{ klio.job_name }}
version: 2
pipeline_options:
  streaming: True
  update: False
  worker_harness_container_image: {{klio.pipeline_options.worker_harness_container_image}}
  {%- if klio.pipeline_options.experiments %}
  experiments:
  {%- for experiment in klio.pipeline_options.experiments %}
    - {{ experiment }}
  {%- endfor %}
  {%- endif %}
  project: {{klio.pipeline_options.project}}
  runner: DirectRunner
  {%- if not klio.use_fnapi %}
  setup_file: setup.py  # relative to job dir
  {%- endif %}
job_config:
  allow_non_klio_messages: False
  events:
    inputs:
    {%- for item in klio.job_options.inputs %}
      - type: pubsub
        topic: {{item.topic}}
        subscription: {{item.subscription}}
    {%- endfor %}
    outputs:
    {%- for item in klio.job_options.outputs %}
      - type: pubsub
        topic: {{item.topic}}
    {%- endfor %}
  data:
    inputs:
    {%- for item in klio.job_options.inputs %}
      - type: file
        location: {{item.data_location}}
    {%- endfor %}
    outputs:
    {%- for item in klio.job_options.outputs %}
      - type: file
        location: {{item.data_location}}
    {%- endfor %}
