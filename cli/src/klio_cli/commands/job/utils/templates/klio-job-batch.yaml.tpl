job_name: {{ klio.job_name }}
version: 2
pipeline_options:
  streaming: False
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
  events:
    inputs:
      - type: file
        location: input_items.txt
    outputs:
      - type: file
        location: output_items.txt
