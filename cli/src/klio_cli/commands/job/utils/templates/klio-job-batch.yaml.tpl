job_name: {{ klio.job_name }}
version: 2
pipeline_options:
  streaming: False
  project: {{ klio.pipeline_options.project }}
  worker_harness_container_image: {{klio.pipeline_options.worker_harness_container_image}}
  {%- if klio.pipeline_options.experiments %}
  experiments:
  {%- for experiment in klio.pipeline_options.experiments %}
    - {{ experiment }}
  {%- endfor %}
  {%- endif %}
  runner: DirectRunner
  {%- if not klio.use_fnapi %}
  setup_file: setup.py  # relative to job dir
  {%- endif %}
job_config:
  allow_non_klio_messages: False
  events:
    inputs:
    {%- for item in klio.job_options.inputs %}
      - type: file
        location: {{item.event_location}}
    {%- endfor %}
    outputs:
    {%- for item in klio.job_options.outputs %}
      - type: file
        location: {{item.event_location}}
    {%- endfor %}
  data:
    inputs:
    {%- for item in klio.job_options.inputs %}
      - type: file
        location: {{item.data_location}}
        # Remove/set to false when job is ready to process input existence checks
        skip_klio_existence_check: True
    {%- endfor %}
    outputs:
    {%- for item in klio.job_options.outputs %}
      - type: file
        location: {{item.data_location}}
        # Remove/set to false when job is ready to process output existence checks
        skip_klio_existence_check: True
    {%- endfor %}