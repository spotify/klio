job_name: {{ klio.job_name }}
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
  region: {{klio.pipeline_options.region}}
  staging_location: {{klio.pipeline_options.staging_location}}
  temp_location: {{klio.pipeline_options.temp_location}}
  num_workers: {{klio.pipeline_options.num_workers}}
  max_num_workers: {{klio.pipeline_options.max_num_workers}}
  autoscaling_algorithm: {{klio.pipeline_options.autoscaling_algorithm}}
  disk_size_gb: {{klio.pipeline_options.disk_size_gb}}
  worker_machine_type: {{klio.pipeline_options.worker_machine_type}}
  runner: DataflowRunner
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
      - type: gcs
        location: {{item.data_location}}
        # Remove/set to false when job is ready to process input existence checks
        skip_klio_existence_check: True
    {%- endfor %}
    outputs:
    {%- for item in klio.job_options.outputs %}
      - type: gcs
        location: {{item.data_location}}
        # Remove/set to false when job is ready to process output existence checks
        skip_klio_existence_check: True
    {%- endfor %}
