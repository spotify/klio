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
  inputs:
  {%- for item in klio.job_options.inputs %}
    - topic: {{item.topic}}
      subscription: {{item.subscription}}
      data_location: {{item.data_location}}
  {%- endfor %}
  outputs:
  {%- for item in klio.job_options.outputs %}
    - topic: {{item.topic}}
      data_location: {{item.data_location}}
  {%- endfor %}
  {%- if klio.job_options.dependencies %}
  dependencies:
  {%- for dep in klio.job_options.dependencies %}
    - job_name: {{ dep.job_name }}
      gcp_project: {{ dep.gcp_project }}
      {%- if dep.input_topics %}
      input_topics:
      {%- for topic in dep.input_topics %}
        - {{ topic }}
      {%- endfor %}
      {%- endif %}
      {%- if dep.region %}
      region: {{ dep.region }}
      {%- endif %}
  {%- endfor %}
  {%- endif %}
