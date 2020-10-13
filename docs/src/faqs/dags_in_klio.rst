How are DAGs described within Klio?
===================================

A DAG (directed acyclic graph) of streaming Klio jobs is defined in a job's ``klio-job.yaml`` configuration file.
The :doc:`output </userguide/io/index>` of one job can be used as the input of another job.

Learn more about how DAGs are used in Klio :doc:`here </userguide/anatomy/graph>`.

Learn more about setting up a DAG of Klio jobs through configuration :doc:`here </userguide/config/job_config>`.
