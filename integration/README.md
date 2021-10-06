# klio integration tests

All code under the `klio/integration` directory (including this job) exists for the purpose of integration testing changes to the `klio` ecosystem and will not work as a regular deployed job.

NOTE FOR MAINTAINERS: Be aware that some of these tests make use of GCP resources. Permissions for this are managed by a single service account (see build configuration files) that have been granted a minimal amount of access. Take care when extending these permissions.

## Notes for Contributors

### Regenerating job-requirements.txt

1.  Make sure you are using the same version of python used in the jobs' `Dockerfile` (currently 3.6).

2.  `pip install pip-tools` to install `pip-compile`

3.  In the job's directory, run `pip-compile --upgrade job-requirements.in`.  The upgrade flag is important when switching python versions, it may not be needed otherwise.