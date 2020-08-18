# klio integration tests

All code under the `klio/integration` directory (including this job) exists for the purpose of integration testing changes to the `klio` ecosystem and will not work as a regular deployed job.

NOTE FOR MAINTAINERS: Be aware that some of these tests make use of GCP resources. Permissions for this are managed by a single service account (see build configuration files) that have been granted a minimal amount of access. Take care when extending these permissions.