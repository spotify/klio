# <--- Do not delete
apache-beam[gcp]==2.24.0  # must match the Dockerfile base image
klio-exec
# --->
# Put any third-party Python dependencies you need for your job here
# you do not need to list the apache-beam package - it is already installed
# with klio-exec (in the Dockerfile)
# be sure to hard-pin the versions, i.e.:
# foo-package==1.2.3
