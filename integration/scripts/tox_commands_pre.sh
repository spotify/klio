#!/usr/bin/env bash +x
#
# Copyright 2020 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# KLIO_TEST_DIR needs to be set (which is when running via tox)

# -r recursive, -p preserve perms
rsync -rp ../core/src ${KLIO_TEST_DIR}/core
rsync -rp ../core/setup.py ${KLIO_TEST_DIR}/core/setup.py
# Need to copy over package's README.rst as it's needed to build it
rsync -rp ../core/README.rst ${KLIO_TEST_DIR}/core/README.rst

rsync -rp ../lib/src ${KLIO_TEST_DIR}/lib
rsync -rp ../lib/setup.py ${KLIO_TEST_DIR}/lib/setup.py
# Need to copy over package's README.rst as it's needed to build it
rsync -rp ../lib/README.rst ${KLIO_TEST_DIR}/lib/README.rst

rsync -rp ../exec/src ${KLIO_TEST_DIR}/exec
rsync -rp ../exec/setup.py ${KLIO_TEST_DIR}/exec/setup.py
# Need to copy over package's README.rst as it's needed to build it
rsync -rp ../exec/README.rst ${KLIO_TEST_DIR}/exec/README.rst

rsync -rp ../audio/src ${KLIO_TEST_DIR}/audio
rsync -rp ../audio/setup.py ${KLIO_TEST_DIR}/audio/setup.py
# Need to copy over package's README.rst as it's needed to build it
rsync -rp ../audio/README.rst ${KLIO_TEST_DIR}/audio/README.rst

if [ -f "${KLIO_TEST_DIR}/it/test-requirements.txt" ];
  then pip install -r ${KLIO_TEST_DIR}/it/test-requirements.txt;
fi
if [ -f "${KLIO_TEST_DIR}/it/before.py" ];
  then python ${KLIO_TEST_DIR}/it/before.py;
fi
