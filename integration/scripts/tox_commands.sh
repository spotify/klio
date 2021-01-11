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

# KLIO_TEST_DIR & TOX_ENV_DIR need to be set (which is when running via tox)

TOX_ENV_BIN_DIR=${TOX_ENV_DIR}/bin

# Exit immediately if a step returns a non-zero code; to call:
# `raise_on_fail <RET_CODE>`
# Use `$?` to get the return code of the last command executed
function raise_on_fail() {
  RET_CODE="$1"
  if [ $RET_CODE -ne 0 ]; then
      echo FAIL
      exit 1
  fi
}

${TOX_ENV_BIN_DIR}/klio image build -j ${KLIO_TEST_DIR} --image-tag ${KLIO_TEST_DIR}
raise_on_fail $?

${TOX_ENV_BIN_DIR}/klio job test -j ${KLIO_TEST_DIR} --image-tag ${KLIO_TEST_DIR} -- tests
raise_on_fail $?

${TOX_ENV_BIN_DIR}/klio job run -j ${KLIO_TEST_DIR} --image-tag ${KLIO_TEST_DIR} --direct-runner 2>&1 | tee ${KLIO_TEST_DIR}/job_output.log
raise_on_fail $?

if [ -f "${KLIO_TEST_DIR}/integration_test.py" ]; then
  python ${KLIO_TEST_DIR}/integration_test.py ;
  raise_on_fail $?
fi

