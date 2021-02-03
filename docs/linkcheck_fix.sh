#! /usr/bin/env bash

# This script is to be executed via `make` or via CI workflow.
#
# This script is a temporary workaround for a bug that was introduced in the `sphinx-reredirects` extension.
# This PR addresses it: https://gitlab.com/documatt/sphinx-reredirects/-/merge_requests/1
# But as we wait for the PR to merge and a new release made (if ever), this script tries to capture
# the specific error message that the bug produces, while raising other errors.

function trap_error_message() {

  # -T = show full traceback for errors; -n = nitpick; -W turn warnings into errors
  RESULT=$(sphinx-build -T -n -W -b linkcheck -d build/doctrees src build/linkcheck 2>&1)
  RC=$?
  if [[ $RC -eq 0 ]]; then
    exit 0
  fi

  LINKCHECK_SUCCESSFUL="build succeeded."

  # if linkcheck did not succeed, raise
  if ! echo "$RESULT" | grep --quiet "$LINKCHECK_SUCCESSFUL"; then
    echo "build did not succeed"
    # this will include the traceback from the sphinx_reredirects bug as well as broken links
    echo "$RESULT"
    exit 1
  else
    echo "$LINKCHECK_SUCCESSFUL"
  fi

  PARTIAL_TRACEBACK="sphinx_reredirects"
  PARTIAL_ERR_MSG="for event 'build-finished' threw an exception (exception: \[Errno 2\] No such file or directory:"

  if echo "$RESULT" | grep --quiet "$PARTIAL_TRACEBACK\|$PARTIAL_ERR_MSG"; then
    exit 0
  else
    echo "$RESULT"
    exit $RC
  fi

}

trap_error_message
