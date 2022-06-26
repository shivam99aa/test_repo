#!/bin/bash

set -e

export AWS_SDK_LOAD_CONFIG="true"
export AWS_REGION="us-east-1"

# Run terratest
for dir in $(find ${BASE_PATH} -maxdepth 1 -mindepth 1 -type d -printf '%f\n'); do
  if [[ -f "${BASE_PATH}/${dir}/test/test.sh" ]]; then
    ./${BASE_PATH}/${dir}/test/test.sh
  fi
done