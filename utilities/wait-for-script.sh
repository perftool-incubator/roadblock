#!/bin/bash

TIME=${1}
RUNTIME=${TIME:-5}

echo "Expected runtime: ${RUNTIME} seconds"

echo "STDOUT at $(date -u)"
echo "STDERR at $(date -u)" 1>&2

for i in $(seq 1 ${RUNTIME}); do
    echo "${i}: $(date -u)"
    sleep 1
done
