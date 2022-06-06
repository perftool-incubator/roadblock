#!/bin/bash

counter=1
for arg in "$@"; do
    echo "argument ${counter}: ${arg}"
    (( counter += 1 ))
done


TIME=${1}
RC=${2}

RUNTIME=${TIME:-5}
RETURN_CODE=${RC:-0}

echo "Expected runtime: ${RUNTIME} seconds"

echo "STDOUT at $(date -u)"
echo "STDERR at $(date -u)" 1>&2

for i in $(seq 1 ${RUNTIME}); do
    echo "${i}: $(date -u)"
    sleep 1
done

echo "Exiting with return code = ${RETURN_CODE}"
exit ${RETURN_CODE}
