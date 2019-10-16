#!/bin/bash

REDIS_PASSWORD=flubber
NUM_FOLLOWERS=50
ROADBLOCK_TIMEOUT=120
DOCKER_FILE=client.test.dockerfile
CONTAINER_NAME=roadblock-client-test

# goto the root of the repo
REPO_DIR=$(dirname $0)/../
if pushd ${REPO_DIR} > /dev/null; then

    echo -e "\nBuilding the container infrastructure"
    
    # build the roadblock container
    if pushd utilities/containers > /dev/null; then
	if ! buildah bud -t ${CONTAINER_NAME} -f ${DOCKER_FILE} .; then
	    echo "ERROR: Could not build roadblock client container"
	    exit 2
	fi
    
	popd > /dev/null
    else
	echo "ERROR: Could not pushd to utilities/containers"
	exit 1
    fi

    # get the redis database container from the registry
    if ! podman pull docker.io/centos/redis-5-centos7; then
	echo "ERROR: Could not pull the redis database container"
	exit 3
    fi

    echo -e "\nStarting the roadblock test"
    
    # start the redis database container
    echo -e "\nStarting the redis database container"
    if ! podman run -d --name redis_database -e REDIS_PASSWORD=${REDIS_PASSWORD} docker.io/centos/redis-5-centos7; then
	echo "ERROR: Could not start the redis database container"
	exit 4
    fi

    REDIS_IP_ADDRESS=$(podman inspect --format "{{.NetworkSettings.IPAddress}}" redis_database)
    ROADBLOCK_UUID=$(uuidgen)
    FOLLOWERS=""
    FOLLOWER_PREFIX="roadblock_follower"
    
    for i in $(seq 1 ${NUM_FOLLOWERS}); do
	FOLLOWERS+="--followers=${FOLLOWER_PREFIX}_${i} "
    done

    # start the roadblock leader container
    echo -e "\nStarting the roadblock leader container"
    if ! podman run -dit --name roadblock_leader localhost/${CONTAINER_NAME} -c \
	 "/opt/roadblock/roadblock.py --uuid=${ROADBLOCK_UUID} --role=leader --redis-server=${REDIS_IP_ADDRESS} --redis-password=${REDIS_PASSWORD} ${FOLLOWERS} \
	 --timeout=${ROADBLOCK_TIMEOUT}"; then
	echo "ERROR: Could not start the roadblock leader container"
	exit 5
    fi

    # start the roadblock follower container(s)
    for i in $(seq 1 ${NUM_FOLLOWERS}); do
	SLEEP_TIME=$((RANDOM%20))
	echo -e "\nStarting the roadblock follower ${i} container with a sleep ${SLEEP_TIME}"
	if ! podman run -dit --name=${FOLLOWER_PREFIX}_${i} localhost/${CONTAINER_NAME} -c \
	     "sleep ${SLEEP_TIME}; /opt/roadblock/roadblock.py --uuid=${ROADBLOCK_UUID} --role=follower --follower-id=${FOLLOWER_PREFIX}_${i} --redis-server=${REDIS_IP_ADDRESS} \
  	     --redis-password=${REDIS_PASSWORD} --timeout=${ROADBLOCK_TIMEOUT}"; then
	    echo "ERROR: Could not start roadblock follower ${i}"
	    echo "       This will cause a timeout to occur"
	fi
    done

    # wait for the roadblock leader container to exit
    echo -e -n "\nWaiting for the roadblock to complete"
    while true; do
	if podman ps --all --format "{{.Status}}" -f name=roadblock_leader | grep -q "^Exited"; then
	    break
	fi
	echo -n "."
	sleep 1
    done
    echo

    # get the roadblock leader container log
    echo -e "\nOutput from the roadblock leader:"
    podman logs -t roadblock_leader

    # get the roadblock follower container(s) log
    echo -e "\nOutput from the roadblock follower(s):"
    for i in $(seq 1 ${NUM_FOLLOWERS}); do
	echo -e "\nFollower ${i}:"
	podman logs -t ${FOLLOWER_PREFIX}_${i}
    done

    # remove the roadblock leader container
    echo -e "\nRemoving the roadblock leader container"
    if ! podman rm roadblock_leader; then
	echo "ERROR: Failed to remove the roadblock leader container"
	echo "       This probably means manual cleanup is required"
    fi

    # remove the roadblock follower container(s)
    for i in $(seq 1 ${NUM_FOLLOWERS}); do
	echo -e "\nRemoving the roadblock follower container ${i}"
	if ! podman rm ${FOLLOWER_PREFIX}_${i}; then
	    echo "ERROR: Failed to remove the roadblock follower ${i} container"
	    echo "       This probably means manual cleanup is required"
	fi
    done

    # stop the redis database container and remove it
    echo -e "\nStopping redis database container"
    if ! podman stop redis_database; then
	echo "ERROR: Failed to stop the redis database container"
	echo "       This probably means manual cleanup is required"
    fi
    echo -e "\nRemoving redis database container"
    if ! podman rm redis_database; then
	echo "ERROR: Failed to remove the redis database container"
	echo "       This probably means manual cleanup is required"
    fi
fi
