#!/bin/bash

REDIS_PASSWORD=flubber
NUM_FOLLOWERS=50
ROADBLOCK_TIMEOUT=120
DOCKER_FILE=client.test.dockerfile
CONTAINER_NAME=roadblock-client-test
POD_NAME=roadblock-test
BUILD=1

# goto the root of the repo
REPO_DIR=$(dirname $0)/../
if pushd ${REPO_DIR} > /dev/null; then

    if [ "${BUILD}" == 1 ]; then
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
    else
	echo -e "\nSkipping container infrastructure build"
    fi

    echo -e "\nStarting the roadblock test"

    # create a pod to place all the containers into
    echo -e "\nCreating roadblock pod"
    if ! podman pod create --name=${POD_NAME} --infra=false; then
	echo "ERROR: Could not create the pod"
	exit 6
    fi
    
    # start the redis database container
    echo -e "\nStarting the redis database container"
    if ! podman run --detach=true --name=redis_database --pod=${POD_NAME} -e REDIS_PASSWORD=${REDIS_PASSWORD} docker.io/centos/redis-5-centos7; then
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
    if ! podman run --detach=true --interactive=true --tty=true --name=roadblock_leader --pod=${POD_NAME} localhost/${CONTAINER_NAME} -c \
	 "/opt/roadblock/roadblock.py --uuid=${ROADBLOCK_UUID} --role=leader --redis-server=${REDIS_IP_ADDRESS} --redis-password=${REDIS_PASSWORD} ${FOLLOWERS} \
	 --timeout=${ROADBLOCK_TIMEOUT}"; then
	echo "ERROR: Could not start the roadblock leader container"
	exit 5
    fi

    # start the roadblock follower container(s)
    for i in $(seq 1 ${NUM_FOLLOWERS}); do
	SLEEP_TIME=$((RANDOM%20))
	echo -e "\nStarting the roadblock follower ${i} container with a sleep ${SLEEP_TIME}"
	if ! podman run --detach --interactive=true --tty=true --name=${FOLLOWER_PREFIX}_${i} --pod=${POD_NAME} localhost/${CONTAINER_NAME} -c \
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
    fi

    # remove the roadblock follower container(s)
    for i in $(seq 1 ${NUM_FOLLOWERS}); do
	echo -e "\nRemoving the roadblock follower container ${i}"
	if ! podman rm ${FOLLOWER_PREFIX}_${i}; then
	    echo "ERROR: Failed to remove the roadblock follower ${i} container"
	fi
    done

    # stop the redis database container and remove it
    echo -e "\nStopping redis database container"
    if ! podman stop redis_database; then
	echo "ERROR: Failed to stop the redis database container"
    fi
    echo -e "\nRemoving redis database container"
    if ! podman rm redis_database; then
	echo "ERROR: Failed to remove the redis database container"
    fi

    # remove the pod and forceably cleanup any remaining containers
    echo -e "\nRemoving the roadblock pod"
    if ! podman pod rm --force ${POD_NAME}; then
	echo "ERROR: Failed to remove the roadblock pod"
	exit 7
    fi
fi
