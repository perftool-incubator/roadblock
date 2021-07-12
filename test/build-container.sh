#!/bin/bash
# -*- mode: sh; indent-tabs-mode: nil; sh-basic-offset: 4 -*-
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=bash

BUILD=1
UPDATE=0

STAGE_1_DOCKER_FILE=client.test.stage1.dockerfile
STAGE_1_UPDATE_DOCKER_FILE=client.test.stage1.update.dockerfile
STAGE_2_DOCKER_FILE=client.test.stage2.dockerfile
STAGE_1_IMAGE_NAME=fedora-redis-python-client
STAGE_2_IMAGE_NAME=roadblock-client-test

# goto the root of the repo
REPO_DIR=$(dirname $0)/../
if pushd ${REPO_DIR} > /dev/null; then
    if [ "${BUILD}" == 1 ]; then
        echo -e "\nBuilding the container infrastructure"

        if [ -z "$(podman images --quiet localhost/${STAGE_1_IMAGE_NAME})" ] ; then
            echo -e "\nBuilding the stage 1 container image"
            if ! buildah bud -t ${STAGE_1_IMAGE_NAME} -f utilities/containers/${STAGE_1_DOCKER_FILE} ${REPO_DIR}; then
                echo "ERROR: Could not build stage 1 container image"
                exit 1
            fi
        else
            if [ "${UPDATE}" == 1 ]; then
                echo -e "\nUpdating the stage 1 container image"
                if ! buildah bud -t ${STAGE_1_IMAGE_NAME} -f utilities/containers/${STAGE_1_UPDATE_DOCKER_FILE} ${REPO_DIR}; then
                    echo "ERROR: Could not update stage 1 container image"
                    exit 2
                fi
            fi
        fi

        if [ -n "$(podman images --quiet localhost/${STAGE_2_IMAGE_NAME})" ]; then
            echo -e "\nRemoving stale stage 2 container image"
            if ! buildah rmi localhost/${STAGE_2_IMAGE_NAME}; then
                echo "ERROR: Could not remove stale stage 2 container image"
                exit 9
            fi
        fi

        echo -e "\nBuilding the stage 2 container image"
        if ! buildah bud -t ${STAGE_2_IMAGE_NAME} -f utilities/containers/${STAGE_2_DOCKER_FILE} ${REPO_DIR}; then
            echo "ERROR: Could not build stage 2 container image"
            exit 8
        fi
    else
        echo -e "\nSkipping container infrastructure build"
    fi

    echo -e "\nAvailable container images"
    buildah images
fi
