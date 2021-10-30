#!/usr/bin/env bash
set -e

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"
echo "Pushing container images for ${DOCKER_TAG}"

# Tag and Push the container
echo "Login into Docker Hub ..."
docker login -u $DOCKER_USER -p $DOCKER_PASS $DOCKER_REGISTRY

make docker_load docker_tag docker_push
