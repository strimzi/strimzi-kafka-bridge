#!/usr/bin/env bash
set -e

# packaging
echo "Packaging ..."
mvn package

echo "PULL_REQUEST=$PULL_REQUEST"
echo "TAG=$TAG"
echo "BRANCH=$BRANCH"

# Docker build and push only if on master, no pull request and "latest" tag
if [ "$PULL_REQUEST" != "false" ] ; then
    echo "Building Pull Request - nothing to push"
elif [ "$TAG" = "latest" ] && [ "$BRANCH" != "master" ] ; then
    echo "Not in master branch and not in release tag - nothing to push"
else
    echo "Login into Docker Hub ..."
    docker login -u $DOCKER_USER -p $DOCKER_PASS
    echo "Building Docker image ..."
    docker build -t strimzi/kafka-bridge:latest .
    echo "Pushing to Docker Hub ..."
    docker push strimzi/kafka-bridge:latest
fi