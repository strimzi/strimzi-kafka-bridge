#!/usr/bin/env bash
set -e

echo "Run Findbugs ..."
make findbugs

echo "Packaging ..."
make java_package

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

    export DOCKER_ORG=strimzi
    export DOCKER_TAG=$TAG

    echo "Building Docker images ..."
    make docker_build

    echo "Pushing to Docker Hub ..."
    make docker_push
fi