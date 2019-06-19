#!/usr/bin/env bash
set -e

# The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [ ${JAVA_MAJOR_VERSION} -gt 1 ] ; then
  export JAVA_VERSION=${JAVA_MAJOR_VERSION}
fi

if [ ${JAVA_MAJOR_VERSION} -eq 1 ] ; then
  # some parts of the workflow should be done only on the main build which is currently Java 8
  export MAIN_BUILD="TRUE"
fi

if [ "${MAIN_BUILD}" = "TRUE" ] ; then
    echo "Run Findbugs ..."
    make findbugs
fi

echo "Run docu check ..."
make docu_check

echo "Packaging ..."
make java_package

echo "PULL_REQUEST=$PULL_REQUEST"
echo "TAG=$TAG"
echo "BRANCH=$BRANCH"

# Docker build and push only if on master, no pull request and "latest" tag
if [ "$PULL_REQUEST" != "false" ] ; then
    make docu_html
    make docu_htmlnoheader

    echo "Building Pull Request - nothing to push"
elif [ "$TAG" = "latest" ] && [ "$BRANCH" != "master" ] ; then
    make docu_html
    make docu_htmlnoheader

    echo "Not in master branch and not in release tag - nothing to push"
else
    if [ "${MAIN_BUILD}" = "TRUE" ] ; then
        echo "Login into Docker Hub ..."
        docker login -u $DOCKER_USER -p $DOCKER_PASS

        export DOCKER_ORG=strimzi
        export DOCKER_TAG=$TAG

        echo "Building Docker images ..."
        make docker_build

        echo "Pushing to Docker Hub ..."
        make docker_push

        if [ "$BRANCH" = "master" ]; then
            make docu_pushtowebsite
        fi
    fi
fi
