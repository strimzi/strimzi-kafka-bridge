#!/usr/bin/env bash

set -e

export DOCKER_ORG=${DOCKER_ORG:-strimzi}
export DOCKER_REGISTRY=${DOCKER_REGISTRY:-quay.io}
export DOCKER_TAG=$COMMIT

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

# The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
if [ "${JAVA_MAJOR_VERSION}" -eq 11 ] ; then
  # some parts of the workflow should be done only one on the main build which is currently Java 11
  export MAIN_BUILD="TRUE"
  echo "Running main build on Java 11"
fi

echo "Run Findbugs ..."
make findbugs

echo "Run docu check ..."
make docu_check

echo "Verifying ..."
make java_verify

echo "Building documentation ..."
make docu_html
make docu_htmlnoheader
echo "Building Docker images ..."
make docker_build

if [ "$BUILD_REASON" == "PullRequest" ] ; then
  echo "Building Pull Request - nothing to push"
elif [[ "$BRANCH" != "refs/tags/"* ]] && [ "$BRANCH" != "refs/heads/main" ]; then
  echo "Not in main branch or in release tag - nothing to push"
else
  if [ "${MAIN_BUILD}" = "TRUE" ] ; then
    if [ "$BRANCH" == "refs/heads/main" ]; then
        export DOCKER_TAG="latest"
    else
        export DOCKER_TAG="${BRANCH#refs/tags/}"
    fi

    echo "In main branch or in release tag - pushing to nexus"
    ./.azure/scripts/push-to-nexus.sh

    echo "In main branch or in release tag - pushing images"
    docker login -u "$DOCKER_USER" -p "$DOCKER_PASS" "$DOCKER_REGISTRY"
    make docker_push

    if [ "$BRANCH" == "refs/heads/main" ]; then
      echo "In main branch - pushing in-development documentatin to the website"
      make docu_pushtowebsite
    fi
  fi
fi
