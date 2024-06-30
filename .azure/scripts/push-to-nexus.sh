#!/usr/bin/env bash

set -e

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

function cleanup() {
  rm -rf signing.gpg
  gpg --delete-keys
  gpg --delete-secret-keys
}

# Run the cleanup on failure / exit
trap cleanup EXIT

export GPG_TTY=$(tty)
echo $GPG_SIGNING_KEY | base64 -d > signing.gpg
gpg --batch --import signing.gpg

GPG_EXECUTABLE=gpg mvn $MVN_ARGS -DskipTests -s ./.azure/scripts/settings.xml -P ossrh verify deploy

cleanup