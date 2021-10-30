#!/usr/bin/env bash

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

GPG_TTY=$(tty)
export GPG_TTY

echo "$GPG_SIGNING_KEY" | base64 -d > signing.gpg
gpg --batch --import signing.gpg

GPG_EXECUTABLE=gpg mvn $MVN_ARGS -DskipTests -s ./.azure/scripts/settings.xml -P ossrh verify deploy

rm -rf signing.gpg
gpg --delete-keys
gpg --delete-secret-keys