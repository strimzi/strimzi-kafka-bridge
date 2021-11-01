#!/usr/bin/env bash

set -e

echo "Build reason: ${BUILD_REASON}"
echo "Source branch: ${BRANCH}"

echo "$GITHUB_DEPLOY_KEY" | base64 -d > github_deploy_key
chmod 600 github_deploy_key
eval "$(ssh-agent -s)"
ssh-add github_deploy_key

git clone git@github.com:strimzi/strimzi.github.io.git /tmp/website
rm -rf /tmp/website/docs/bridge/in-development/images
rm -rf /tmp/website/docs/bridge/in-development/full/images
cp -v documentation/htmlnoheader/bridge.html /tmp/website/docs/bridge/in-development/bridge.html
cp -v documentation/html/bridge.html /tmp/website/docs/bridge/in-development/full/bridge.html
cp -vrL documentation/htmlnoheader/images /tmp/website/docs/bridge/in-development/images
cp -vrL documentation/htmlnoheader/images /tmp/website/docs/bridge/in-development/full/images

pushd /tmp/website

if [[ -z $(git status -s) ]]; then
    echo "No changes to the output on this push; exiting."
    exit 0
fi

git config user.name "Strimzi CI"
git config user.email "ci@strimzi.io"

git add -A
git commit -s -m "Update Kafka Bridge documentation (Commit ${COMMIT})" --allow-empty
git push origin main

popd
