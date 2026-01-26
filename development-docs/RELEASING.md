# Releasing Kafka Bridge

This document describes how to release a new version of the Strimzi Kafka Bridge.

## Regular releases

### Create release branch

Before releasing new major or minor version of Kafka Bridge, the release branch has to be created.
The release branch should be named as `release-<Major>.<Minor>.x`.
For example for release 1.2.0, the branch should be named `release-1.2.x`.
The release branch is normally created from the `main` branch.
This is normally done locally and the branch is just pushed into the GitHub repository.

When releasing a new patch version, the release branch should already exist.
You just need to cherry-pick bug fixes or add them through PRs.

### Prepare the release

For any new release - major, minor or patch - we need to prepare the release.
The release preparation includes updating the installation files for the new version or changing the version of the Maven project.
This is implemented in the `Makefiles`, you just need to run the command `RELEASE_VERSION=<NewRealeaseVersion> make release`.
For example, for release 1.2.0, you would run `RELEASE_VERSION=1.2.0 make release`.

Review and commit the changes done by the `make` command and push them into the repository.
The build workflow should automatically start for any new commit pushed into the release branch.

### Running the release workflow

Wait until the build workflow is (successfully) finished for the last commit in the release branch.
The regular workflow will build the release ZIP / TAR.GZ files as well as the documentation and store them as artifacts.
Note the GitHub Actions Run ID from the URL (e.g., `https://github.com/strimzi/strimzi-kafka-bridge/actions/runs/1234567890` - the Run ID is `1234567890`).

Then run the release workflow manually from the GitHub Actions UI:
1. Go to the **Actions** tab in the GitHub repository
2. Select the **Release** workflow from the left sidebar
3. Click **Run workflow** button
4. Fill in the required parameters:
   * **Release Version**: for example `1.2.0`
   * **Build suffixed images**: check this to create suffixed images (e.g., `1.2.0-0`)
   * **Release Suffix**: for example `0` (used only if suffixed images are enabled - identifies different builds done for example due to base image CVEs)
   * **GitHub Actions Run ID of the source build**: the Run ID from the build workflow (from the URL as noted above)

The release workflow will push the images to the registry and publish the Java artifacts to Maven Central.

### Smoke tests

After the release workflow is finished, it is always good idea to do some smoke tests of the images to double-check they were pushed correctly.

### Creating the release

After the release workflow is finished, the release has to be created:

* Tag the right commit from the release branch with the release name (e.g. `git tag 1.2.0`) and push it to GitHub
* The Java artifacts are automatically pushed to Maven Central by the release workflow.
Once this is done, go to the Sonatype portal and publish the artifacts.
* Update the documentation on the website using the HTML files from the build pipeline artifacts
* On GitHub, create the release and attach the ZIP / TAR.GZ artifacts from the build workflow artifacts (download from the Actions run)

### Announcements

Announce the release on following channels:
* Mailing lists
* Slack
* Twitter (if the release is significant enough)

### Update to next release

The `main` git branch has to be updated with the next SNAPSHOT version.
Update the version to the next SNAPSHOT version using the `next_version` `make` target. 
For example to update the next version to 0.32.0-SNAPSHOT run: `make NEXT_VERSION=0.32.0-SNAPSHOT next_version`.
Add a header for the new release to the CHANGELOG.md file

### Release candidates

Release candidates are built with the same release workflow as the final releases.
When starting the workflow, use the RC name as the release version.
For example `1.2.0-rc1` or `1.2.0-rc2`.
For release candidates, you should disable the suffixed build since it is not needed.

When doing the release candidates, the release branch should be already prepared for the final release.
E.g. when building `1.2.0-rc1`, the release branch should have already the `1.2.0` versions set.
The release candidate version (e.g. `1.2.0-rc1`) should be used for the GitHub tag and release.

## Rebuilding container images for base image CVEs

In case of a CVE in the base container image, we might need to rebuild the Kafka Bridge container image.
This can be done using the **CVE Container Rebuild** workflow.
This workflow will take previously built binaries from GHCR and use them to build a new container image.
It will push the container image to the container registry with the suffixed tag (e.g. `1.2.0-2`).
Afterwards, it will wait for manual approval in the `cve-validation` environment.
This gives additional time to manually test the new container image.
After the manual approval, the image will be also pushed under the tag without suffix (e.g. `1.2.0`).

The suffix can be specified when starting the rebuild workflow.
You should always check what was the previous suffix and increment it.
That way, the older images will be still available in the container registry under their own suffixes.
Only the latest rebuild will be available under the un-suffixed tag.

To run the CVE rebuild workflow:
1. Go to the **Actions** tab in the GitHub repository
2. Select the **CVE Container Rebuild** workflow from the left sidebar
3. Click **Run workflow** button
4. Fill in the required parameters:
   * **Release Version**: for example `1.2.0`
   * **Release Suffix**: for example `2` (used to create the suffixed images such as `strimzi/kafka-bridge:1.2.0-2` to identify different builds done for different CVEs)
   * **GitHub Actions Run ID of the source build**: the Run ID from the original build workflow that created the binaries

This process should be used only for CVEs in the base images.
Any CVEs in our code or in the Java dependencies require new patch (or minor) release.