# Building Strimzi Kafka Bridge

Strimzi Kafka Bridge uses `make` as its main build system. 
Our `make` build supports several different targets, mainly for building the Java binaries and pushing Docker images.

## Build Pre-Requisites

To build this project you must first install several command line utilities.

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) - Maven CLI
- [`docker`](https://www.docker.com/) - Docker

In order to use `make` these all need to be available in your `$PATH`.

### Mac OS

The `make` build is using the GNU versions of the `find` and `sed` utilities and is not compatible with the BSD versions available on Mac OS. 
When using Mac OS, you have to install the GNU versions of `find` and `sed`.
When using `brew`, you can do `brew install gnu-sed findutils grep coreutils asciidoctor`.
This command will install the GNU versions as `gcp`, `ggrep`, `gsed` and `gfind` and our `make` build will automatically pick them up and use them.

## Docker image

### Building Docker images

The `docker_build` target will build the Docker image provided by the Strimzi Kafka Bridge project.
You can build all Strimzi Docker images by calling `make docker_build` from the root of the repository.
The `docker_build` target will always build the images under the `strimzi` organization. 
The `DOCKER_TAG` environment variable configures the Docker tag to use (default is `latest`).

### Tagging and pushing Docker images

The `docker_tag` target can be used to tag the Docker images built by the `docker_build` target. 
This target is automatically called by the `docker_push` target and doesn't have to be called separately. 

To configure the `docker_tag` and `docker_push` targets you can set following environment variables:
* `DOCKER_ORG` configures the Docker organization for tagging/pushing the images (defaults to the value of the `$USER` environment variable)
* `DOCKER_TAG` configured Docker tag (default is `latest`)
* `DOCKER_REGISTRY` configures the Docker registry where the image will be pushed (default is `docker.io`)

## Building everything

The `make all` command can be used to trigger all the tasks above - build the Java code, Docker image, tag it and push it to the configured repository.

`make` invokes Maven for packaging Java based applications. 
The `mvn` command can be customized by setting the `MVN_ARGS` environment variable when launching `make all`. 
For example, `MVN_ARGS=-DskipTests make all` can be used to avoid running the unit tests.

## Release

The `make release` target can be used to create a release. 
The `RELEASE_VERSION` environment variable (default value `latest`) can be used to define the release version. 
The `release` target will:
* Update all the tags of Docker images to `RELEASE_VERSION`
* Update the documentation version to `RELEASE_VERSION`
* Set the version of the main Maven project to `RELEASE_VERSION` 
* Create ZIP and TAR.GZ archives with the Strimzi Kafka Bridge which can be used outside of Kubernetes / OpenShift
 
The `release` target will not build the Docker images - they should be built and pushed automatically by Travis CI when the release is tagged in the GitHub repository. 
It also doesn't deploy the Java artifacts anywhere. 

The release process should normally look like this:
1. Create a release branch
2. Export the desired version into the environment variable `RELEASE_VERSION`
3. Run `make release`
4. Commit the changes to the existing files (do not add the ZIP and TAR.GZ archives into Git)
5. Push the changes to the release branch on GitHub
6. Create the tag and push it to GitHub. 
The tag name determines the tag of the resulting Docker images. 
Therefore the git tag name has to be the same as the `RELEASE_VERSION`,
7. Once the CI build for the tag is finished and the Docker images are pushed to Docker Hub, create a GitHub release based on the tag. 
Attach the ZIP and TAR.GZ archives to the release.
8. Build the documentation using `make docu_html` and `make docu_htmlnoheader` and add it to the [Strimzi.io](https://strimzi.io) website.
8. On the `master` git branch:
  * Update the versions to the next SNAPSHOT version using the `next_version` `make` target. 
  For example to update the next version to `0.6.0-SNAPSHOT` run: `make NEXT_VERSION=0.6.0-SNAPSHOT next_version`.
