# Building Strimzi Kafka Bridge

Strimzi Kafka Bridge uses `make` as its main build system. 
Our `make` build supports several different targets, mainly for building the Java binaries and pushing Docker images.

## Build Pre-Requisites

To build this project you must first install several command line utilities.

- [`make`](https://www.gnu.org/software/make/) - Make build system
- [`mvn`](https://maven.apache.org/index.html) - Maven CLI
- [`docker`](https://www.docker.com/) - Docker or [`podman`](https://podman.io/)

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

## Building container images for other platforms with Docker `buildx`

Docker supports building images for different platforms using the `docker buildx` command. If you want to use it to
build Strimzi images, you can just set the environment variable `DOCKER_BUILDX` to `buildx`, set the environment
variable `DOCKER_BUILD_ARGS` to pass additional build options such as the platform and run the build. For example
following can be used to build Strimzi images for Linux on PowerPC/ppc64le architecture:

```
export DOCKER_BUILDX=buildx
export DOCKER_BUILD_ARGS="--platform linux/ppc64le"
make all
```
