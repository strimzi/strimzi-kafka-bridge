include ./Makefile.os
include ./Makefile.docker
include ./Makefile.maven

PROJECT_NAME ?= kafka-bridge
GITHUB_VERSION ?= master
RELEASE_VERSION ?= latest

ifneq ($(RELEASE_VERSION),latest)
  GITHUB_VERSION = $(RELEASE_VERSION)
endif

.PHONY: all
all: java_package docker_build docker_push

.PHONY: clean
clean: java_clean

.PHONY: release
release: release_prepare release_maven release_package

.PHONY: next_version
next_version:
	echo $(shell echo $(NEXT_VERSION) | tr a-z A-Z) > release.version
	mvn versions:set -DnewVersion=$(shell echo $(NEXT_VERSION) | tr a-z A-Z)
	mvn versions:commit

.PHONY: release_prepare
release_prepare:
	echo "Update release.version to $(RELEASE_VERSION)"
	echo $(shell echo $(RELEASE_VERSION) | tr a-z A-Z) > release.version
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn versions:commit

.PHONY: release_maven
release_maven:
	echo "Update pom versions to $(RELEASE_VERSION)"
	mvn versions:set -DnewVersion=$(shell echo $(RELEASE_VERSION) | tr a-z A-Z)
	mvn versions:commit

.PHONY: release_package
release_package: java_package

##########
# Documentation targets
##########

.PHONY: docu_html
docu_html: docu_htmlclean docu_check
	mkdir -p documentation/html
	$(CP) -vrL documentation/book/images documentation/html/images
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) documentation/book/master.adoc -o documentation/html/index.html

.PHONY: docu_htmlnoheader
docu_htmlnoheader: docu_htmlnoheaderclean docu_check
	mkdir -p documentation/htmlnoheader
	$(CP) -vrL documentation/book/images documentation/htmlnoheader/images
	asciidoctor -v --failure-level WARN -t -dbook -a ProductVersion=$(RELEASE_VERSION) -s documentation/book/master.adoc -o documentation/htmlnoheader/master.html

.PHONY: docu_api
docu_api: 
	mvn -P apidoc io.github.swagger2markup:swagger2markup-maven-plugin:convertSwagger2markup@generate-apidoc

.PHONY: docu_check
docu_check: docu_api
	./.travis/check_docs.sh

.PHONY: docu_clean
docu_clean: docu_htmlclean docu_htmlnoheaderclean

.PHONY: docu_htmlclean
docu_htmlclean:
	rm -rf documentation/html

.PHONY: docu_htmlnoheaderclean
docu_htmlnoheaderclean:
	rm -rf documentation/htmlnoheader

.PHONY: docu_pushtowebsite
docu_pushtowebsite: docu_htmlnoheader docu_html
	./.travis/docu-push-to-website.sh