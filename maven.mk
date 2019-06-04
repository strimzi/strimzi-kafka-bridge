# Makefile.maven contains the shared tasks for building Java applications. This file is
# included into the Makefile files which contain some Java sources which should be build

.PHONY: java_compile
java_compile:
	echo "Building JAR file ..."
	mvn $(MVN_ARGS) compile

.PHONY: java_verify
java_verify:
	echo "Building JAR file ..."
	mvn $(MVN_ARGS) verify

src := $(shell find src/ | sort)
#deps := $(shell mvn dependency:resolve -DoutputAbsoluteArtifactFilename=true | grep -F '[INFO]    ' | sed 's/.*://g')

target/kafka-bridge-$(RELEASE_VERSION).zip: pom.xml $(src) $(deps)

target/kafka-bridge-$(RELEASE_VERSION).zip:
	echo "Packaging project ..."
	mvn $(MVN_ARGS) package

.PHONY: java_package
java_package: target/kafka-bridge-$(RELEASE_VERSION).zip

.PHONY: java_install
java_install:
	echo "Installing JAR files ..."
	mvn $(MVN_ARGS) install

.PHONY: java_clean
java_clean:
	echo "Cleaning Maven build ..."
	mvn clean

.PHONY: findbugs
findbugs: java_compile
	mvn $(MVN_ARGS) org.codehaus.mojo:findbugs-maven-plugin:check
