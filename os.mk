FIND = find
SED = sed
GREP = grep
CP = cp
DOCKER = docker

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	FIND = gfind
	SED = gsed
	GREP = ggrep
	CP = gcp
endif