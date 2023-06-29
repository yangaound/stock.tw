.PHONY: format

all: format

format:
	autoflake -ir --remove-all-unused-imports bin lib; \
	isort --quiet bin lib; \
	black --preview bin lib;
