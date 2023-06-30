.PHONY: format

all: format

format:
	autoflake -ir --remove-all-unused-imports bin stock_tw; \
	isort --quiet bin stock_tw; \
	black --preview bin stock_tw;
