SHELL := /bin/bash
PYTHON := python
PIP := pip

BUILD_DIR := .build

clean:
	find . -name "*.py[co]" -delete
	rm -f .coverage

distclean: clean
	rm -rf $(BUILD_DIR)

run: deps
	dev_appserver.py .

deps: py_dev_deps

py_dev_deps: $(BUILD_DIR)/pip-dev.log

$(BUILD_DIR)/pip-dev.log: requirements.txt
	@mkdir -p .build
	$(PIP) install -Ur requirements.txt | tee $(BUILD_DIR)/pip-dev.log

unit: clean
	nosetests

integrations:
	nosetests --logging-level=ERROR -a slow --with-coverage

test: clean integrations

