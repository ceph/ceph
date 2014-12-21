#!/bin/bash
source test/docker-test-helper.sh
main_docker "$@" --os-type ubuntu --os-version 14.04 --dev -- ./run-make-check.sh --enable-root-make-check
