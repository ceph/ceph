#!/bin/bash
source test/docker-test-helper.sh
main_docker "$@" --os-type centos --os-version centos7 --dev -- ./run-make-check.sh --enable-root-make-check
