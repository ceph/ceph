#!/usr/bin/env bash

with_libvirt() {
    if [[ -n "${JENKINS_HOME}" ]]; then
        sg libvirt -c "$1"
    else
        eval "$1"
    fi
}
