#!/usr/bin/env bash
#
# lib-build.sh - A library of build and test bash shell functions.
#
# There should be few, or none, globals in this file beyond function
# definitions.
#
# This script should be `shellcheck`ed. Please run shellcheck when
# making changes to this script and use ignore comments
# (ref: https://www.shellcheck.net/wiki/Ignore ) to explicitly mark
# where a line is intentionally ignoring a typical rule.
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

# The following global only exists to help detect if lib-build has already been
# sourced. This is only needed because the scripts that are being migrated are
# often sourcing (as opposed to exec'ing one another).
# shellcheck disable=SC2034
_SOURCED_LIB_BUILD=1

function in_jenkins() {
    [ -n "$JENKINS_HOME" ]
}

function ci_debug() {
    if in_jenkins || [ "${FORCE_CI_DEBUG}" ]; then
        echo "CI_DEBUG: $*"
    fi
}

# get_processors returns 1/2 the value of the value returned by
# the nproc program OR the value of the environment variable NPROC
# allowing the user to tune the number of cores visible to the
# build scripts.
function get_processors() {
    # get_processors() depends on coreutils nproc.
    if [ -n "$NPROC" ]; then
        echo "$NPROC"
    else
        if [ "$(nproc)" -ge 2 ]; then
            echo "$(($(nproc) / 2))"
        else
            echo 1
        fi
    fi
}
