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

# has_build_dir returns true if a build directory exists and can be used
# for builds. has_build_dir is designed to interoperate with do_cmake.sh
# and uses the same BUILD_DIR environment variable. It checks for the
# directory relative to the current working directory.
function has_build_dir() {
    ( cd "${BUILD_DIR:=build}" && [[ -f build.ninja || -f Makefile ]] )
}

# discover_compiler takes one argument, purpose, which may be used
# to adjust the results for a specific need. It sets three environment
# variables `discovered_c_compiler`, `discovered_cxx_compiler` and
# `discovered_compiler_env`. The `discovered_compiler_env` variable
# may be blank. If not, it will contain a file that needs to be sourced
# prior to using the compiler.
function discover_compiler() {
    # nb: currently purpose is not used for detection
    local purpose="$1"
    ci_debug "Finding compiler for ${purpose}"

    local compiler_env=""
    local cxx_compiler=g++
    local c_compiler=gcc
    # ubuntu/debian ci builds prefer clang
    for i in {17..12}; do
        if type -t "clang-$i" > /dev/null; then
            cxx_compiler="clang++-$i"
            c_compiler="clang-$i"
            break
        fi
    done
    # but if this is {centos,rhel} we need gcc-toolset
    if [ -f "/opt/rh/gcc-toolset-11/enable" ]; then
        ci_debug "Detected SCL gcc-toolset-11 environment file"
        compiler_env="/opt/rh/gcc-toolset-11/enable"
        # shellcheck disable=SC1090
        cxx_compiler="$(. ${compiler_env} && command -v g++)"
        # shellcheck disable=SC1090
        c_compiler="$(. ${compiler_env} && command -v gcc)"
    fi

    export discovered_c_compiler="${c_compiler}"
    export discovered_cxx_compiler="${cxx_compiler}"
    export discovered_compiler_env="${compiler_env}"
    return 0
}
