#!/bin/bash

#
# ps-ceph.sh: Displays a list of ceph processes running locally
#
# We're using Bash pattern matching. So this isn't POSIX-compatible.
#
# Copyright (C) 2010, Dreamhost
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software 
# Foundation.  See file COPYING.
#

shopt -s extglob # turn on advanced Bash pattern matching

cleanup() {
        rm -f "${TEMPFILE}"
}

is_my_process() {
        username="$1"
        [[ "${username}" == "${whoami}" ]] && return 1
        [[ "${username}" == "root" ]] && return 1
        return 0
}

is_ceph_process() {
        pname="$1"
        [[ "${pname}" == ?(!([:alnum:]))ceph?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))cfuse?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))cmds?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))cmon?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))cosd?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))osdmaptool?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))rados?([:blank:]|) ]] && return 1
        [[ "${pname}" == ?(!([:alnum:]))vstart.sh?([:blank:]|) ]] && return 1
        [[ "${pname}" == test/test_* ]] && return 1
        return 0
}

TEMPFILE=`mktemp`
trap cleanup INT TERM EXIT
whoami=`whoami`

ps aux > "${TEMPFILE}"

while read line; do
        uname=`echo "${line}" | awk '{ print $1 }'`
        is_my_process "${uname}"
        [ $? -eq 0 ] && continue

        pname=`echo "${line}" | awk '{ print $11 }'`
        is_ceph_process "${pname}"
        [ $? -eq 0 ] && continue

        echo ${line}
done < "${TEMPFILE}"
