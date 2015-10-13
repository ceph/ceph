#!/bin/bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
function install_deps() {
    git archive --remote=git://git.ceph.com/ceph.git master install-deps.sh | tar -xvf -
    bash -x install-deps.sh
}

function git_submodules() {
    # see http://tracker.ceph.com/issues/13426
    perl -pi -e 's|git://ceph.com/git/ceph-object-corpus.git|https://github.com/ceph/ceph-object-corpus.git|' .gitmodules
    local force=$(if git submodule usage 2>&1 | grep --quiet 'update.*--force'; then echo --force ; fi)
    git submodule sync || return 1
    git submodule update $force --init --recursive || return 1
}

function get_ceph() {
    local git_ceph_url=$1
    local sha1=$2

    test -d ceph || git clone ${git_ceph_url}
    cd ceph
    git checkout ${sha1}
}

function init_ceph() {
    local git_ceph_url=$1
    local sha1=$2
    get_ceph $git_ceph_url $sha1 || return 1
    git_submodules || return 1
    install_deps || return 1
}

function flavor2configure() {
    local flavor=$1

    if test $flavor = notcmalloc ; then
        echo --without-tcmalloc --without-cryptopp
    fi
}
