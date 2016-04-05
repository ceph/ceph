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

    test -d ceph || git clone ${git_ceph_url} ceph
    cd ceph
    if test -d src ; then # so we don't try to fetch when using a fixture
       git fetch --tags http://github.com/ceph/ceph
    fi
    git fetch --tags ${git_ceph_url}
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

    eval $(dpkg-architecture)

    if test $flavor = notcmalloc || test "$DEB_HOST_GNU_CPU" = aarch64 ; then
        echo --without-tcmalloc --without-cryptopp
    fi
}

#
# for a given $sha1 in the $ceph_dir repository, lookup all references
# from the remote origin and tags matching the sha1. Add a symbolic
# link in $ref_dir to the $sha1 for each reference found. If the
# reference is a tag, also add a symbolic link to the commit to which
# the tag points, if it is an annotated tag.
#
function link_same() {
    local ref_dir=$1
    local ceph_dir=$2
    local sha1=$3

    mkdir -p $ref_dir
    (
        cd ${ceph_dir}
        git for-each-ref refs/tags/** refs/remotes/origin/** | grep $sha1 | \
            while read sha1 type ref ; do
                if test $type = 'tag' ; then
                    commit_sha1=$(git rev-parse $ref^{commit})
                    if test $commit_sha1 != $sha1 ; then
                        echo ../sha1/$sha1 ../sha1/$commit_sha1
                    fi
                fi
                echo ../sha1/$sha1 $(basename $ref)
            done
    ) | while read from to ; do
        ( cd $ref_dir ; ln -sf $from $to )
    done
}

function test_link_same() {
    local d=/tmp/link_same$$
    mkdir -p $d/primary
    cd $d/primary
    git init
    touch a ; git add a ; git commit -m 'm' a
    git tag tag1
    tag1=$(git rev-parse HEAD)
    git branch branch1
    touch b ; git add b ; git commit -m 'm' b
    git tag --annotate -m 'a' tag2
    tag2=$(git rev-parse tag2)
    sha1_tag2=$(git rev-parse tag2^{commit})
    git branch branch2
    touch c ; git add c ; git commit -m 'm' c
    git branch branch3
    sha1_branch3=$(git rev-parse branch3)

    git clone $d/primary $d/secondary
    cd $d/secondary
    mkdir $d/ref $d/sha1

    touch $d/sha1/$sha1_branch3
    link_same $d/ref $d/secondary $sha1_branch3
    test $(readlink --canonicalize $d/ref/branch3) = $d/sha1/$sha1_branch3 || return 1
    test $(readlink --canonicalize $d/ref/master) = $d/sha1/$sha1_branch3 || return 1

    touch $d/sha1/$tag2
    link_same $d/ref $d/secondary $tag2
    test $(readlink --canonicalize $d/ref/tag2) = $d/sha1/$tag2 || return 1
    test $(readlink --canonicalize $d/sha1/$sha1_tag2) = $d/sha1/$tag2 || return 1

    touch $d/sha1/$tag1
    link_same $d/ref $d/secondary $tag1
    test $(readlink --canonicalize $d/ref/tag1) = $d/sha1/$tag1 || return 1
    test $(readlink --canonicalize $d/ref/branch1) = $d/sha1/$tag1 || return 1

    rm -fr $d
}

function maybe_parallel() {
    local nproc=$1
    local vers=$2

    if echo $vers | grep --quiet '0\.67' ; then
        return
    fi

    if test $nproc -gt 1 ; then
        echo -j${nproc}
    fi
}

function test_maybe_parallel() {
    test "$(maybe_parallel 1 0.72)" = "" || return 1
    test "$(maybe_parallel 8 0.67)" = "" || return 1
    test "$(maybe_parallel 8 0.72)" = "-j8" || return 1
}

if test "$1" = "TEST" ; then
    shopt -s -o xtrace
    PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '
    test_link_same
    test_maybe_parallel
fi
