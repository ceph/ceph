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

function get_ceph() {
    local git_base_url=$1
    local sha1=$2

    test -d ceph || git clone ${git_base_url}/ceph
    cd ceph
    git checkout ${sha1}
}
