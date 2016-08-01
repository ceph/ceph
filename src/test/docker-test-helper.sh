#!/bin/bash
#
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
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
function get_image_name() {
    local os_type=$1
    local os_version=$2

    echo ceph-$os_type-$os_version-$USER
}

function setup_container() {
    local os_type=$1
    local os_version=$2
    local opts="$3"

    local image=$(get_image_name $os_type $os_version)
    local build=true
    if docker images $image | grep --quiet "^$image " ; then
        eval touch --date=$(docker inspect $image | jq '.[0].Created') $image
        found=$(find -L test/$os_type-$os_version/* -newer $image)
        rm $image
        if test -n "$found" ; then
            docker rmi $image
        else
            build=false
        fi
    fi
    if $build ; then
        # 
        # In the dockerfile,
        # replace environment variables %%FOO%% with their content
        #
        rm -fr dockerfile
        cp --dereference --recursive test/$os_type-$os_version dockerfile
        os_version=$os_version user_id=$(id -u) \
            perl -p -e 's/%%(\w+)%%/$ENV{$1}/g' \
            dockerfile/Dockerfile.in > dockerfile/Dockerfile
        docker $opts build --tag=$image dockerfile
        rm -fr dockerfile
    fi
}

function get_upstream() {
    git rev-parse --show-toplevel
}

function get_downstream() {
    local os_type=$1
    local os_version=$2

    local image=$(get_image_name $os_type $os_version)
    local upstream=$(get_upstream)
    local dir=$(dirname $upstream)
    echo "$dir/$image"
}

function setup_downstream() {
    local os_type=$1
    local os_version=$2
    local ref=$3

    local image=$(get_image_name $os_type $os_version)
    local upstream=$(get_upstream)
    local dir=$(dirname $upstream)
    local downstream=$(get_downstream $os_type $os_version)
    
    (
        cd $dir
        if ! test -d $downstream ; then
            # Inspired by https://github.com/git/git/blob/master/contrib/workdir/git-new-workdir
            mkdir -p $downstream/.git || return 1
            for x in config refs logs/refs objects info hooks packed-refs remotes rr-cache
            do
	        case $x in
	            */*)
		        mkdir -p "$downstream/.git/$x"
		        ;;
	        esac
	        ln -s "$upstream/.git/$x" "$downstream/.git/$x"
                cp "$upstream/.git/HEAD" "$downstream/.git/HEAD"
            done
        fi
        cd $downstream
        git reset --hard $ref || return 1
        git submodule sync --recursive || return 1
        git submodule update --force --init --recursive || return 1
    )
}

function run_in_docker() {
    local os_type=$1
    shift
    local os_version=$1
    shift
    local ref=$1
    shift
    local opts="$1"
    shift
    local script=$1

    setup_downstream $os_type $os_version $ref || return 1
    setup_container $os_type $os_version "$opts" || return 1
    local downstream=$(get_downstream $os_type $os_version)
    local image=$(get_image_name $os_type $os_version)
    local upstream=$(get_upstream)
    local ccache
    mkdir -p $HOME/.ccache
    ccache="--volume $HOME/.ccache:$HOME/.ccache"
    user="--user $USER"
    local cmd="docker run $opts --rm --name $image --privileged $ccache"
    cmd+=" --volume $downstream:$downstream"
    cmd+=" --volume $upstream:$upstream"
    local status=0
    if test "$script" = "SHELL" ; then
        $cmd --tty --interactive --workdir $downstream $user $image bash
    else
        if ! $cmd --workdir $downstream $user $image "$@" ; then
            status=1
        fi
    fi
    return $status
}

function remove_all() {
    local os_type=$1
    local os_version=$2
    local image=$(get_image_name $os_type $os_version)

    docker rm $image
    docker rmi $image
}

function usage() {
    cat <<EOF
Run commands within Ceph sources, in a docker container
$0 [options] command args ...

   [-h|--help]            display usage
   [--verbose]            trace all shell lines

   [--os-type type]       docker image repository (centos, ubuntu, etc.) 
                          (defaults to ubuntu)
   [--os-version version] docker image tag (7 for centos, 12.04 for ubuntu, etc.)
                          (defaults to 14.04)
   [--ref gitref]         git reset --hard gitref before running the command
                          (defaults to git rev-parse HEAD)
   [--all types+versions] list of docker image repositories and tags

   [--shell]              run an interactive shell in the container
   [--remove-all]         remove the container and the image for the specified types+versions

   [--opts options]       run the contain with 'options'

docker-test.sh must be run from a Ceph clone and it will run the
command in a container, using a copy of the clone so that long running
commands such as make check are not disturbed while development
continues. Here is a sample use case including an interactive session
and running a unit test:

   $ lsb_release -d
   Description:	Ubuntu Trusty Tahr (development branch)
   $ test/docker-test.sh --os-type centos --os-version 7 --shell
   HEAD is now at 1caee81 autotools: add --enable-docker
   bash-4.2$ pwd
   /srv/ceph/ceph-centos-7
   bash-4.2$ lsb_release -d
   Description:	CentOS Linux release 7.0.1406 (Core) 
   bash-4.2$ 
   $ time test/docker-test.sh --os-type centos --os-version 7 unittest_str_map
   HEAD is now at 1caee81 autotools: add --enable-docker
   Running main() from gtest_main.cc
   [==========] Running 2 tests from 1 test case.
   [----------] Global test environment set-up.
   [----------] 2 tests from str_map
   [ RUN      ] str_map.json
   [       OK ] str_map.json (1 ms)
   [ RUN      ] str_map.plaintext
   [       OK ] str_map.plaintext (0 ms)
   [----------] 2 tests from str_map (1 ms total)
   
   [----------] Global test environment tear-down
   [==========] 2 tests from 1 test case ran. (1 ms total)
   [  PASSED  ] 2 tests.
   
   real	0m3.759s
   user	0m0.074s
   sys	0m0.051s

The --all argument is a bash associative array literal listing the
operating system version for each operating system type. For instance

   docker-test.sh --all '([ubuntu]="12.04 14.04" [centos]="6 7")' 

is strictly equivalent to

   docker-test.sh --os-type ubuntu --os-version 12.04
   docker-test.sh --os-type ubuntu --os-version 14.04
   docker-test.sh --os-type centos --os-version 6
   docker-test.sh --os-type centos --os-version 7

The --os-type and --os-version must be exactly as displayed by docker images:

   $ docker images
   REPOSITORY            TAG                 IMAGE ID          ...
   centos                7                   87e5b6b3ccc1      ...
   ubuntu                14.04               6b4e8a7373fe      ...

The --os-type value can be any string in the REPOSITORY column, the --os-version
can be any string in the TAG column.

The --shell and --remove actions are mutually exclusive.

Run make check in centos 7
docker-test.sh --os-type centos --os-version 7 -- make check

Run make check on a giant
docker-test.sh --ref giant -- make check

Run an interactive shell and set resolv.conf to use 172.17.42.1
docker-test.sh --opts --dns=172.17.42.1 --shell

Run make check on centos 6, centos 7, ubuntu 12.04 and ubuntu 14.04
docker-test.sh --all '([ubuntu]="12.04 14.04" [centos]="6 7")' -- make check
EOF
}

function main_docker() {
    if ! docker ps > /dev/null 2>&1 ; then
        echo "docker not available: $0"
        return 0
    fi

    local temp
    temp=$(getopt -o scht:v:o:a:r: --long remove-all,verbose,shell,help,os-type:,os-version:,opts:,all:,ref: -n $0 -- "$@") || return 1

    eval set -- "$temp"

    local os_type=ubuntu
    local os_version=14.04
    local all
    local remove=false
    local shell=false
    local opts
    local ref=$(git rev-parse HEAD)

    while true ; do
	case "$1" in
            --remove-all)
                remove=true
                shift
                ;;
            --verbose)
                set -xe
                PS4='${BASH_SOURCE[0]}:$LINENO: ${FUNCNAME[0]}:  '
                shift
                ;;
            -s|--shell)
                shell=true
                shift
                ;;
	    -h|--help)
                usage
                return 0
                ;;
	    -t|--os-type) 
                os_type=$2
                shift 2
                ;;
	    -v|--os-version) 
                os_version=$2
                shift 2
                ;;
	    -o|--opts) 
                opts="$2"
                shift 2
                ;;
	    -a|--all) 
                all="$2"
                shift 2
                ;;
	    -r|--ref) 
                ref="$2"
                shift 2
                ;;
	    --)
                shift
                break
                ;;
	    *)
                echo "unexpected argument $1"
                return 1
                ;;
	esac
    done

    if test -z "$all" ; then
        all="([$os_type]=\"$os_version\")"
    fi

    declare -A os_type2versions
    eval os_type2versions="$all"

    for os_type in ${!os_type2versions[@]} ; do
        for os_version in ${os_type2versions[$os_type]} ; do
            if $remove ; then
                remove_all $os_type $os_version || return 1
            elif $shell ; then
                run_in_docker $os_type $os_version $ref "$opts" SHELL || return 1
            else
                run_in_docker $os_type $os_version $ref "$opts" "$@" || return 1
            fi
        done
    done
}
