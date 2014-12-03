#!/bin/bash
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
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

    echo ceph-$os_type-$os_version
}

function get_branch() {
    git rev-parse --abbrev-ref HEAD
}

function compile_ubuntu() {
    cat <<EOF
set -ex
if ! test -f config.status ; then
   ./autogen.sh
   ./configure --disable-static --with-debug CC='ccache gcc' CXX='ccache g++' CFLAGS="-Wall -g" CXXFLAGS="-Wall -g"
fi
make -j4
EOF
}

function compile_centos() {
    cat <<EOF
set -ex
if ! test -f config.status ; then
   ./autogen.sh
   ./configure --disable-static --with-debug CC='ccache gcc' CXX='ccache g++' CFLAGS="-Wall -g" CXXFLAGS="-Wall -g"
fi
make -j4
EOF
}

function setup_container() {
    local os_type=$1
    local os_version=$2
    local opts="$3"

    local image=$(get_image_name $os_type $os_version)
    if ! docker images $image | grep --quiet "^$image " ; then
        # 
        # In the dockerfile,
        # replace environment variables %%FOO%% with their content except
        #
        cat test/$os_type.dockerfile | \
            os_version=$os_version user_id=$(id -u) \
            perl -p -e 's/%%(\w+)%%/$ENV{$1}/g' | \
            docker $opts build --tag=$image -
    fi
}

function get_downstream() {
    local os_type=$1
    local os_version=$2

    local image=$(get_image_name $os_type $os_version)
    local root=$(git rev-parse --show-toplevel)
    local dir=$(dirname $root)
    local upstream=$(basename $root)
    echo "$dir/$image"
}

function setup_downstream() {
    local os_type=$1
    local os_version=$2

    local image=$(get_image_name $os_type $os_version)
    local root=$(git rev-parse --show-toplevel)
    local branch=$(get_branch)
    local dir=$(dirname $root)
    local upstream=$(basename $root)
    local downstream=$(get_downstream $os_type $os_version)
    
    (
        cd $dir
        if ! test -d $downstream ; then
            git clone $upstream $downstream || return 1
            cd $downstream
            git checkout -b docker-build origin/$branch || return 1
            git submodule update --init || return 1
        else
            cd $downstream
        fi
        git fetch origin
        git reset --hard origin/$branch || return 1
    )
}

function run_in_docker() {
    local os_type=$1
    shift
    local os_version=$1
    shift
    local dev=$1
    shift
    local user=$1
    shift
    local opts="$1"
    shift
    local script=$1

    setup_downstream $os_type $os_version || return 1
    setup_container $os_type $os_version "$opts" || return 1
    local downstream=$(get_downstream $os_type $os_version)
    local image=$(get_image_name $os_type $os_version)
    local ccache
    if test -d $HOME/.ccache ; then
        ccache="--volume $HOME/.ccache:$HOME/.ccache"
    fi
    if $dev ; then
        dev="--volume /dev:/dev"
    else
        dev=
    fi
    if test $user != root ; then
        user="--user $user"
    else
        user=
    fi
    local cmd="docker run $opts --rm --name $image --privileged $ccache --volume $downstream:$downstream"
    local status=0
    if test "$script" = "bash" ; then
        $cmd --tty --interactive --workdir $downstream $user $dev $image bash
    elif test "$script" = "compile" ; then
        local compile=$downstream/compile.$$
        compile_$os_type > $compile
        if ! $cmd --workdir $downstream $user $image bash $compile ; then
            status=1
        fi
        rm $compile
    else
        if ! $cmd --workdir $downstream/src $user $dev $image "$@" ; then
            status=1
        fi
    fi
    return $status
}

declare -A OS_TYPE2VERSIONS=([ubuntu]="12.04 14.04" [centos]="centos6 centos7")

function self_in_docker() {
    local script=$1
    shift

    if test $# -gt 0 ; then
        eval OS_TYPE2VERSIONS="$@"
    fi
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
   [--os-version version] docker image tag (centos6 for centos, 12.04 for ubuntu, etc.)
                          (defaults to 14.04)
   [--all types+versions] list of docker image repositories and tags

   [--shell]              run an interactive shell in the container
   [--compile]            run ./autogen.sh && ./configure && make in the specified types+versions
   [--remove-all]         remove the container and the image for the specified types+versions

   [--dev]                run the container with --volume /dev:/dev
   [--user name]          execute the command as user 'name' (defaults to $USER)
   [--opts options]       run the contain with 'options'

docker-test.sh must be run from a Ceph clone and it will run the
command in a container, using a copy of the clone so that long running
commands such as make check are not disturbed while development
continues. Here is a sample use case including an interactive session
and running a unit test:

   $ lsb_release -d
   Description:	Ubuntu Trusty Tahr (development branch)
   $ test/docker-test.sh --os-type centos --os-version centos7 --shell
   HEAD is now at 1caee81 autotools: add --enable-docker
   bash-4.2$ pwd
   /srv/ceph/ceph-centos-centos7
   bash-4.2$ lsb_release -d
   Description:	CentOS Linux release 7.0.1406 (Core) 
   bash-4.2$ 
   $ time test/docker-test.sh --os-type centos --os-version centos7 unittest_str_map
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

   docker-test.sh --all '([ubuntu]="12.04 14.04" [centos]="centos6 centos7")' 

is strictly equivalent to

   docker-test.sh --os-type ubuntu --os-version 12.04
   docker-test.sh --os-type ubuntu --os-version 14.04
   docker-test.sh --os-type centos --os-version centos6
   docker-test.sh --os-type centos --os-version centos7

The --os-type and --os-version must be exactly as displayed by docker images:

   $ docker images
   REPOSITORY            TAG                 IMAGE ID          ...
   centos                centos7             87e5b6b3ccc1      ...
   ubuntu                14.04               6b4e8a7373fe      ...

The --os-type value can be any string in the REPOSITORY column, the --os-version
can be any string in the TAG column.

The --shell, --compile and --remove actions are mutually exclusive.

Compile in centos7
docker-test.sh --os-type centos --os-version centos7 --compile

Run make check in centos7
docker-test.sh --os-type centos --os-version centos7 -- make check

Run a test as root with access to the host /dev for losetup to work
docker-test.sh --user root --dev -- make TESTS=test/ceph-disk-root.sh check

Run an interactive shell and set resolv.conf to use 172.17.42.1
docker-test.sh --opts --dns=172.17.42.1 --shell

Run make check on centos6, centos7, ubuntu-12.04 and ubuntu-14.04
docker-test.sh --all '([ubuntu]="12.04 14.04" [centos]="centos6 centos7")' -- make check
EOF
}

function main_docker() {
    if ! docker ps > /dev/null 2>&1 ; then
        echo "docker not available: $0"
        return 0
    fi

    local temp
    temp=$(getopt -o scdht:v:u:o:a: --long remove-all,verbose,shell,compile,dev,help,os-type:,os-version:,user:,opts:,all: -n $0 -- "$@") || return 1

    eval set -- "$temp"

    local os_type=ubuntu
    local os_version=14.04
    local all
    local remove=false
    local shell=false
    local compile=false
    local dev=false
    local user=$USER
    local opts
    while true ; do
	case "$1" in
            --remove-all)
                remove=true
                shift
                ;;
            --verbose)
                set -xe
                PS4='${FUNCNAME[0]}: $LINENO: '
                shift
                ;;
            -s|--shell)
                shell=true
                shift
                ;;
            -c|--compile)
                compile=true
                shift
                ;;
            -d|--dev)
                dev=true
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
	    -u|--user) 
                user="$2"
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
                run_in_docker $os_type $os_version $dev $user "$opts" bash || return 1
            elif $compile ; then
                run_in_docker $os_type $os_version $dev $user "$opts" compile || return 1
            else
                run_in_docker $os_type $os_version $dev $user "$opts" "$@" || return 1
            fi
        done
    done
}
