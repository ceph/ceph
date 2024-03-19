#!/usr/bin/env bash

set -e

if [ `uname` = FreeBSD ]; then
    GETOPT=/usr/local/bin/getopt
else
    GETOPT=getopt
fi

function usage() {
    local prog_name=$(basename $1)
    shift
    cat <<EOF
$prog_name [options] ... [test_name]

options:

  [-h|--help]         display this help message
  [--source-dir dir]  root source directory of Ceph. deduced by the path of this script by default.
  [--build-dir dir]   build directory of Ceph. "\$source_dir/build" by default.
  [--tox-path dir]    directory in which "tox.ini" is located. if "test_name" is not specified, it is the current directory by default, otherwise the script will try to find a directory with the name of specified \$test_name with a "tox.ini" under it.
  <--tox-envs envs>   tox envlist. this option is required.
  [--venv-path]       the python virtualenv path. \$build_dir/\$test_name by default.

example:

following command will run tox with envlist of "py3,mypy" using the "tox.ini" in current directory.

  $prog_name --tox-envs py3,mypy

following command will run tox with envlist of "py3" using "/ceph/src/python-common/tox.ini"

  $prog_name --tox-envs py3 --tox-path /ceph/src/python-common
EOF
}

function get_cmake_variable() {
    local cmake_cache=$1/CMakeCache.txt
    shift
    local variable=$1
    shift
    if [ -e $cmake_cache ]; then
        grep "$variable" $cmake_cache | cut -d "=" -f 2
    fi
}

function get_tox_path() {
    local test_name=$1
    if [ -n "$test_name" ]; then
        local found=$(find $source_dir -path "*/$test_name/tox.ini")
        echo $(dirname $found)
    elif [ -e tox.ini ]; then
        echo $(pwd)
    fi
}

function main() {
    local tox_path
    local script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    local build_dir=$script_dir/../../build
    local source_dir=$(get_cmake_variable $build_dir ceph_SOURCE_DIR)
    local tox_envs
    local options

    options=$(${GETOPT} --name "$0" --options 'h' --longoptions "help,source-dir:,build-dir:,tox-path:,tox-envs:,venv-path:" -- "$@")
    if [ $? -ne 0 ]; then
        exit 2
    fi
    eval set -- "${options}"
    while true; do
        case "$1" in
            -h|--help)
                usage $0
                exit 0;;
            --source-dir)
                source_dir=$2
                shift 2;;
            --build-dir)
                build_dir=$2
                shift 2;;
            --tox-path)
                tox_path=$2
                shift 2;;
            --tox-envs)
                tox_envs=$2
                shift 2;;
            --venv-path)
                venv_path=$2
                shift 2;;
            --)
                shift
                break;;
            *)
                echo "bad option $1" >& 2
                exit 2;;
        esac
    done

    local test_name
    if [ -z "$tox_path" ]; then
        # try harder
        if [ $# -gt 0 ]; then
            test_name=$1
            shift
        fi
        tox_path=$(get_tox_path $test_name)
        venv_path="$build_dir/$test_name"
    else
        test_name=$(basename $tox_path)
    fi

    if [ ! -f ${venv_path}/bin/activate ]; then
        if [ -d "$venv_path" ]; then
            cd $venv_path
            echo "$PWD already exists, but it's not a virtualenv. test_name empty?"
            exit 1
        fi
        $source_dir/src/tools/setup-virtualenv.sh ${venv_path}
    fi
    source ${venv_path}/bin/activate
    pip install tox

    # tox.ini will take care of this.
    export CEPH_BUILD_DIR=$build_dir
    # use the wheelhouse prepared by install-deps.sh
    export PIP_FIND_LINKS="$tox_path/wheelhouse"
    tox_cmd=(tox -c $tox_path/tox.ini)
    if [ "$tox_envs" != "__tox_defaults__" ]; then
        tox_cmd+=("-e" "$tox_envs")
    fi
    "${tox_cmd[@]}" "$@"
}

main "$@"
