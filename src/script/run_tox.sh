#!/usr/bin/env bash

set -e

if [ `uname` = FreeBSD ]; then
    GETOPT=/usr/local/bin/getopt
else
    GETOPT=getopt
fi

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

    options=$(${GETOPT} --name "$0" --options '' --longoptions "source-dir:,build-dir:,tox-path:,tox-envs:,venv-path:" -- "$@")
    if [ $? -ne 0 ]; then
        exit 2
    fi
    eval set -- "${options}"
    while true; do
        case "$1" in
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

    # normalize options
    [ "$with_python2" = "ON" ] && with_python2=true || with_python2=false
    # WITH_PYTHON3 might be set to "ON" or to the python3 RPM version number
    # prevailing on the system - e.g. "3", "36"
    [[ "$with_python3" =~ (^3|^ON) ]] && with_python3=true || with_python3=false

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
        $source_dir/src/tools/setup-virtualenv.sh ${venv_path}
    fi
    source ${venv_path}/bin/activate
    pip install tox

    # tox.ini will take care of this.
    export CEPH_BUILD_DIR=$build_dir

    tox -c $tox_path/tox.ini -e "$tox_envs" "$@"
}

main "$@"
