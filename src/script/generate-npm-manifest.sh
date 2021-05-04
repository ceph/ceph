#!/bin/bash

this_script=$(basename "$0")

function usage {
    cat <<EOM >&2

This script file is used to generate a .txt file which contains all the npm
dependencies and its version, from the package-lock.json file.

Usage:
    ${this_script} path/to/package-lock.json path/for/manifest-file.txt

Example:
    ${this_script} ../pybind/mgr/dashboard/frontend/package-lock.json ../pybind/mgr/dashboard/manifest.txt

EOM
}

empty=""
if [ "$1" == "--help" ] || [ -z $1 ]
then
    usage
    exit
fi

DEP_PATH=$1
OUT_PATH=$2

#check if package-lock.json exists
if [ -e "$DEP_PATH" ]
then
    cat $DEP_PATH | jq -r '.dependencies | to_entries[] | "\(.key)  \(.value.version)  \(.value.resolved)"' > $OUT_PATH
    echo "Manifest generated..."
else
    echo "Invalid path..."
fi
