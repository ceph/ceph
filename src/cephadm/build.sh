#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

clean_up() {
    if [ -e ${buildir} ]; then
        rm -rf ${builddir}
    fi
}
trap clean_up EXIT

PYTHON=$(which python3)

# Create build directory and install required dependencies
target_fpath=${SCRIPT_DIR}/cephadm
if [ -n "$1" ]; then
    target_fpath="$1"
fi
builddir=$(mktemp -d)
if [ -e "requirements.txt" ]; then
    $PYTHON -m pip install -r requirements.txt --target ${builddir}
fi

# Make sure all newly created source files are copied here as well!
cp ${SCRIPT_DIR}/cephadm.py ${builddir}/__main__.py

version=$($PYTHON --version)
if [[ "$version" =~ ^Python[[:space:]]([[:digit:]]+)\.([[:digit:]]+)\.([[:digit:]]+)$ ]]; then
    major=${BASH_REMATCH[1]}
    minor=${BASH_REMATCH[2]}

    compress=""
    if [[ "$major" -ge 3 && "$minor" -ge 7 ]]; then
        echo "Python version compatible with --compress, compressing cephadm binary"
        compress="--compress"
    elif [[ "$major" -lt 3 || "$major" -eq 3 && "$minor" -lt 5 ]]; then
	echo "zipapp module requires Python35 or greater"
	exit 1
    fi

    $PYTHON -mzipapp -p $PYTHON ${builddir} ${compress} --output $target_fpath
    echo written to ${target_fpath}
else
    echo "Couldn't parse Python version"
    exit 1
fi
