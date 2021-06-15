#!/usr/bin/env bash

set -ex

target_fpath="$(pwd)/cephadm"
if [ -n "$1" ]; then
    target_fpath="$1"
fi
builddir=$(mktemp -d)
if [ -e "requirements.txt" ]; then
    python3 -m pip install -r requirements.txt --target ${builddir}
fi
# Make sure all newly created source files are added here as well!
cp cephadm.py ${builddir}/__main__.py
python3 -m zipapp -p python3 ${builddir} --compress --output $target_fpath
echo written to ${target_fpath}
rm -rf ${builddir}
