#!/usr/bin/env bash
#find which dir shell script is ran from
DIR=$(dirname -- "$( readlink -f -- "$0"; )")
PYTHON_SCRIPT="fscrypt-snippets.py"

#run it
${DIR}/${PYTHON_SCRIPT}
