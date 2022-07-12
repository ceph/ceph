#!/bin/bash

# Not entirely sure why these executables don't seem to be available in the
# $PATH when running from tox. Calling out to `which` seems to fix it, at the
# expense of making the script a bit obtuse

mktemp=$(which mktemp)
cat=$(which cat)
grep=$(which grep)
PYTHON_EXECUTABLE=`which python3`
STDERR_FILE=$($mktemp)
INVALID="→"

echo "stderr file created: $STDERR_FILE"

INVALID="$INVALID" $PYTHON_EXECUTABLE $1 2> ${STDERR_FILE}

retVal=$?

if [ $retVal -ne 0 ]; then
    echo "Failed test: Unexpected failure from running Python script"
    echo "Below is output of stderr captured:"
    $cat "${STDERR_FILE}"
    exit $retVal
fi

$grep --quiet "$INVALID" ${STDERR_FILE}

retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Failed test: expected to find \"${INVALID}\" character in tmpfile: \"${STDERR_FILE}\""
    echo "Below is output of stderr captured:"
    $cat "${STDERR_FILE}"
fi
exit $retVal
