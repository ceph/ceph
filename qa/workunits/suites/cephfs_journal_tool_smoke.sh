#!/usr/bin/env bash

set -ex

export BIN="${BIN:-cephfs-journal-tool --rank=cephfs:0}"
export JOURNAL_FILE=/tmp/journal.bin
export JSON_OUTPUT=/tmp/json.tmp
export BINARY_OUTPUT=/tmp/binary.tmp

if [ -d $BINARY_OUTPUT ] ; then
    rm -rf $BINARY_OUTPUT
fi

# Check that the import/export stuff really works as expected
# first because it's used as the reset method between
# following checks.
echo "Testing that export/import cycle preserves state"
HEADER_STATE=`$BIN header get`
EVENT_LIST=`$BIN event get list`
$BIN journal export $JOURNAL_FILE
$BIN journal import $JOURNAL_FILE
NEW_HEADER_STATE=`$BIN header get`
NEW_EVENT_LIST=`$BIN event get list`

if [ ! "$HEADER_STATE" = "$NEW_HEADER_STATE" ] ; then
    echo "Import failed to preserve header state"
    echo $HEADER_STATE
    echo $NEW_HEADER_STATE
    exit -1
fi

if [ ! "$EVENT_LIST" = "$NEW_EVENT_LIST" ] ; then
    echo "Import failed to preserve event state"
    echo $EVENT_LIST
    echo $NEW_EVENT_LIST
    exit -1
fi

echo "Testing 'journal' commands..."

# Simplest thing: print the vital statistics of the journal
$BIN journal inspect
$BIN header get

# Make a copy of the journal in its original state
$BIN journal export $JOURNAL_FILE
if [ ! -s $JOURNAL_FILE ] ; then
    echo "Export to $JOURNAL_FILE failed"
    exit -1
fi

# Can we execute a journal reset?
$BIN journal reset --yes-i-really-really-mean-it
$BIN journal inspect
$BIN header get

echo "Rolling back journal to original state..."
$BIN journal import $JOURNAL_FILE

echo "Testing 'header' commands..."
$BIN header get
$BIN header set write_pos 123
$BIN header set expire_pos 123
$BIN header set trimmed_pos 123

echo "Rolling back journal to original state..."
$BIN journal import $JOURNAL_FILE

echo "Testing 'event' commands..."
$BIN event get summary
$BIN event get --type=UPDATE --path=/ --inode=0 --frag=0x100 summary
$BIN event get json --path $JSON_OUTPUT
if [ ! -s $JSON_OUTPUT ] ; then
    echo "Export to $JSON_OUTPUT failed"
    exit -1
fi
$BIN event get binary --path $BINARY_OUTPUT
if [ ! -s $BINARY_OUTPUT ] ; then
    echo "Export to $BINARY_OUTPUT failed"
    exit -1
fi
$BIN event recover_dentries summary
$BIN event splice summary

# Tests finish.
# Metadata objects have been modified by the 'event recover_dentries' command.
# Journal is no long consistent with respect to metadata objects (especially inotable).
# To ensure mds successfully replays its journal, we need to do journal reset.
$BIN journal reset --yes-i-really-really-mean-it
cephfs-table-tool all reset session

