#!/bin/bash

set -e
set -x

export BIN="${BIN:-cephfs-journal-tool}"
export JOURNAL_FILE=/tmp/journal.bin
export JSON_OUTPUT=/tmp/json.tmp
export BINARY_OUTPUT=/tmp/binary.tmp

if [ -d $BINARY_OUTPUT ] ; then
    rm -rf $BINARY_OUTPUT
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

# Can we import what we exported?
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
$BIN event apply summary
$BIN event splice summary

echo "Rolling back journal to original state..."
$BIN journal import $JOURNAL_FILE

echo "Testing 'header' commands..."

$BIN header get
$BIN header set write_pos 123
$BIN header set expire_pos 123
$BIN header set trimmed_pos 123

echo "Rolling back journal to original state..."
$BIN journal import $JOURNAL_FILE

# XXX doing this *last* because journal reset appends to the journal,
# and import does not truncated the appended part, resulting in
# corruption when trying to do an 'import' after a 'reset'.
# Can we execute a journal reset?
$BIN journal reset
$BIN journal inspect
$BIN header get

