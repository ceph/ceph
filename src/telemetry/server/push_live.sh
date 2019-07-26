#!/bin/bash -ex

TMP=/tmp/telemetry-push

rm -rf $TMP
mkdir $TMP
git archive HEAD src/telemetry | tar -x -C $TMP
sudo cp -a $TMP/src/telemetry/server/. /opt/telemetry/server
