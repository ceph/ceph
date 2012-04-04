#!/bin/sh -ex

bin/teuthology-nuke -t $1 -r --owner $2
bin/teuthology-lock --unlock -t $1 --owner $2
