#!/bin/sh -ex

teuthology-lock --list-targets --owner $1 > /tmp/$$
teuthology-nuke --unlock -t /tmp/$$ -r --owner $1
rm /tmp/$$

