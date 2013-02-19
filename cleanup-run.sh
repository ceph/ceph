#!/bin/sh -ex

owner=`teuthology-lock -a --list --desc-pattern /$1/ --status up | grep locked_by | head -1 | awk '{print $2}' | sed 's/"//g' | sed 's/,//'`
teuthology-lock --list-targets --desc-pattern /$1/ --status up --owner $owner > /tmp/$$
teuthology-nuke --unlock -t /tmp/$$ -r --owner $owner
rm /tmp/$$

