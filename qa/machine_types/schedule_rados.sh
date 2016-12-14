#!/bin/bash

# $1 - part
# $2 - branch name
# $3 - machine name

teuthology-suite -v -c $2 -m $3 -k distro -s rados --subset $(echo "(($(date +%U) % 2) * 7) + $1" | bc)/14
