#!/bin/bash

git --no-pager grep -n '127.0.0.1:[0-9]\+' | sed -n 's/.*127.0.0.1:\([0-9]\+\).*/\1/p' | sort -n | uniq -u
