#!/bin/bash

command="/usr/lib/ltp/testcases/bin/fsstress -d fsstress-`hostname`$$ -l 1 -n 1000 -p 10"

echo "Starting fsstress $command"
mkdir fsstress`hostname`-$$
$command
