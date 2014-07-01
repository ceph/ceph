#!/bin/bash
#
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
code=0
echo "Run unit tests that need a cluster, using vstart.sh"
while read line ; do
  echo "================ START ================"
  echo "$line"
  echo "======================================="
  if ! test/vstart_wrapper.sh $line ; then
      code=1
  fi
  echo "================ STOP ================="  
done <<EOF
../qa/workunits/cephtool/test.sh --asok-does-not-need-root
EOF
exit $code
