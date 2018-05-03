#!/bin/bash
env=$1
package_file=${@:2}
SCRIPT=$(readlink -f "$0")
script_dir=$(dirname "$SCRIPT")
if [ $env == "pro" ]
then
  RSYNC_PASSWORD=h83SS!el rsync -av ${package_file} rsync://didi_deph@uploadrpm.sys.xiaojukeji.com:8099/didi_deph/7/x86_64
else
  RSYNC_PASSWORD=3InBuCIa rsync -av ${package_file} rsync://didi_deph_test@uploadrpm.sys.xiaojukeji.com:8099/didi_deph_test/7/x86_64
fi
