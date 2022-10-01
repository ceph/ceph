#!/bin/sh -x

p() {
 echo "$*" > /sys/kernel/debug/dynamic_debug/control
}

echo 9 > /proc/sysrq-trigger
p 'module ceph +p'
p 'module libceph +p'
p 'module rbd +p'
p 'file net/ceph/messenger.c -p'
p 'file' `grep -- --- /sys/kernel/debug/dynamic_debug/control | grep ceph | awk '{print $1}' | sed 's/:/ line /'` '+p'
p 'file' `grep -- === /sys/kernel/debug/dynamic_debug/control | grep ceph | awk '{print $1}' | sed 's/:/ line /'` '+p'
