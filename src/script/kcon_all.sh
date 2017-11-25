#!/bin/sh -x

p() {
 echo "$*" > /sys/kernel/debug/dynamic_debug/control
}

echo 9 > /proc/sysrq-trigger
p 'module ceph +p'
p 'module libceph +p'
p 'module rbd +p'

## Logging for 5 min
dmesg -w > $(hostname)_$(date +%Y%m%d).log
sleep 300

## disable debug
echo 0 > /proc/sysrq-trigger
p 'module ceph -p'
p 'module libceph -p'
p 'module rbd -p'
