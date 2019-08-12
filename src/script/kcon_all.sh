#!/bin/sh -x

p() {
 echo "$*" > /sys/kernel/debug/dynamic_debug/control
}

echo 9 > /proc/sysrq-trigger
p 'module ceph +p'
p 'module libceph +p'
p 'module rbd +p'
