#include <linux/module.h>
#include <linux/fs.h>
#include <linux/smp_lock.h>
#include <linux/slab.h>

#include <linux/ceph_fs.h>

#include "super.h"

MODULE_AUTHOR("Patience Warnick <patience@newdream.net>");
MODULE_DESCRIPTION("Ceph filesystem for Linux");
MODULE_LICENSE("GPL");

