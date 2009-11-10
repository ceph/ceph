/*
 * Copyright (C) 2007 Oracle.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License v2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 021110-1307, USA.
 */

#ifndef __IOCTL_
#define __IOCTL_
#include <linux/ioctl.h>

#define BTRFS_IOCTL_MAGIC 0x94
#define BTRFS_VOL_NAME_MAX 255
#define BTRFS_PATH_NAME_MAX 4087

/* this should be 4k */
struct btrfs_ioctl_vol_args {
	__s64 fd;
	char name[BTRFS_PATH_NAME_MAX + 1];
};

struct btrfs_ioctl_clone_range_args {
  __s64 src_fd;
  __u64 src_offset, src_length;
  __u64 dest_offset;
};

#define BTRFS_IOC_SNAP_CREATE _IOW(BTRFS_IOCTL_MAGIC, 1, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_DEFRAG _IOW(BTRFS_IOCTL_MAGIC, 2, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_RESIZE _IOW(BTRFS_IOCTL_MAGIC, 3, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_SCAN_DEV _IOW(BTRFS_IOCTL_MAGIC, 4, \
				   struct btrfs_ioctl_vol_args)
/* trans start and trans end are dangerous, and only for
 * use by applications that know how to avoid the
 * resulting deadlocks
 */
#define BTRFS_IOC_TRANS_START  _IO(BTRFS_IOCTL_MAGIC, 6)
#define BTRFS_IOC_TRANS_END    _IO(BTRFS_IOCTL_MAGIC, 7)
#define BTRFS_IOC_SYNC         _IO(BTRFS_IOCTL_MAGIC, 8)

#define BTRFS_IOC_CLONE        _IOW(BTRFS_IOCTL_MAGIC, 9, int)
#define BTRFS_IOC_ADD_DEV _IOW(BTRFS_IOCTL_MAGIC, 10, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_RM_DEV _IOW(BTRFS_IOCTL_MAGIC, 11, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_BALANCE _IOW(BTRFS_IOCTL_MAGIC, 12, \
				   struct btrfs_ioctl_vol_args)

#define BTRFS_IOC_CLONE_RANGE _IOW(BTRFS_IOCTL_MAGIC, 13, \
				  struct btrfs_ioctl_clone_range_args)

#define BTRFS_IOC_SUBVOL_CREATE _IOW(BTRFS_IOCTL_MAGIC, 14, \
				   struct btrfs_ioctl_vol_args)
#define BTRFS_IOC_SNAP_DESTROY _IOW(BTRFS_IOCTL_MAGIC, 15, \
				struct btrfs_ioctl_vol_args)

/* usertrans ops */
#define BTRFS_IOC_UT_OP_OPEN         1
#define BTRFS_IOC_UT_OP_CLOSE        2
#define BTRFS_IOC_UT_OP_PWRITE       3
#define BTRFS_IOC_UT_OP_UNLINK       4
#define BTRFS_IOC_UT_OP_LINK         5
#define BTRFS_IOC_UT_OP_MKDIR        6
#define BTRFS_IOC_UT_OP_RMDIR        7
#define BTRFS_IOC_UT_OP_TRUNCATE     8
#define BTRFS_IOC_UT_OP_SETXATTR     9
#define BTRFS_IOC_UT_OP_REMOVEXATTR 10
#define BTRFS_IOC_UT_OP_CLONERANGE  11

#define BTRFS_IOC_UT_OP_FLAG_FD_SAVE       (1<< 1)
#define BTRFS_IOC_UT_OP_FLAG_FD_ARG0       (1<< 2)
#define BTRFS_IOC_UT_OP_FLAG_FD_ARG1       (1<< 3)
#define BTRFS_IOC_UT_OP_FLAG_FD_ARG2       (1<< 4)
#define BTRFS_IOC_UT_OP_FLAG_FD_ARG3       (1<< 5)
#define BTRFS_IOC_UT_OP_FLAG_FD_ARG4       (1<< 6)
#define BTRFS_IOC_UT_OP_FLAG_FAIL_ON_NE    (1<< 7)
#define BTRFS_IOC_UT_OP_FLAG_FAIL_ON_EQ    (1<< 8)
#define BTRFS_IOC_UT_OP_FLAG_FAIL_ON_LT    (1<< 9)
#define BTRFS_IOC_UT_OP_FLAG_FAIL_ON_GT    (1<<10)
#define BTRFS_IOC_UT_OP_FLAG_FAIL_ON_LTE   (1<<11)
#define BTRFS_IOC_UT_OP_FLAG_FAIL_ON_GTE   (1<<12)

struct btrfs_ioctl_usertrans_op {
	__u64 op;
	__s64 args[5];
	__s64 rval;
	__u64 flags;
	__u64 fd_num;
};

/*
 * If an op fails and we cannot complete the transaction, we may want
 * to lock up the file system (requiring a reboot) to prevent a
 * partial result from committing.
 */
#define BTRFS_IOC_UT_FLAG_WEDGEONFAIL (1<<13)

struct btrfs_ioctl_usertrans {
	__u64 num_ops;                  /* in: # ops */
	__u64 ops_ptr;                  /* in: usertrans_op array */
	__u64 num_fds;	                /* in: max fds we use */
	__u64 data_bytes, metadata_ops; /* in: for space reservation */
	__u64 flags;                    /* in: flags */
	__u64 ops_completed;            /* out: # ops completed */
};

#define BTRFS_IOC_USERTRANS  _IOW(BTRFS_IOCTL_MAGIC, 16,	\
				  struct btrfs_ioctl_usertrans)

#endif
