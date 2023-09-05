/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// Those definitions allow handling information coming from Ceph and should
// not be passed to Windows functions.

#pragma once

#define S_IFLNK   0120000

#define S_ISTYPE(m, TYPE) ((m & S_IFMT) == TYPE)
#define S_ISLNK(m)  S_ISTYPE(m, S_IFLNK)
#define S_ISUID     04000
#define S_ISGID     02000
#define S_ISVTX     01000

#define LOCK_SH    1
#define LOCK_EX    2
#define LOCK_NB    4
#define LOCK_UN    8
#define LOCK_MAND  32
#define LOCK_READ  64
#define LOCK_WRITE 128
#define LOCK_RW    192

#define AT_SYMLINK_NOFOLLOW 0x100
#define AT_REMOVEDIR        0x200

#define MAXSYMLINKS  65000

#define O_DIRECTORY 0200000
#define O_NOFOLLOW  0400000

#define XATTR_CREATE  1
#define XATTR_REPLACE 2

typedef unsigned int uid_t;
typedef unsigned int gid_t;
