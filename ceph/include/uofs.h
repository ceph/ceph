// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

/*
 * uofs.h
 * 
 * user-level object-based file system
 */
 
 #ifndef _UOFS_H_
 #define _UOFS_H_

 #include <sys/types.h>
 #include <unistd.h>
 #include <stdio.h>


 int device_open(char *path, int xflags);
 void device_findsizes(int fd, long long *sz, int *bsz);

 int uofs_format(int bdev_id, int donode_size, int bd_ratio, int reg_size, int sb_size, int lb_size,
	         int nr_hash_table_buckets, int delay_allocation, int flush_interval);

 int uofs_mount(int bdev_id);
 void uofs_shutdown(void);

 int uofs_read(long long oid, void *buf, off_t offset, size_t count);
 int uofs_write(long long oid, void *buf, off_t offset, size_t count);
 int uofs_del(long long oid);
 int uofs_sync(long long oid);
 int uofs_exist(long long oid);

 int uofs_get_size(long long oid);

 void uofs_superblock_printout(void);
 int  get_large_object_pages(void);

 int uofs_buffer_size(void);
 #endif
