// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *

 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPHFS_FSCRYPT_H
#define CEPHFS_FSCRYPT_H

struct ceph_fscrypt_last_block_header {
        __u8  ver;
        __u8  compat;

        /* If the last block is located in a file hole the length
	 * will be sizeof(i_version + file_offset + block_size),
	 * or will plus to extra BLOCK SIZE.
	 */
        uint32_t data_len;

	/* inode change attr version */
        uint64_t change_attr;

	/*
	 * For a file hole, this will be 0, or it will be the offset from
	 * which will write the last block
	 */
        uint64_t file_offset;

	/* It should always be the fscrypt block size */
        uint32_t block_size;
};

#endif
