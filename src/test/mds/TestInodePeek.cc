// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "mds/CInode.h"
#include "include/ceph_features.h"
#include "gtest/gtest.h"

// InodeStoreBase::peek_bare() must agree with a full decode_bare() on every
// combination of the fields it skips over.
TEST(InodeStorePeek, AgreesWithDecode)
{
  for (int mask = 0; mask < 64; ++mask) {
    InodeStore st;
    auto in = st.get_inode();
    in->ino = 0x10000000000 + mask;
    in->nlink = 1;
    if (mask & 1)
      in->mode = S_IFDIR | 0755;
    else if (mask & 2)
      in->mode = S_IFLNK | 0777;
    else
      in->mode = S_IFREG | 0644;
    if ((mask & 3) == 2)
      st.symlink = "some/symlink/target";
    if (mask & 4) {
      auto x = InodeStoreBase::allocate_xattr_map();
      (*x)["user.a"] = bufferptr("hello", 5);
      (*x)["user.empty"] = bufferptr();
      st.reset_xattrs(std::move(x));
    }
    if (mask & 8)
      st.dirfragtree.split(frag_t(), 3);
    if (mask & 16) {
      auto o = InodeStoreBase::allocate_old_inode_map();
      auto& oi = (*o)[snapid_t(5)];
      oi.first = 2;
      oi.inode = *in;
      st.reset_old_inodes(std::move(o));
    }
    if (mask & 32)
      encode(std::string("a snaprealm blob"), st.snap_blob);

    bufferlist bl;
    st.encode_bare(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

    bool is_dir = !(mask & 1), has_old_inodes = !(mask & 16);
    auto p = bl.cbegin();
    ASSERT_TRUE(InodeStoreBase::peek_bare(p, &is_dir, &has_old_inodes))
      << "mask " << mask;

    InodeStore full;
    auto q = bl.cbegin();
    full.decode_bare(q);
    EXPECT_EQ(full.inode->is_dir(), is_dir) << "mask " << mask;
    EXPECT_EQ(full.old_inodes && !full.old_inodes->empty(), has_old_inodes)
      << "mask " << mask;
  }
}
