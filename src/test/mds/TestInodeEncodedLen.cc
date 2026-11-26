// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/ceph_features.h"
#include "include/cephfs/types.h"
#include "mds/snap.h"
#include "gtest/gtest.h"

using inode = inode_t<std::allocator>;

// the MDS computes dentry value lengths without encoding inodes (see
// CDir::dentry_value_length()), from an empty inode's encoded length plus
// get_variable_encoded_len(). this checks that sum against encode()
static void check(const inode &in)
{
  for (uint64_t features : {CEPH_FEATURES_ALL,
                            CEPH_FEATURES_ALL & ~CEPH_FEATURE_FS_FILE_LAYOUT_V2}) {
    bufferlist empty, bl;
    encode(inode(), empty, features);
    encode(in, bl, features);
    EXPECT_EQ(bl.length(), empty.length() + in.get_variable_encoded_len(features))
      << "features " << std::hex << features;
  }
}

TEST(InodeEncodedLen, Empty)
{
  check(inode());
}

TEST(InodeEncodedLen, FixedSizeFields)
{
  inode in;
  in.ino = 0x10000000001;
  in.mode = 0100644;
  in.size = 1 << 20;
  in.version = 42;
  in.rstat.rbytes = 1 << 20;
  in.dirstat.nfiles = 3;
  in.quota.max_bytes = 1 << 30;
  in.export_pin = 1;
  check(in);
}

TEST(InodeEncodedLen, VariableSizeFields)
{
  inode in;
  in.layout.pool_ns = "namespace";
  in.old_pools.insert(1);
  in.old_pools.insert(2);
  in.client_ranges[client_t(4100)].range.last = 1 << 22;
  in.client_ranges[client_t(4101)].follows = 3;
  bufferlist data;
  data.append("inline file data");
  in.inline_data.set_data(data);
  in.stray_prior_path = "/dir/file";
  in.fscrypt_auth = {1, 2, 3};
  in.fscrypt_file = {4, 5};
  in.fscrypt_last_block = {6};
  auto& charmap = in.set_charmap();
  charmap.set_normalization("nfkd");
  charmap.set_encoding("utf8");
  check(in);
}

// the same for the snaprealm, from an empty sr_t's encoded length
static void check(const sr_t &sr)
{
  bufferlist empty, bl;
  encode(sr_t(), empty);
  encode(sr, bl);
  EXPECT_EQ(bl.length(), empty.length() + sr.get_variable_encoded_len());
}

TEST(SrnodeEncodedLen, Empty)
{
  check(sr_t());
}

TEST(SrnodeEncodedLen, FixedSizeFields)
{
  sr_t sr;
  sr.seq = 9;
  sr.created = 2;
  sr.last_created = 8;
  sr.last_destroyed = 7;
  sr.current_parent_since = 3;
  sr.mark_parent_global();
  sr.change_attr = 5;
  check(sr);
}

TEST(SrnodeEncodedLen, VariableSizeFields)
{
  sr_t sr;
  for (snapid_t s : {2, 5}) {
    SnapInfo& info = sr.snaps[s];
    info.snapid = s;
    info.ino = 0x10000000000;
    info.name = "snap" + std::to_string(s.val);
    if (s == 5) {
      info.alternate_name = "Snap5";
      info.metadata["key"] = "value";
      info.metadata["k2"] = "";
    }
  }
  sr.past_parents[snapid_t(4)].ino = 0x1;
  sr.past_parents[snapid_t(4)].first = 2;
  sr.past_parent_snaps = {2, 3, 4};
  check(sr);
}
