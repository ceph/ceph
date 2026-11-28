// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
#include "include/scope_guard.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iterator>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <vector>

TEST(LibCephFS, ReaddirRCB) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char c_dir[256];
  sprintf(c_dir, "/readdir_r_cb_tests_%d", getpid());
  struct ceph_dir_result *dirp;
  ASSERT_EQ(0, ceph_mkdirs(cmount, c_dir, 0777));
  ASSERT_LE(0, ceph_opendir(cmount, c_dir, &dirp));

  // dir is empty, check that it only contains . and ..
  int buflen = 100;
  char *buf = new char[buflen];
  // . is 2, .. is 3 (for null terminators)
  ASSERT_EQ(5, ceph_getdnames(cmount, dirp, buf, buflen));
  char c_file[256];
  sprintf(c_file, "/readdir_r_cb_tests_%d/foo", getpid());
  int fd = ceph_open(cmount, c_file, O_CREAT, 0777);
  ASSERT_LT(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // check correctness with one entry
  ASSERT_LE(0, ceph_closedir(cmount, dirp));
  ASSERT_LE(0, ceph_opendir(cmount, c_dir, &dirp));
  ASSERT_EQ(9, ceph_getdnames(cmount, dirp, buf, buflen)); // ., .., foo

  // check correctness if buffer is too small
  ASSERT_LE(0, ceph_closedir(cmount, dirp));
  ASSERT_GE(0, ceph_opendir(cmount, c_dir, &dirp));
  ASSERT_EQ(-ERANGE, ceph_getdnames(cmount, dirp, buf, 1));

  //check correctness if it needs to split listing
  ASSERT_LE(0, ceph_closedir(cmount, dirp));
  ASSERT_LE(0, ceph_opendir(cmount, c_dir, &dirp));
  ASSERT_EQ(5, ceph_getdnames(cmount, dirp, buf, 6));
  ASSERT_EQ(4, ceph_getdnames(cmount, dirp, buf, 6));

  // free cmount after finishing testing
  ASSERT_LE(0, ceph_closedir(cmount, dirp));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

TEST(LibCephFS, ConcurrentReaddirPreservesCachedOffsets) {
  struct ceph_mount_info *cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, nullptr));
  auto release_mount = make_scope_guard([&] {
    if (cmount) {
      ceph_release(cmount);
    }
  });
  ASSERT_EQ(0, ceph_conf_read_file(cmount, nullptr));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  auto unmount = make_scope_guard([&] {
    EXPECT_EQ(0, ceph_unmount(cmount));
  });

  const std::string dir_path =
    "/readdir_concurrent_offset_" + std::to_string(getpid());
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0755));

  std::vector<std::string> created;
  auto cleanup = make_scope_guard([&] {
    for (const auto& name : created) {
      ceph_unlink(cmount, (dir_path + "/" + name).c_str());
    }
    ceph_rmdir(cmount, dir_path.c_str());
  });

  auto create_file = [&](const char *name) {
    int r = ceph_mknod(cmount, (dir_path + "/" + name).c_str(),
                       S_IFREG | 0600, 0);
    if (r == 0) {
      created.emplace_back(name);
    }
    return r;
  };

  auto readdir_with_offsets = [&](std::vector<std::pair<std::string, int64_t>>& entries) {
    entries.clear();
    struct ceph_dir_result *dir = nullptr;
    int r = ceph_opendir(cmount, dir_path.c_str(), &dir);
    if (r != 0) {
      return r;
    }
    auto close_dir = make_scope_guard([&] {
      EXPECT_EQ(0, ceph_closedir(cmount, dir));
    });
    struct dirent entry;
    while ((r = ceph_readdir_r(cmount, dir, &entry)) > 0) {
      if (strcmp(entry.d_name, ".") && strcmp(entry.d_name, "..")) {
        entries.emplace_back(entry.d_name, ceph_telldir(cmount, dir));
      }
    }
    return r;
  };

  // col_103420, col_121287, and col_58493 all hash to 0x24ed95 under the
  // default rjenkins dir hash, while zz1 hashes to 0x91ebf7 (so it sorts
  // after them and none of the colliding entries is at EOF).
  ASSERT_EQ(0, create_file("col_121287"));
  ASSERT_EQ(0, create_file("col_58493"));
  ASSERT_EQ(0, create_file("zz1"));

  // Populate a complete, ordered readdir cache: col_121287=(H,2), col_58493=(H,3).
  std::vector<std::pair<std::string, int64_t>> initial;
  ASSERT_EQ(0, readdir_with_offsets(initial));
  ASSERT_EQ(3U, initial.size());
  auto first = std::find_if(initial.begin(), initial.end(), [](const auto& e) {
    return e.first == "col_121287";
  });
  ASSERT_NE(initial.end(), first);
  ASSERT_NE(initial.end(), std::next(first));
  ASSERT_EQ("col_58493", std::next(first)->first);
  ASSERT_EQ(first->second + 1, std::next(first)->second)
    << std::hex << "col_121287 (" << first->second << ") and col_58493 ("
    << std::next(first)->second << ") do not share a hash bucket, this cluster"
       " does not use the rjenkins dir hash";

  // Pause dirp2 after reading col_121287 from readdir_cache. That leaves
  // dirp2 with last_name="col_121287", next_offset=3, and release_count=0.
  struct ceph_dir_result *dirp2 = nullptr;
  ASSERT_EQ(0, ceph_opendir(cmount, dir_path.c_str(), &dirp2));
  auto close_dirp2 = make_scope_guard([&] {
    EXPECT_EQ(0, ceph_closedir(cmount, dirp2));
  });
  struct dirent entry;
  bool saw_mid = false;
  while (ceph_readdir_r(cmount, dirp2, &entry) > 0) {
    if (strcmp(entry.d_name, "col_121287") == 0) {
      saw_mid = true;
      break;
    }
  }
  ASSERT_TRUE(saw_mid);

  // Insert an earlier colliding entry, clearing I_DIR_ORDERED and readdir_cache.
  ASSERT_EQ(0, create_file("col_103420"));

  // Start dirp1 from the beginning and read one real entry: the MDS reply
  // populates readdir_cache with col_103420=(H,2), col_121287=(H,3),
  // col_58493=(H,4), while the directory is not yet marked complete+ordered.
  struct ceph_dir_result *dirp1 = nullptr;
  ASSERT_EQ(0, ceph_opendir(cmount, dir_path.c_str(), &dirp1));
  auto close_dirp1 = make_scope_guard([&] {
    EXPECT_EQ(0, ceph_closedir(cmount, dirp1));
  });
  std::vector<std::pair<std::string, int64_t>> from_dirp1;
  while (from_dirp1.empty()) {
    ASSERT_EQ(1, ceph_readdir_r(cmount, dirp1, &entry));
    if (strcmp(entry.d_name, ".") && strcmp(entry.d_name, "..")) {
      from_dirp1.emplace_back(entry.d_name, ceph_telldir(cmount, dirp1));
    }
  }

  // Resume dirp2: because the directory is not complete+ordered yet, it sends
  // READDIR starting after col_121287 with stale next_offset=3. Its reply
  // skips updating readdir_cache (release_count=0) and must not overwrite
  // col_58493's cached Dentry::offset from (H,4) back to (H,3).
  ASSERT_EQ(1, ceph_readdir_r(cmount, dirp2, &entry));
  EXPECT_STREQ("col_58493", entry.d_name);

  // Finish draining dirp1 to EOF so the directory becomes complete+ordered.
  int r;
  while ((r = ceph_readdir_r(cmount, dirp1, &entry)) > 0) {
    if (strcmp(entry.d_name, ".") && strcmp(entry.d_name, "..")) {
      from_dirp1.emplace_back(entry.d_name, ceph_telldir(cmount, dirp1));
    }
  }
  ASSERT_EQ(0, r);
  ASSERT_EQ(4U, from_dirp1.size());

  // A subsequent cached listing uses lower_bound() on Dentry::offset per call.
  // If dirp2 clobbered col_58493's offset to (H,3), col_58493 is skipped here.
  std::vector<std::pair<std::string, int64_t>> after;
  ASSERT_EQ(0, readdir_with_offsets(after));
  ASSERT_EQ(4U, after.size());
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_EQ(from_dirp1[i], after[i]);
  }
  EXPECT_EQ(from_dirp1[3].first, after[3].first);
  EXPECT_EQ("col_103420", after[0].first);
  EXPECT_EQ("col_121287", after[1].first);
  EXPECT_EQ("col_58493", after[2].first);
  EXPECT_EQ(after[0].second + 1, after[1].second);
  EXPECT_EQ(after[1].second + 1, after[2].second);
}
