// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "os/filestore/FileStore.h"
#include <gtest/gtest.h>

class TestFileStore {
public:
  static void create_backend(FileStore &fs, long f_type) {
    fs.create_backend(f_type);
  }
};

TEST(FileStore, create)
{
  {
    map<string,string> pm;
    FileStore fs("a", "b");
    TestFileStore::create_backend(fs, 0);
    fs.collect_metadata(&pm);
    ASSERT_EQ(pm["filestore_backend"], "generic");
  }
#if defined(__linux__)
  {
    map<string,string> pm;
    FileStore fs("a", "b");
    TestFileStore::create_backend(fs, BTRFS_SUPER_MAGIC);
    fs.collect_metadata(&pm);
    ASSERT_EQ(pm["filestore_backend"], "btrfs");
  }
# ifdef HAVE_LIBXFS
  {
    map<string,string> pm;
    FileStore fs("a", "b");
    TestFileStore::create_backend(fs, XFS_SUPER_MAGIC);
    fs.collect_metadata(&pm);
    ASSERT_EQ(pm["filestore_backend"], "xfs");
  }
# endif
#endif
#ifdef HAVE_LIBZFS
  {
    map<string,string> pm;
    FileStore fs("a", "b");
    TestFileStore::create_backend(fs, ZFS_SUPER_MAGIC);
    fs.collect_metadata(&pm);
    ASSERT_EQ(pm["filestore_backend"], "zfs");
  }
#endif
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val("osd_journal_size", "100");
  g_ceph_context->_conf->apply_changes(NULL);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_filestore && 
 *    ./ceph_test_filestore \
 *        --gtest_filter=*.* --log-to-stderr=true --debug-filestore=20
 *  "
 * End:
 */
