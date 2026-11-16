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

/*
 * What an rgw_fh_hk is good for, and for how long.
 *
 * FSAL_RGW hands a client the 128 bits of rgw_fh_hk as its NFS filehandle
 * and resolves them back through rgw_lookup_handle() (FSAL_RGW
 * export.c:create_handle()), returning ESTALE when that fails.  Since
 * RGWLibFS::lookup_handle() is a cache lookup and the key is a one-way
 * hash of the object's path, a handle can only be resolved while the
 * RGWFileHandle it names is still cached -- there is nothing to
 * reconstruct it from.
 *
 * These tests pin that down, because it is easy to assume otherwise:  a
 * filehandle is supposed to be durable, and this one is not.  Each
 * negative is paired with a positive that would fail if the resolution
 * path were simply broken.
 */

#include <stdint.h>
#include <tuple>
#include <iostream>
#include <string>
#include <vector>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file_int.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

  using namespace rgw;

  string uid("testuser");
  string access_key("");
  string secret_key("");

  librgw_t rgw_h = nullptr;
  struct rgw_fs *fs = nullptr;

  struct rgw_file_handle* bucket_fh = nullptr;
  struct rgw_file_handle* object_fh = nullptr;

  string bucket_name{"fhcache"};
  string object_name{"pinned"};

  /* the bits a client would hold as its NFS filehandle */
  struct rgw_fh_hk saved_hk{};

  bool do_create = false;
  bool evicted = false;

  struct {
    int argc;
    char **argv;
  } saved_args;

} /* namespace */

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw_h, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw_h, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount(rgw_h, uid.c_str(), access_key.c_str(),
		      secret_key.c_str(), &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);
}

TEST(LibRGW, SETUP) {
  struct stat st;
  st.st_uid = 867; st.st_gid = 5309; st.st_mode = 755;

  int ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  if (ret != 0) {
    ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st,
		    RGW_SETATTR_UID|RGW_SETATTR_GID|RGW_SETATTR_MODE,
		    &bucket_fh, RGW_MKDIR_FLAG_NONE);
    do_create = true;
  }
  ASSERT_EQ(ret, 0);
  ASSERT_NE(bucket_fh, nullptr);

  ret = rgw_lookup(fs, bucket_fh, object_name.c_str(), &object_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(object_fh, nullptr);

  /* what FSAL_RGW would put on the wire */
  saved_hk = object_fh->fh_hk;
  ASSERT_TRUE(saved_hk.bucket != 0 || saved_hk.object != 0);
}

/* Control.  While the handle is referenced and cached, the bits resolve.
 * Everything below asserts a *failure*, which proves nothing unless this
 * passes -- a broken resolution path would fail all of them. */
TEST(LibRGW, RESOLVE_WHILE_CACHED) {
  struct rgw_file_handle* fh = nullptr;
  int ret = rgw_lookup_handle(fs, &saved_hk, &fh, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0) << "a live handle did not resolve;  the rest of this "
		       "suite would pass for the wrong reason";
  ASSERT_NE(fh, nullptr);
  ASSERT_EQ(fh->fh_hk.bucket, saved_hk.bucket);
  ASSERT_EQ(fh->fh_hk.object, saved_hk.object);
  ASSERT_EQ(rgw_fh_rele(fs, fh, 0), 0);
}

/* Releasing a reference is not removal from the mapping.  The two
 * structures have separate jobs:  fh_cache is the key-to-handle mapping (a
 * partitioned hash with a backing b-tree), and fh_lru governs lifetime and
 * reclaim.  rgw_fh_rele() unrefs in the LRU;  the mapping entry goes away
 * on reclaim, which is why release_evict() has to do both.
 *
 * So the bits still resolve here, and asserting it keeps the next test
 * honest -- an eviction test would pass for the wrong reason if rele had
 * already dropped the mapping. */
TEST(LibRGW, RESOLVE_AFTER_RELE) {
  ASSERT_NE(object_fh, nullptr);
  ASSERT_EQ(rgw_fh_rele(fs, object_fh, 0), 0);
  object_fh = nullptr;

  struct rgw_file_handle* fh = nullptr;
  int ret = rgw_lookup_handle(fs, &saved_hk, &fh, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0) << "releasing a reference should not make the handle "
		       "unresolvable";
  ASSERT_EQ(rgw_fh_rele(fs, fh, 0), 0);
}

/* An evicted handle's bits name nothing.  A client holding them gets
 * ESTALE from FSAL_RGW without the object having changed at all.
 *
 * RGWLibFS::release_evict() is the mechanism -- it drops the entry from
 * fh_cache and releases the sentinel ref -- and is what
 * librgw_file_chunksim.cc and librgw_file_nfsns.cc already use to force
 * this.  Filling the LRU to provoke a natural eviction would work too but
 * is neither deterministic nor cheap, and it depends on the cache sizing
 * the run happens to have. */
TEST(LibRGW, STALE_AFTER_EVICTION) {
  /* re-resolve to get a referenced handle to evict */
  struct rgw_file_handle* fh = nullptr;
  ASSERT_EQ(rgw_lookup_handle(fs, &saved_hk, &fh, RGW_LOOKUP_FLAG_NONE), 0);
  ASSERT_NE(fh, nullptr);

  auto* rgw_fh = get_rgwfh(fh);
  ASSERT_NE(rgw_fh, nullptr);
  static_cast<RGWLibFS*>(fs->fs_private)->release_evict(rgw_fh);
  evicted = true;

  struct rgw_file_handle* gone = nullptr;
  int ret = rgw_lookup_handle(fs, &saved_hk, &gone, RGW_LOOKUP_FLAG_NONE);
  ASSERT_NE(ret, 0)
      << "handle resolved after eviction;  if this ever passes, librgw has "
	 "acquired a way to reconstruct a handle and HANDLE_IDENTITY.md "
	 "needs revisiting";
}

/* And nothing survives a remount.  This is the case that matters
 * operationally:  a client's filehandle does not outlive the NFS server
 * that issued it, because there is no persistent handle table and the key
 * is a one-way hash.  open_by_handle_at() would resolve here;  this
 * cannot. */
TEST(LibRGW, STALE_AFTER_REMOUNT) {
  ASSERT_NE(fs, nullptr);
  if (bucket_fh) {
    rgw_fh_rele(fs, bucket_fh, 0);
    bucket_fh = nullptr;
  }
  ASSERT_EQ(rgw_umount(fs, RGW_UMOUNT_FLAG_NONE), 0);
  fs = nullptr;

  int ret = rgw_mount(rgw_h, uid.c_str(), access_key.c_str(),
		      secret_key.c_str(), &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);

  /* the object is still there -- prove it, so the failure below cannot be
   * blamed on the object having gone away */
  ret = rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		   nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  struct rgw_file_handle* fh = nullptr;
  ret = rgw_lookup_handle(fs, &saved_hk, &fh, RGW_LOOKUP_FLAG_NONE);
  ASSERT_NE(ret, 0)
      << "a filehandle resolved across a remount;  librgw has gained "
	 "handle persistence and HANDLE_IDENTITY.md needs revisiting";
}

TEST(LibRGW, CLEANUP) {
  if (object_fh) {
    rgw_fh_rele(fs, object_fh, 0);
    object_fh = nullptr;
  }
  if (bucket_fh) {
    rgw_fh_rele(fs, bucket_fh, 0);
    bucket_fh = nullptr;
  }
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;
  ASSERT_EQ(rgw_umount(fs, RGW_UMOUNT_FLAG_NONE), 0);
  fs = nullptr;
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw_h);
}

int main(int argc, char *argv[])
{
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  char* v = getenv("AWS_ACCESS_KEY_ID");
  if (v) {
    access_key = v;
  }
  v = getenv("AWS_SECRET_ACCESS_KEY");
  if (v) {
    secret_key = v;
  }

  string val;
  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_witharg(args, arg_iter, &val, "--access",
			      (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--secret",
				     (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      uid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
				  (char*) nullptr)) {
      do_create = true;
    } else {
      ++arg_iter;
    }
  }

  if ((access_key == "") || (secret_key == "")) {
    std::cout << argv[0] << " no AWS credentials, exiting" << std::endl;
    return EPERM;
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
