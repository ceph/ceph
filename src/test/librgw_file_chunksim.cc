// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc.s
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <cstdint>
#include <cstdlib>
#include <stdint.h>
#include <tuple>
#include <iostream>
#include <fstream>
#include <stack>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"
#include "rgw/rgw_file_int.h"
#include "rgw/rgw_lib_frontend.h" // direct requests

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace {

  using namespace rgw;
  using std::get;
  using std::string;

  librgw_t rgw_h = nullptr;
  string userid("testuser");
  string access_key("");
  string secret_key("");
  struct rgw_fs *fs = nullptr;
  CephContext* cct = nullptr;

  uint32_t owner_uid = 867;
  uint32_t owner_gid = 5309;

  uint16_t num_rele = 1;
  uint32_t num_scans = 2;

  uint32_t create_mask = RGW_SETATTR_UID | RGW_SETATTR_GID | RGW_SETATTR_MODE;

  string bucket_name("dchunks");

  class obj_rec
  {
  public:
    string name;
    struct rgw_file_handle* fh;
    struct rgw_file_handle* parent_fh;
    RGWFileHandle* rgw_fh; // alias into fh

    obj_rec(string _name, struct rgw_file_handle* _fh,
	    struct rgw_file_handle* _parent_fh, RGWFileHandle* _rgw_fh)
      : name(std::move(_name)), fh(_fh), parent_fh(_parent_fh),
	rgw_fh(_rgw_fh) {}

    void clear() {
      fh = nullptr;
      rgw_fh = nullptr;
    }

    friend ostream& operator<<(ostream& os, const obj_rec& rec);
  };

#if 0
  ostream& operator<<(ostream& os, const obj_rec& rec)
  {
    RGWFileHandle* rgw_fh = rec.rgw_fh;
    if (rgw_fh) {
      const char* type = rgw_fh->is_dir() ? "DIR " : "FILE ";
      os << rec.rgw_fh->full_object_name()
	 << " (" << rec.rgw_fh->object_name() << "): "
	 << type;
    }
    return os;
  }
#endif

  class DirentChunk {
  public:
    static constexpr uint32_t max_entries = 200;
    std::vector<obj_rec> dirents;
    uint32_t num_entries{0};

    bool full() {
      return (num_entries >= max_entries);
    }

    bool add(obj_rec& obj) {
      if (full()) {
	return false;
      }
      dirents.emplace_back(std::move(obj));
      ++num_entries;
      return true;
    }

    void clear(bool evict=false) {
      // release all entries and set num_entries := 0
      [[maybe_unused]] auto ret{0};
      for (auto &obj : dirents) {
        // XXX obj refcnt is sentinel+1 before returning 1 ref */
        auto refcnt = obj.rgw_fh->get_refcnt();
        /* XXX the following assertion permits entry refcnt to be
         * 2 /or/ 3 for the following reason:
         * if the number of directory entries is >= to the total
         * capacity of the chunk cache, the uniform fill order is
         * sufficient to guarantee that no handle instance from a
         * prior scan remains in the cache when it is re-added by
         * the next scan.  this would permit a strict assertion that
         * refcnt==2 at this location.  however, if the test were
         * changed to allow the number of entries to vary to smaller
         * counts, then assert(refcnt==2) would fail (because refcnt
         * could rise to 3), probably causing concern.  It is in fact
         * provable that given current workflow, refcnt can never
         * exceed 3 at this location, for all values of num_scans
	 * irrespective of entry count */
        if (refcnt != 2) {
          auto ref_str = to_string(refcnt);
          std::cout << "trapped on refcnt=" << ref_str << std::endl;
          ceph_assert((refcnt == 2) || (refcnt == 3));
	}
        if (unlikely(evict)) {
	  static_cast<RGWLibFS*>(fs->fs_private)->release_evict(obj.rgw_fh);
	} else {
          ret = rgw_fh_rele(fs, obj.fh, 0);
          if (unlikely(num_rele > 1)) {
	    for (uint16_t ix = 1; ix < num_rele; ++num_rele) {
              ret = rgw_fh_rele(fs, obj.fh, 0);
	    }
	  }
	}
      }
      dirents.clear();
      num_entries = 0;
    }

  }; /* DirentChunk */

  /* a simplification of the ganesha mdcache chunk cache.  here,
   * the only supported operation is a single, continued readdir()
   * on one directory.  to consume the next set of incoming entries,
   * the readdir callback must get a fill chunk--which is one of
   * (1) a new chunk (if there is no active/being-filled chunk)
   * (2) a chunk currently being filled;  if the current chunk has
   * filled, the cache will either allocate a new chunk (iff
   * active_chunks.size() < max_chunks), or else recycle the least
   * recently allocated existing chunk. */

  class ChunkCache {
  public:
    using chunk_list_t = std::deque<DirentChunk *>;
    DirentChunk* curr_chunk{nullptr};
    static constexpr uint32_t max_chunks = 25; // XXX param
    chunk_list_t active_chunks;
    uint32_t recycle_count;

    DirentChunk* reclaim_chunk() {
      auto chunk = active_chunks.back();
      active_chunks.pop_back();
      chunk->clear();
      recycle_count++;
      return chunk;
    }

    DirentChunk* get_fill_chunk() {
      if (! curr_chunk) {
        curr_chunk = new DirentChunk();
	active_chunks.push_front(curr_chunk);
	goto out;
      }
      if (curr_chunk->full()) {
        if (active_chunks.size() >= max_chunks) {
          // reclaim chunk
	  curr_chunk = reclaim_chunk();
        } else {
	  // we can allocate a new chunk
	  curr_chunk = new DirentChunk();
	}
	active_chunks.push_front(curr_chunk);
      }
    out:
      return curr_chunk;
    } /* get_fill_chunk */

    uint32_t drain() {
      uint32_t drain_cnt{0};
      for (auto &chunk : active_chunks) {
        chunk->clear(true /* evict */);
        delete (chunk);
	drain_cnt++;
      }
      active_chunks.clear();
      return drain_cnt;
    }
  }; /* Chunkcache */

  ChunkCache dirent_cache;

  bool do_create = false;
  bool verbose = false;

  string marker_dir("nfs_marker");
  struct rgw_file_handle *bucket_fh = nullptr;
  struct rgw_file_handle *marker_fh;
  uint32_t chunkdir_nobjs = 200000;

  using dirent_t = std::tuple<std::string, uint64_t>;
  struct dirent_vec
  {
    std::vector<dirent_t> obj_names;
    uint32_t count;
    dirent_vec() : count(0) {}
  };

  struct {
    int argc;
    char **argv;
  } saved_args;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw_h, saved_args.argc, saved_args.argv);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw_h, nullptr);
}

TEST(LibRGW, MOUNT) {
  int ret = rgw_mount2(rgw_h, userid.c_str(), access_key.c_str(),
                       secret_key.c_str(), "/", &fs, RGW_MOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(fs, nullptr);

  cct = static_cast<RGWLibFS*>(fs->fs_private)->get_context();
}

TEST(LibRGW, CHUNK_SETUP_BUCKET) {
  struct stat st;
  int ret;

  st.st_uid = owner_uid;
  st.st_gid = owner_gid;
  st.st_mode = 755;

  (void) rgw_lookup(fs, fs->root_fh, bucket_name.c_str(), &bucket_fh,
		    nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  if (! bucket_fh) {
    if (do_create) {
      struct stat st;

      st.st_uid = owner_uid;
      st.st_gid = owner_gid;
      st.st_mode = 755;

      ret = rgw_mkdir(fs, fs->root_fh, bucket_name.c_str(), &st, create_mask,
		      &bucket_fh, RGW_MKDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
  }

  ASSERT_NE(bucket_fh, nullptr);

  (void) rgw_lookup(fs, bucket_fh, marker_dir.c_str(), &marker_fh,
		    nullptr, 0, RGW_LOOKUP_FLAG_NONE);
  if (! marker_fh) {
    if (do_create) {
      ret = rgw_mkdir(fs, bucket_fh, marker_dir.c_str(), &st, create_mask,
		      &marker_fh, RGW_MKDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
    }
  }

  ASSERT_NE(marker_fh, nullptr);
} /* setup bucket */

TEST(LibRGW, CHUNK_SETUP_OBJECTS)
{
  /* "large" directory enumeration test.  this one deals only with
   * file objects */

  if (do_create) {
    int ret;

    for (uint32_t ix = 0; ix < chunkdir_nobjs; ++ix) {
      std::string object_name("f_");
      object_name += to_string(ix);
      obj_rec obj{object_name, nullptr, marker_fh, nullptr};
      // lookup object--all operations are by handle
      ret = rgw_lookup(fs, marker_fh, obj.name.c_str(), &obj.fh,
		       nullptr, 0, RGW_LOOKUP_FLAG_CREATE);
      ASSERT_EQ(ret, 0);
      obj.rgw_fh = get_rgwfh(obj.fh);
      // open object--open transaction
      ret = rgw_open(fs, obj.fh, 0 /* posix flags */, RGW_OPEN_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_TRUE(obj.rgw_fh->is_open());

      // unstable write data
      size_t nbytes;
      string data("data for ");
      data += object_name;
      int ret = rgw_write(fs, obj.fh, 0, data.length(), &nbytes,
			  (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, data.length());

      // commit transaction (write on close)
      ret = rgw_close(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);
      if (verbose) {
	/* XXX std:cout fragged...did it get /0 in the stream
	 * somewhere? */
	printf("created: %s:%s\n", bucket_name.c_str(), obj.name.c_str());
      }
    }
  }
} /* setup objects */

TEST(LibRGW, CHUNK_CACHE) {
  ChunkCache cache;
  auto chunk = cache.get_fill_chunk();
  obj_rec obj{"dummy0", nullptr, nullptr, nullptr};
  chunk->add(obj);
}

struct ReaddirArg {
  int32_t total_entries{0};
  std::string next_marker{""};
};

extern "C" {
  static int r2_cb(const char* name, void *arg, uint64_t offset,
		    struct stat* st, uint32_t st_mask,
		    uint32_t flags) {

    ReaddirArg& acc = *(static_cast<ReaddirArg*>(arg));

    string name_str{name};
    if (!((name_str == ".") || (name_str == ".."))) {

      /* lookup (takes ref) on the next dirent */
      obj_rec obj{name_str, nullptr, marker_fh, nullptr};
      // lookup object--all operations are by handle
      int ret = rgw_lookup(fs, marker_fh, obj.name.c_str(), &obj.fh,
			   nullptr, 0, RGW_LOOKUP_FLAG_RCB);
      ceph_assert(ret == 0);
      obj.rgw_fh = get_rgwfh(obj.fh);

      auto chunk = dirent_cache.get_fill_chunk();
      chunk->add(obj);

      acc.next_marker = name_str;
      ++(acc.total_entries);
    }

    printf("%s bucket=%s dir=%s iv count=%d called back name=%s flags=%d\n",
	   __func__,
	   bucket_name.c_str(),
	   marker_dir.c_str(),
	   acc.total_entries,
	   name,
	   flags);

    return true; /* XXX */
  }
}

TEST(LibRGW, CHUNKED_READDIR)
{
  using std::get;

  uint32_t grand_total_entries{0};
  uint32_t grand_total_readdir_cnt{0};
  bool eof = false;

  for (uint32_t scan_ix = 0; scan_ix < num_scans; ++scan_ix) {
    uint32_t readdir_count{0};
    std::string marker{""}; // starting offset==0
    ReaddirArg arg;
    do {
      int ret = rgw_readdir2(fs, marker_fh,
                             (marker.length() > 0) ? marker.c_str() : nullptr,
                             r2_cb, &arg, &eof, RGW_READDIR_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      marker = arg.next_marker;
      std::cout << "new marker: " << marker << std::endl;
      ++readdir_count;
    } while ((!eof));

    grand_total_entries += arg.total_entries;
    grand_total_readdir_cnt += readdir_count;

    std::cout << " entries returned: "
              << arg.total_entries
	      << " readdir invocations: " << readdir_count
	      << " for scan #" << scan_ix
	      << std::endl;
  } /* scan_ix */

  auto drain_count = dirent_cache.drain();

  // print totals
  std::cout << " total entries returned: " << grand_total_entries
            << " total readdir invocations: " << grand_total_readdir_cnt
            << " reclaimed chunks: " << dirent_cache.recycle_count
            << " drained chunks: " << drain_count << std::endl;
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--userid",
				     (char*) nullptr)) {
      userid = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--bn",
				     (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--uid",
				     (char*) nullptr)) {
      owner_uid = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--gid",
				     (char*) nullptr)) {
      owner_gid = std::stoi(val);
    } else if (ceph_argparse_flag(args, arg_iter, "--create",
					    (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--num_scans",
				     (char*) nullptr)) {
      num_scans = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--num_objs",
				     (char*) nullptr)) {
      chunkdir_nobjs = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--num_rele",
				     (char*) nullptr)) {
      num_rele = std::stoi(val);
    } else if (ceph_argparse_flag(args, arg_iter, "--verbose",
					    (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  /* don't accidentally run as anonymous */
  if ((access_key == "") ||
      (secret_key == "")) {
    std::cout << argv[0] << " no AWS credentials, exiting" << std::endl;
    return EPERM;
  }

  saved_args.argc = argc;
  saved_args.argv = argv;

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
