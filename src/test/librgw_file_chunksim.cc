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
#include <unordered_set>
#include <unordered_map>
#include <mutex>

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

  /* abort at the first refcount violation (full backtrace and core, which
   * is the diagnosis for a reference leak), or record every one and fail
   * the test through gtest at the end of the scan */
  bool fatal_refcnt = true;

  /* how the stateless-finalize timer is driven;  see the comment on
   * RGWLibFS::StatelessTimerMode.  the check is exact in any of them--the
   * mode decides which behaviour the run exercises, not whether the
   * arithmetic works.  the default is the production one, so an ordinary
   * run covers what it covered before these modes existed. */
  std::string timer_mode("normal");

  /* Injected leak, for proving the detector can fail:  every Nth dirent
   * gets a second lookup(RCB) reference which nothing ever returns--the
   * shape of the historical CHUNK_SETUP_OBJECTS bug.  0 disables it.
   * A control which cannot produce the failure it screens for is not a
   * control, and this is a runtime knob precisely so that demonstrating
   * it needs no edit to the code under test. */
  uint32_t leak_every = 0;

  /* Injected leak for the drain sweep, which screens a different
   * population:  handles still in the FHCache at teardown.  The
   * per-entry leak above never reaches it, because ChunkCache::drain()
   * release_evict()s every enumerated handle out of the cache first.
   * This takes references after the scan, so the handle is cached and
   * above its sentinel when close() drains.  0 disables it. */
  uint32_t sweep_leak = 0;

  struct RefViolation
  {
    std::string name;
    uint64_t fh_hk;
    uint32_t observed;
    uint32_t expected;
  };
  std::vector<RefViolation> refcnt_violations;

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

    /* Live lookup(RCB) references, by handle.  A dirent may sit in more
     * than one chunk at once--a scan re-enumerating an entry whose
     * previous chunk has not been reclaimed yet takes a second reference
     * on the same handle--so the number of live references is a property
     * of the cache, not a constant.  Counting it here is what lets the
     * check below be an equality. */
    inline static std::unordered_map<RGWFileHandle*, uint32_t> live_refs;

    static uint32_t live_refs_for(RGWFileHandle* rgw_fh) {
      auto it = live_refs.find(rgw_fh);
      return (it == live_refs.end()) ? 0 : it->second;
    }

    static void drop_live_ref(RGWFileHandle* rgw_fh) {
      auto it = live_refs.find(rgw_fh);
      ceph_assert(it != live_refs.end());
      if (--(it->second) == 0) {
	live_refs.erase(it);
      }
    }

    bool add(obj_rec& obj) {
      if (full()) {
	return false;
      }
      if (obj.rgw_fh) {
	++live_refs[obj.rgw_fh];
      }
      dirents.emplace_back(std::move(obj));
      ++num_entries;
      return true;
    }

    /* The exact expected count, checked wherever an entry's reference is
     * about to be returned.  Both call sites matter:  clear() covers
     * entries a reclaim passed through, and drain() covers the rest--
     * below the cache's capacity nothing is ever reclaimed, so without
     * the drain pass an entire run can go unchecked. */
    static void check_entry(const obj_rec& obj, uint32_t num_entries,
			    size_t dirents_size) {
      /* The exact expected count.  No term here is a tolerance:  each
       * is a reference somebody is known to hold, so a mismatch of one
       * is a leak and is reported as one.
       *
       *   1                     the FHCache sentinel
       *   live_refs_for()       this cache's live lookup(RCB) refs--more
       *                         than one when a dirent sits in two
       *                         chunks at once, which happens whenever a
       *                         scan re-enumerates an entry whose
       *                         previous chunk is not reclaimed yet
       *   stateless_pins_for()  outstanding StatelessFinalize refs
       *
       * The last cannot be read off the handle:  close_global() clears
       * file::stateless_timer_id while the pending event still holds
       * its reference, so after an explicit close the pin is live with
       * nothing to read it from.  RGWLibFS keeps the count at the two
       * points that take and return it instead.  Both reads are taken
       * under stateless_pin_mtx so a timer firing between them cannot
       * make the pair inconsistent. */
      uint32_t refcnt, expected;
      {
        std::lock_guard<std::mutex> pin_guard(RGWLibFS::stateless_pin_mtx);
        refcnt = obj.rgw_fh->get_refcnt();
        expected = 1 /* FHCache sentinel */
          + live_refs_for(obj.rgw_fh)
          + RGWLibFS::stateless_pins_for(obj.rgw_fh);
      }
      if (refcnt != expected) {
        std::cout << "refcnt=" << refcnt << " expected=" << expected
                  << " name=" << obj.name
                  << " fh_hk=" << std::hex
                  << obj.rgw_fh->get_key().fh_hk.object
                  << std::dec
                  << (obj.rgw_fh->is_dir() ? " dir" : " file")
                  << " chunk_entries=" << num_entries
                  << " dirents=" << dirents_size
                  << std::endl;
        refcnt_violations.push_back(
          RefViolation{obj.name, obj.rgw_fh->get_key().fh_hk.object,
                       refcnt, expected});
        /* fail fast by default:  the backtrace and core name whoever
         * holds the extra reference, and an unwound gtest failure does
         * not.  returning from clear() mid-loop would also strand the
         * refs of every entry after this one.  --fatal_refcnt=0 records
         * them all instead and CHUNKED_READDIR reports the list. */
        if (fatal_refcnt) {
          ceph_assert(refcnt == expected);
        }
	}
    }

    void clear(bool evict=false) {
      // release all entries and set num_entries := 0
      [[maybe_unused]] auto ret{0};
      for (auto &obj : dirents) {
        if (! obj.rgw_fh) {
          /* a placeholder with no handle -- CHUNK_CACHE adds one.  Nothing to
           * check and nothing to release;  dereferencing it here is what made
           * that a latent crash rather than a no-op. */
          continue;
        }
        check_entry(obj, num_entries, dirents.size());
        drop_live_ref(obj.rgw_fh);
        if (unlikely(evict)) {
	  static_cast<RGWLibFS*>(fs->fs_private)->release_evict(obj.rgw_fh);
	} else {
          ret = rgw_fh_rele(fs, obj.fh, 0);
          if (unlikely(num_rele > 1)) {
	    /* ++ix, not ++num_rele:  incrementing the bound never reaches it */
	    for (uint16_t ix = 1; ix < num_rele; ++ix) {
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
      std::unordered_set<RGWFileHandle*> evicted;
      /* Check before releasing anything:  release_evict() drops the
       * sentinel, so once the first pass has run the expected count for
       * a handle's remaining occurrences no longer holds.  One report
       * per distinct handle. */
      {
	std::unordered_set<RGWFileHandle*> seen;
	for (auto &chunk : active_chunks) {
	  for (auto &obj : chunk->dirents) {
	    if (obj.rgw_fh && seen.insert(obj.rgw_fh).second) {
	      DirentChunk::check_entry(obj, chunk->num_entries,
				       chunk->dirents.size());
	    }
	  }
	}
      }
      for (auto &chunk : active_chunks) {
	for (auto &obj : chunk->dirents) {
	  if (! obj.rgw_fh) {
	    continue;
	  }
	  if (evicted.insert(obj.rgw_fh).second) {
	    static_cast<RGWLibFS*>(fs->fs_private)->release_evict(obj.rgw_fh);
	  } else {
	    rgw_fh_rele(fs, obj.fh, 0);
	  }
	}
	chunk->dirents.clear();
	chunk->num_entries = 0;
        delete (chunk);
	drain_cnt++;
      }
      active_chunks.clear();
      /* every lookup(RCB) ref this cache held has now been returned */
      DirentChunk::live_refs.clear();
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
  uint32_t cur_scan{0}; /* refcount tracing only */

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
  /* before anything opens, so no handle is armed under a mode other than
   * the one asked for */
  if (timer_mode == "disarmed") {
    RGWLibFS::stateless_timer_mode = RGWLibFS::StatelessTimerMode::DISARMED;
  } else if (timer_mode == "held") {
    RGWLibFS::stateless_timer_mode = RGWLibFS::StatelessTimerMode::HELD;
  } else if (timer_mode == "normal" || timer_mode == "quiesce") {
    RGWLibFS::stateless_timer_mode = RGWLibFS::StatelessTimerMode::NORMAL;
  } else {
    std::cout << "unknown --timer_mode " << timer_mode << std::endl;
    ASSERT_TRUE(false);
  }

  /* Only the modes which leave a finalize reference outstanding need the
   * library to account for one.  DISARMED arms no timer, so the expected
   * count is entirely the harness's own bookkeeping and the library is
   * left alone. */
  if (timer_mode != "disarmed") {
    RGWLibFS::stateless_ledger = true;
  }

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
      const bool trace = verbose && (ix == 0);
      if (trace) {
	std::cout << "TRACE setup " << obj.name << " after lookup(CREATE): "
		  << obj.rgw_fh->get_refcnt() << std::endl;
      }
      // open object--open transaction
      ret = rgw_open(fs, obj.fh, 0 /* posix flags */, RGW_OPEN_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      if (trace) {
	std::cout << "TRACE setup " << obj.name << " after open: "
		  << obj.rgw_fh->get_refcnt() << std::endl;
      }
      ASSERT_TRUE(obj.rgw_fh->is_open());

      // unstable write data
      size_t nbytes;
      string data("data for ");
      data += object_name;
      int ret = rgw_write(fs, obj.fh, 0, data.length(), &nbytes,
			  (void*) data.c_str(), RGW_WRITE_FLAG_NONE);
      ASSERT_EQ(ret, 0);
      ASSERT_EQ(nbytes, data.length());
      if (trace) {
	std::cout << "TRACE setup " << obj.name << " after write: "
		  << obj.rgw_fh->get_refcnt() << std::endl;
      }

      // commit transaction (write on close)
      ret = rgw_close(fs, obj.fh, 0 /* flags */);
      ASSERT_EQ(ret, 0);

      /* release the ref rgw_lookup() took.  rgw_close() ends the write
       * transaction;  it does not drop the handle reference, and obj_rec has
       * no destructor that does -- so without this every object created here
       * carried a leaked ref for the life of the process, and CHUNKED_READDIR
       * below saw refcounts one higher than the range it checks for. */
      if (trace) {
	std::cout << "TRACE setup " << obj.name << " after close: "
		  << obj.rgw_fh->get_refcnt() << std::endl;
      }
      ret = rgw_fh_rele(fs, obj.fh, 0);
      ASSERT_EQ(ret, 0);
      if (trace) {
	std::cout << "TRACE setup " << obj.name << " after rele: "
		  << obj.rgw_fh->get_refcnt() << std::endl;
      }

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
  /* Clear and drain it.  Without this the cache went out of scope with the
   * entry still in it, so the handle-less placeholder never reached clear()
   * and the null dereference there stayed latent -- and the chunk leaked. */
  chunk->clear();
  ASSERT_EQ(cache.drain(), 1u);
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
      if (verbose && (name_str == "f_0")) {
	std::cout << "TRACE scan " << cur_scan << " " << name_str
		  << " after lookup(RCB): " << obj.rgw_fh->get_refcnt()
		  << std::endl;
      }

      if (leak_every && ((acc.total_entries % leak_every) == 0)) {
	/* take a second reference and drop it on the floor */
	struct rgw_file_handle* leaked_fh{nullptr};
	int lret = rgw_lookup(fs, marker_fh, obj.name.c_str(), &leaked_fh,
			      nullptr, 0, RGW_LOOKUP_FLAG_RCB);
	ceph_assert(lret == 0);
      }

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

  if (timer_mode == "quiesce") {
    /* fire every armed finalize and wait for the last reference back, so
     * the scan runs against a settled count.  this is the mode which
     * verifies StatelessFinalize::operator() returns exactly the
     * reference its constructor took--an imbalance there shows up as a
     * violation here, and is invisible in every other mode. */
    const auto t0 = ceph::mono_clock::now();
    const auto advanced = RGWLibFS::quiesce_stateless_timers();
    const auto elapsed = ceph::mono_clock::now() - t0;

    /* The barrier must have had events to advance, or this mode has
     * asserted nothing about it. */
    EXPECT_GT(advanced, 0UL);

    /* And it must have advanced them rather than waited out the
     * configured interval.  A timer which does not wake on an earlier
     * deadline still gets here, just rgw_nfs_stateless_finalize_secs
     * later -- so without a bound this passes while doing nothing, and
     * the barrier silently degrades into a sleep. */
    const auto interval = std::chrono::seconds(
      cct->_conf->rgw_nfs_stateless_finalize_secs);
    EXPECT_LT(elapsed, interval / 2);
    std::cout << "quiesce advanced " << advanced << " events in "
	      << std::chrono::duration_cast<std::chrono::milliseconds>(
		   elapsed).count()
	      << " ms (interval " << interval.count() << "s)" << std::endl;
  }

  uint32_t grand_total_entries{0};
  uint32_t grand_total_readdir_cnt{0};
  bool eof = false;

  for (uint32_t scan_ix = 0; scan_ix < num_scans; ++scan_ix) {
    cur_scan = scan_ix;
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

  /* Report on the test's own thread.  The in-callback ceph_assert is the
   * diagnosis when it fires;  this is what makes a non-fatal run fail
   * visibly, with every violation listed rather than only the first. */
  if (! refcnt_violations.empty()) {
    std::cout << "refcount violations: " << refcnt_violations.size()
              << std::endl;
    for (const auto& v : refcnt_violations) {
      std::cout << "  name=" << v.name
                << " fh_hk=" << std::hex << v.fh_hk << std::dec
                << " observed=" << v.observed
                << " expected=" << v.expected
                << std::endl;
    }
  }
  if (leak_every) {
    /* Control run.  Catching the injected leak is the success case, so
     * the polarity inverts:  finding nothing means the detector is
     * broken, and that is what fails here.  Each violation must also be
     * an excess, never a shortfall. */
    EXPECT_FALSE(refcnt_violations.empty());
    for (const auto& v : refcnt_violations) {
      EXPECT_GT(v.observed, v.expected);
    }
  } else {
    ASSERT_TRUE(refcnt_violations.empty());
  }

  // print totals
  std::cout << " total entries returned: " << grand_total_entries
            << " total readdir invocations: " << grand_total_readdir_cnt
            << " reclaimed chunks: " << dirent_cache.recycle_count
            << " drained chunks: " << drain_count << std::endl;
}

TEST(LibRGW, SWEEP_LEAK_INJECT) {
  if (! sweep_leak) {
    return;
  }
  /* look the object back up and keep the reference.  the scan already
   * evicted it, so this puts it in the cache at sentinel + 1 and leaves
   * it there for close() to find. */
  for (uint32_t ix = 0; ix < sweep_leak; ++ix) {
    struct rgw_file_handle* leaked_fh{nullptr};
    int ret = rgw_lookup(fs, marker_fh, "f_0", &leaked_fh,
			 nullptr, 0, RGW_LOOKUP_FLAG_NONE);
    ASSERT_EQ(ret, 0);
  }
}

TEST(LibRGW, UMOUNT) {
  if (! fs)
    return;

  /* Give back the two handles this suite has held since setup.  The
   * sweep below counts anything above the sentinel as leaked, and it is
   * right to:  these were the first thing it found. */
  if (marker_fh) {
    (void) rgw_fh_rele(fs, marker_fh, 0);
    marker_fh = nullptr;
  }
  if (bucket_fh) {
    (void) rgw_fh_rele(fs, bucket_fh, 0);
    bucket_fh = nullptr;
  }

  /* Give back the two handles this suite has held since setup.  The
   * sweep counts anything above the sentinel as leaked, and it is right
   * to:  these were the first thing it found. */
  if (marker_fh) {
    (void) rgw_fh_rele(fs, marker_fh, 0);
    marker_fh = nullptr;
  }
  if (bucket_fh) {
    (void) rgw_fh_rele(fs, bucket_fh, 0);
    bucket_fh = nullptr;
  }

  /* The per-entry check only sees handles a reclaim passed through;  one
   * the cache still holds at teardown is invisible to it.  The drain
   * sweep sees every handle, so arm it before the umount which drains. */
  RGWLibFS::sweep_on_drain = true;
  RGWLibFS::sweep_leaks = 0;

  int ret = rgw_umount(fs, RGW_UMOUNT_FLAG_NONE);
  ASSERT_EQ(ret, 0);

  /* every handle still cached should have been at its sentinel count
   * plus any finalize event still pending;  above that was leaked by a
   * path which never gave its reference back */
  if (sweep_leak) {
    /* control:  this injection leaves the handle cached above its
     * sentinel, so the sweep must see it.  finding it is success. */
    EXPECT_GT(RGWLibFS::sweep_leaks.load(), 0UL);
  } else if (! leak_every) {
    EXPECT_EQ(RGWLibFS::sweep_leaks.load(), 0UL);
  } else {
    /* --leak_every strands references on handles which may or may not
     * still be in the cache when close() drains it:  one whose chunk was
     * reclaimed was released with rgw_fh_rele() and stays cached, one
     * still in a live chunk is release_evict()ed out of it first.  Which
     * of those happens depends on the object count, so assert nothing
     * on the sweep here -- the per-entry check is this injection's
     * detector, and it has already run. */
    std::cout << "drain sweep saw " << RGWLibFS::sweep_leaks.load()
              << " leaked handles" << std::endl;
  }
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
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--timer_mode",
				     (char*) nullptr)) {
      timer_mode = val;
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--fatal_refcnt",
				     (char*) nullptr)) {
      fatal_refcnt = (std::stoi(val) != 0);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--sweep_leak",
				     (char*) nullptr)) {
      sweep_leak = std::stoi(val);
    } else if (ceph_argparse_witharg(args, arg_iter, &val, "--leak_every",
				     (char*) nullptr)) {
      leak_every = std::stoi(val);
      /* a control run collects;  fail-fast would abort it at the first
       * find, which is the thing it is trying to demonstrate */
      fatal_refcnt = false;
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
