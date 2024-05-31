// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdlib>
#include <ctime>
#include <sstream>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/snap_types.h"
#include "global/global_init.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/stringify.h"
#include "osdc/ObjectCacher.h"

#include "FakeWriteback.h"
#include "MemWriteback.h"

#include <atomic>

using namespace std;

int flush_test()
{
  bool fail = false;
  bool done = false;
  uint64_t delay_ns = 0;
  ceph::mutex lock = ceph::make_mutex("object_cacher_misc");
  MemWriteback writeback(g_ceph_context, &lock, delay_ns);

  int max_dirty_age = 1;
  uint64_t max_cache = 1 << 20; // max cache size, 1MB
  uint64_t max_dirty = 1 << 19; // max dirty, 512KB
  uint64_t target_dirty = 1 << 18; // target dirty, 256KB

  int bl_size = 1 << 12;
  ceph::_page_shift = 16; // 64KB
  int max_dirty_bhs = max_dirty / (1 << ceph::_page_shift); // 8

  std::cout << "Test configuration:\n"
      << setw(20) << "max_cache: " << max_cache << "\n"
      << setw(20) << "max_dirty_age: " << max_dirty_age << "\n"
      << setw(20) << "max_dirty: " << max_dirty << "\n"
      << setw(20) << "ceph::_page_shift: " << ceph::_page_shift << "\n"
      << setw(20) << "max_dirty_bh: " << max_dirty_bhs << "\n"
      << setw(20) << "write extent size: " << bl_size << "\n\n";

  ObjectCacher obc(g_ceph_context, "test", writeback, lock, NULL, NULL,
		   max_cache, // max cache size, 1MB
		   1, // max objects, just one
		   max_dirty, // max dirty, 512KB
		   target_dirty, // target dirty, 256KB
		   max_dirty_age,
		   true);
  obc.start();

  SnapContext snapc;
  ceph_tid_t journal_tid = 0;
  std::string oid("flush_test_obj");
  ObjectCacher::ObjectSet object_set(NULL, 0, 0);
  ceph::bufferlist zeroes_bl;
  zeroes_bl.append_zero(bl_size);

  std::map<int, C_SaferCond> create_finishers;

  utime_t last_start;
  for (int i = 0; i < max_dirty_bhs; ++i) {
    if (i == (max_dirty_bhs - 1)) {
      last_start = ceph_clock_now();
    }
    ObjectCacher::OSDWrite *wr = obc.prepare_write(snapc, zeroes_bl,
						   ceph::real_clock::zero(), 0,
						   ++journal_tid);
    ObjectExtent extent(oid, 0, zeroes_bl.length()*i, zeroes_bl.length(), 0);
    extent.oloc.pool = 0;
    extent.buffer_extents.push_back(make_pair(0, bl_size));
    wr->extents.push_back(extent);
    lock.lock();
    obc.writex(wr, &object_set, &create_finishers[i]);
    lock.unlock();
  }
  utime_t last_end = ceph_clock_now();

  std::cout << "Write " << max_dirty_bhs << " extents"
      << ", total size " << zeroes_bl.length() * max_dirty_bhs
      << ", attain max dirty bufferheads " << max_dirty_bhs
      << ", but below max dirty " << max_dirty << std::endl;

  if (last_end - last_start > utime_t(max_dirty_age, 0)) {
    std::cout << "Error: the last writex took more than " << max_dirty_age
        << "s(max_dirty_age), fail to trigger flush" << std::endl;
    fail = true;;
  } else {
    std::cout << "Info: the last writex took " << last_end - last_start
        << ", success to trigger flush" << std::endl;
  }

  for (int i = 0; i < max_dirty_bhs; ++i) {
    create_finishers[i].wait();
  }

  lock.lock();
  C_SaferCond flushcond;
  obc.flush_all(&flushcond);
  done = obc.flush_all(&flushcond);
  if (!done) {
    lock.unlock();
    flushcond.wait();
    lock.lock();
  }

  obc.release_set(&object_set);
  lock.unlock();
  obc.stop();

  if (fail) {
    std::cout << "Test ObjectCacher flush completed failed" << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "Test ObjectCacher flush completed successfully" << std::endl;
  return EXIT_SUCCESS;
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  bool flush = false;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end();) {
    if (ceph_argparse_flag(args, i, "--flush-test", NULL)) {
      flush = true;
    } else {
      cerr << "unknown option " << *i << std::endl;
      return EXIT_FAILURE;
    }
  }

  if (flush) {
    return flush_test();
  }
}
