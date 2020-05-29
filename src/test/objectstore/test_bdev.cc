// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <string.h>
#include <iostream>
#include <gtest/gtest.h>
#include "global/global_init.h"
#include "global/global_context.h"
#include "common/ceph_context.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"

#include "blk/BlockDevice.h"

class TempBdev {
public:
  TempBdev(uint64_t size)
    : path{get_temp_bdev(size)}
  {}
  ~TempBdev() {
    rm_temp_bdev(path);
  }
  const std::string path;
private:
  static string get_temp_bdev(uint64_t size)
  {
    static int n = 0;
    string fn = "ceph_test_bluefs.tmp.block." + stringify(getpid())
      + "." + stringify(++n);
    int fd = ::open(fn.c_str(), O_CREAT|O_RDWR|O_TRUNC, 0644);
    ceph_assert(fd >= 0);
    int r = ::ftruncate(fd, size);
    ceph_assert(r >= 0);
    ::close(fd);
    return fn;
  }
  static void rm_temp_bdev(string f)
  {
    ::unlink(f.c_str());
  }
};

TEST(KernelDevice, Ticket45337) {
   // Large (>=2 GB) writes are incomplete when bluefs_buffered_io = true

  uint64_t size = 1048576ull * 8192;
  TempBdev bdev{ size };
  
  const bool buffered = true;

  std::unique_ptr<BlockDevice> b(
    BlockDevice::create(g_ceph_context, bdev.path, NULL, NULL,
      [](void* handle, void* aio) {}, NULL));
  bufferlist bl;
  // writing a bit less than 4GB
  for (auto i = 0; i < 4000; i++) {
    string s(1048576, 'a' + (i % 28));
    bl.append(s);
  }
  uint64_t magic_offs = bl.length();
  string s(4086, 'z');
  s += "0123456789";
  bl.append(s);

  {
    int r = b->open(bdev.path);
    if (r < 0) {
      std::cerr << "open " << bdev.path << " failed" << std::endl;
      return;
    }
  }
  std::unique_ptr<IOContext> ioc(new IOContext(g_ceph_context, NULL));

  auto r = b->aio_write(0, bl, ioc.get(), buffered);
  ASSERT_EQ(r, 0);

  if (ioc->has_pending_aios()) {
    b->aio_submit(ioc.get());
    ioc->aio_wait();
  }

  char outbuf[0x1000];
  r = b->read_random(magic_offs, sizeof(outbuf), outbuf, buffered);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(memcmp(s.c_str(), outbuf, sizeof(outbuf)), 0);

  b->close();
}

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  map<string,string> defaults = {
    { "debug_bdev", "1/20" }
  };

  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.set_val(
    "enable_experimental_unrecoverable_data_corrupting_features",
    "*");
  g_ceph_context->_conf.apply_changes(nullptr);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
