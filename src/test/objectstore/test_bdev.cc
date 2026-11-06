// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "libaio_probe.h"
#include "blk/BlockDevice.h"

using namespace std;

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
  SKIP_IF_NO_LIBAIO();
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


// The buffered variants below exercise reads issued through the non-O_DIRECT
// descriptor, which is what bluestore_page_cache_read turns on. Correctness
// there rests entirely on the kernel dropping page cache pages that an
// O_DIRECT write has superseded, so that is tested explicitly.

namespace {

const uint64_t BUF_TEST_SIZE = 1048576ull * 128;
const uint64_t BUF_TEST_OFF = 1048576ull;
const uint64_t BUF_TEST_LEN = 0x10000;

std::unique_ptr<BlockDevice> open_temp_bdev(const std::string& path)
{
  std::unique_ptr<BlockDevice> b(
    BlockDevice::create(g_ceph_context, path, NULL, NULL,
      [](void* handle, void* aio) {}, NULL));
  if (b->open(path) < 0) {
    return nullptr;
  }
  return b;
}

bufferlist make_pattern(char c, uint64_t len)
{
  bufferlist bl;
  bl.append(std::string(len, c));
  return bl;
}

// Write through the O_DIRECT descriptor, the way BlueStore writes object data.
// aio_write() claims the buffers it is given, so take a copy: the caller
// still needs the original to compare against.
void direct_write(BlockDevice* b, uint64_t off, bufferlist bl)
{
  IOContext ioc(g_ceph_context, NULL);
  ASSERT_EQ(0, b->aio_write(off, bl, &ioc, false));
  if (ioc.has_pending_aios()) {
    b->aio_submit(&ioc);
    ioc.aio_wait();
  }
  ASSERT_EQ(0, b->flush());
}

// A buffered aio_read is done synchronously and queues nothing on the
// IOContext, so the submit and wait have to stay conditional.
void issue_read(BlockDevice* b, uint64_t off, uint64_t len,
                bufferlist* out, bool buffered)
{
  IOContext ioc(g_ceph_context, NULL);
  ASSERT_EQ(0, b->aio_read(off, len, out, &ioc, buffered));
  if (ioc.has_pending_aios()) {
    b->aio_submit(&ioc);
    ioc.aio_wait();
    ASSERT_EQ(0, ioc.get_return_value());
  }
  ASSERT_EQ(len, out->length());
}

} // anonymous namespace

TEST(KernelDevice, BufferedAioReadRoundTrip) {
  TempBdev bdev{BUF_TEST_SIZE};
  auto b = open_temp_bdev(bdev.path);
  ASSERT_TRUE(b != nullptr);

  bufferlist out_bl = make_pattern('q', BUF_TEST_LEN);
  direct_write(b.get(), BUF_TEST_OFF, out_bl);

  bufferlist in_bl;
  issue_read(b.get(), BUF_TEST_OFF, BUF_TEST_LEN, &in_bl, true);
  ASSERT_TRUE(in_bl.contents_equal(out_bl));

  b->close();
}

TEST(KernelDevice, BufferedAioReadMatchesDirect) {
  TempBdev bdev{BUF_TEST_SIZE};
  auto b = open_temp_bdev(bdev.path);
  ASSERT_TRUE(b != nullptr);

  bufferlist out_bl = make_pattern('r', BUF_TEST_LEN);
  direct_write(b.get(), BUF_TEST_OFF, out_bl);

  bufferlist direct_bl, buffered_bl;
  issue_read(b.get(), BUF_TEST_OFF, BUF_TEST_LEN, &direct_bl, false);
  issue_read(b.get(), BUF_TEST_OFF, BUF_TEST_LEN, &buffered_bl, true);
  ASSERT_TRUE(direct_bl.contents_equal(buffered_bl));
  ASSERT_TRUE(direct_bl.contents_equal(out_bl));

  b->close();
}

TEST(KernelDevice, BufferedAioReadCoherentWithDirectWrite) {
  // The property bluestore_page_cache_read depends on: once a range has been
  // pulled into the page cache by a buffered read, an O_DIRECT write over that
  // range must not leave the stale copy readable. This is the same property
  // bluefs_buffered_io has relied on by default for years; if it does not hold,
  // reading object data through the page cache is not safe.
  TempBdev bdev{BUF_TEST_SIZE};
  auto b = open_temp_bdev(bdev.path);
  ASSERT_TRUE(b != nullptr);

  bufferlist first = make_pattern('a', BUF_TEST_LEN);
  direct_write(b.get(), BUF_TEST_OFF, first);

  // populate the page cache for this range
  bufferlist warm_bl;
  issue_read(b.get(), BUF_TEST_OFF, BUF_TEST_LEN, &warm_bl, true);
  ASSERT_TRUE(warm_bl.contents_equal(first));

  // now supersede it via the O_DIRECT descriptor
  bufferlist second = make_pattern('b', BUF_TEST_LEN);
  direct_write(b.get(), BUF_TEST_OFF, second);

  bufferlist after_bl;
  issue_read(b.get(), BUF_TEST_OFF, BUF_TEST_LEN, &after_bl, true);
  ASSERT_FALSE(after_bl.contents_equal(first))
      << "buffered read returned stale data after an O_DIRECT overwrite";
  ASSERT_TRUE(after_bl.contents_equal(second));

  b->close();
}

TEST(KernelDevice, BufferedAioReadIsSynchronous) {
  // libaio cannot submit buffered I/O without blocking, so a buffered read is
  // always served synchronously. That is observable: the synchronous path
  // leaves nothing queued on the IOContext. This is what distinguishes a
  // buffered read from a direct one at this layer, so assert on it rather
  // than only on the data being correct.
  TempBdev bdev{BUF_TEST_SIZE};
  auto b = open_temp_bdev(bdev.path);
  ASSERT_TRUE(b != nullptr);

  bufferlist out_bl = make_pattern('s', BUF_TEST_LEN);
  direct_write(b.get(), BUF_TEST_OFF, out_bl);

  {
    bufferlist in;
    IOContext ioc(g_ceph_context, NULL);
    ASSERT_EQ(0, b->aio_read(BUF_TEST_OFF, BUF_TEST_LEN, &in, &ioc, false));
    ASSERT_TRUE(ioc.has_pending_aios())
        << "a direct read should be queued for async submission";
    b->aio_submit(&ioc);
    ioc.aio_wait();
    ASSERT_TRUE(in.contents_equal(out_bl));
  }
  {
    bufferlist in;
    IOContext ioc(g_ceph_context, NULL);
    ASSERT_EQ(0, b->aio_read(BUF_TEST_OFF, BUF_TEST_LEN, &in, &ioc, true));
    ASSERT_FALSE(ioc.has_pending_aios())
        << "a buffered read must not be handed to libaio";
    // completed inline, so the data is already there with no submit/wait
    ASSERT_TRUE(in.contents_equal(out_bl));
  }

  b->close();
}

TEST(KernelDevice, BufferedAndDirectReadsInOneIOContext) {
  // A buffered read may complete synchronously while a direct one is still
  // queued on the same IOContext. Both must land, and in the order issued.
  TempBdev bdev{BUF_TEST_SIZE};
  auto b = open_temp_bdev(bdev.path);
  ASSERT_TRUE(b != nullptr);

  bufferlist bl_a = make_pattern('x', BUF_TEST_LEN);
  bufferlist bl_b = make_pattern('y', BUF_TEST_LEN);
  direct_write(b.get(), BUF_TEST_OFF, bl_a);
  direct_write(b.get(), BUF_TEST_OFF + BUF_TEST_LEN, bl_b);

  bufferlist got_a, got_b;
  IOContext ioc(g_ceph_context, NULL);
  ASSERT_EQ(0, b->aio_read(BUF_TEST_OFF, BUF_TEST_LEN, &got_a, &ioc, true));
  ASSERT_EQ(0, b->aio_read(BUF_TEST_OFF + BUF_TEST_LEN, BUF_TEST_LEN, &got_b,
                           &ioc, false));
  if (ioc.has_pending_aios()) {
    b->aio_submit(&ioc);
    ioc.aio_wait();
    ASSERT_EQ(0, ioc.get_return_value());
  }
  ASSERT_TRUE(got_a.contents_equal(bl_a));
  ASSERT_TRUE(got_b.contents_equal(bl_b));

  b->close();
}

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);
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
