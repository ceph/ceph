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

#if defined(HAVE_POSIXAIO)
TEST(KernelDevice, PosixAioSingleBufferCompletion) {
  // Single-aiocb async write+read round trip. Regression guard for the
  // aiocbp[0]/EV_ONESHOT completion path (already correct, but previously
  // untested) alongside the multi-iovec case below.
  uint64_t size = 1048576ull * 16;
  TempBdev bdev{ size };
  const bool buffered = false;

  std::unique_ptr<BlockDevice> b(
    BlockDevice::create(g_ceph_context, bdev.path, NULL, NULL,
      [](void* handle, void* aio) {}, NULL));
  ASSERT_EQ(b->open(bdev.path), 0);

  string s(4096, 'x');
  bufferlist wbl;
  wbl.append(s);

  std::unique_ptr<IOContext> wioc(new IOContext(g_ceph_context, NULL));
  ASSERT_EQ(b->aio_write(0, wbl, wioc.get(), buffered), 0);
  if (wioc->has_pending_aios()) {
    b->aio_submit(wioc.get());
    wioc->aio_wait();
  }

  bufferlist rbl;
  std::unique_ptr<IOContext> rioc(new IOContext(g_ceph_context, NULL));
  ASSERT_EQ(b->aio_read(0, s.size(), &rbl, rioc.get()), 0);
  if (rioc->has_pending_aios()) {
    b->aio_submit(rioc.get());
    rioc->aio_wait();
  }

  ASSERT_EQ(rbl.length(), s.size());
  ASSERT_EQ(memcmp(s.c_str(), rbl.c_str(), s.size()), 0);

  b->close();
}

TEST(KernelDevice, PosixAioMultiIovecCompletion) {
  // Multiple discrete bufferlist segments -> multiple iovecs -> the
  // lio_listio()/n_aiocb > 1 completion path this PR's EV_ONESHOT fix
  // targets. Segments are appended separately (not rebuilt into one
  // contiguous buffer) specifically to force that path.
  uint64_t size = 1048576ull * 16;
  TempBdev bdev{ size };
  const bool buffered = false;

  std::unique_ptr<BlockDevice> b(
    BlockDevice::create(g_ceph_context, bdev.path, NULL, NULL,
      [](void* handle, void* aio) {}, NULL));
  ASSERT_EQ(b->open(bdev.path), 0);

  const int nseg = 8;
  const size_t seglen = 4096;
  bufferlist wbl;
  for (int i = 0; i < nseg; i++) {
    string s(seglen, 'a' + i);
    wbl.append(s);
  }
  ASSERT_GT(wbl.get_num_buffers(), 1u)
    << "test needs a genuinely multi-segment bufferlist to exercise "
       "the lio_listio() path -- got a single contiguous segment instead";

  std::unique_ptr<IOContext> wioc(new IOContext(g_ceph_context, NULL));
  ASSERT_EQ(b->aio_write(0, wbl, wioc.get(), buffered), 0);
  if (wioc->has_pending_aios()) {
    b->aio_submit(wioc.get());
    wioc->aio_wait();
  }

  bufferlist rbl;
  std::unique_ptr<IOContext> rioc(new IOContext(g_ceph_context, NULL));
  ASSERT_EQ(b->aio_read(0, nseg * seglen, &rbl, rioc.get()), 0);
  if (rioc->has_pending_aios()) {
    b->aio_submit(rioc.get());
    rioc->aio_wait();
  }

  ASSERT_EQ(rbl.length(), nseg * seglen);
  for (int i = 0; i < nseg; i++) {
    string expected(seglen, 'a' + i);
    bufferlist chunk;
    chunk.substr_of(rbl, i * seglen, seglen);
    ASSERT_EQ(memcmp(expected.c_str(), chunk.c_str(), seglen), 0)
      << "segment " << i << " mismatch";
  }

  b->close();
}

TEST(KernelDevice, PosixAioConcurrentCompletion) {
  // Multiple independent aio_t's queued on the same IOContext before a
  // single aio_submit() -- real queue depth on the kqueue, not just
  // multiple iovecs within one lio_listio() call. This is the actual
  // scenario the EV_ONESHOT fix guards against: without it, a persistent
  // kevent completion could in principle re-fire and be misattributed to
  // a different, already-completed and already-freed aio_t.
  uint64_t size = 1048576ull * 16;
  TempBdev bdev{ size };
  const bool buffered = false;

  std::unique_ptr<BlockDevice> b(
    BlockDevice::create(g_ceph_context, bdev.path, NULL, NULL,
      [](void* handle, void* aio) {}, NULL));
  ASSERT_EQ(b->open(bdev.path), 0);

  const int ndepth = 6;
  const size_t seglen = 4096;
  std::vector<string> patterns;
  std::vector<bufferlist> wbls(ndepth);
  for (int i = 0; i < ndepth; i++) {
    patterns.push_back(string(seglen, 'a' + i));
    wbls[i].append(patterns[i]);
  }

  std::unique_ptr<IOContext> wioc(new IOContext(g_ceph_context, NULL));
  for (int i = 0; i < ndepth; i++) {
    ASSERT_EQ(b->aio_write(i * seglen, wbls[i], wioc.get(), buffered), 0);
  }
  ASSERT_EQ(wioc->num_pending.load(), ndepth)
    << "test needs genuine queue depth -- aio_write() calls were "
       "coalesced or already submitted instead of staying pending";
  if (wioc->has_pending_aios()) {
    b->aio_submit(wioc.get());
    wioc->aio_wait();
  }

  bufferlist rbl;
  std::unique_ptr<IOContext> rioc(new IOContext(g_ceph_context, NULL));
  ASSERT_EQ(b->aio_read(0, ndepth * seglen, &rbl, rioc.get()), 0);
  if (rioc->has_pending_aios()) {
    b->aio_submit(rioc.get());
    rioc->aio_wait();
  }

  ASSERT_EQ(rbl.length(), ndepth * seglen);
  for (int i = 0; i < ndepth; i++) {
    bufferlist chunk;
    chunk.substr_of(rbl, i * seglen, seglen);
    ASSERT_EQ(memcmp(patterns[i].c_str(), chunk.c_str(), seglen), 0)
      << "segment " << i << " mismatch -- possible misattributed/stale "
         "completion";
  }

  b->close();
}

#endif // HAVE_POSIXAIO

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
