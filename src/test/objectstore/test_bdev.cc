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
#include "blk/aio/aio.h"

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
  // A write made of several separate page-aligned segments must be
  // submitted as a single aio_t whose iovec array has one entry per
  // segment. The segments are page-aligned so aio_write() does not
  // rebuild them into one contiguous buffer.
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
    bufferptr p = buffer::create_small_page_aligned(seglen);
    memset(p.c_str(), 'a' + i, seglen);
    wbl.append(std::move(p));
  }

  std::unique_ptr<IOContext> wioc(new IOContext(g_ceph_context, NULL));
  ASSERT_EQ(b->aio_write(0, wbl, wioc.get(), buffered), 0);
  ASSERT_EQ(wioc->pending_aios.size(), 1u);
  ASSERT_EQ(wioc->pending_aios.front().iov.size(), (size_t)nseg);
  b->aio_submit(wioc.get());
  wioc->aio_wait();

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

#if defined(HAVE_POSIXAIO)
// aio_t::advance() splits a request that the kernel completed short, so that
// the rest can be resubmitted. It cannot be reached through BlockDevice: a
// short return cannot be provoked from userspace, it is what a raw device
// does on its own. So test the splitting itself, where an off-by-one or a
// missed alignment check would corrupt whatever is written next.
//
// The rest of the request is direct IO again, so advance() must refuse any
// split that would leave the start of the first iovec, or the file offset,
// unaligned, and it must leave the request untouched when it refuses.
namespace {

// A page-aligned buffer to carve iovecs out of, so that the aligned cases
// really are aligned.
class AlignedBuf {
public:
  explicit AlignedBuf(size_t len) : length(len) {
    void *p = nullptr;
    ceph_assert(::posix_memalign(&p, 4096, len) == 0);
    ::memset(p, 0, len);
    base = static_cast<char*>(p);
  }
  ~AlignedBuf() { ::free(base); }
  char *base;
  size_t length;
};

// An aio_t that writes nseg segments of seglen bytes, starting at offset.
void make_write(aio_t &a, AlignedBuf &buf, size_t nseg, size_t seglen,
                uint64_t offset) {
  for (size_t i = 0; i < nseg; i++) {
    a.iov.push_back(iovec{buf.base + i * seglen, seglen});
  }
  a.pwritev(offset, nseg * seglen);
}

// Total of the iovec lengths, which must always equal length.
uint64_t iov_total(const aio_t &a) {
  uint64_t t = 0;
  for (const auto &v : a.iov) {
    t += v.iov_len;
  }
  return t;
}

} // namespace

TEST(KernelDevice, PosixAioShortReturnRejectsWhatItCannotSplit) {
  AlignedBuf buf{32768};
  const uint64_t align = 4096;

  // Each case must be refused, and must leave the request as it was.
  struct {
    const char *what;
    uint64_t n;
    uint64_t align;
  } cases[] = {
    { "nothing transferred",      0,     align },
    { "the whole request",        16384, align },
    { "more than the request",    20480, align },
    { "no alignment given",       4096,  0     },
    { "not a multiple of align",  4095,  align },
    { "not a multiple of align",  6144,  align * 2 },
  };

  for (const auto &c : cases) {
    aio_t a{nullptr, 7};
    make_write(a, buf, 4, 4096, 8192);
    const auto before = a.iov;

    ASSERT_FALSE(a.advance(c.n, c.align)) << c.what << " (n=" << c.n << ")";
    ASSERT_EQ(a.offset, 8192u) << c.what;
    ASSERT_EQ(a.length, 16384u) << c.what;
    ASSERT_EQ(a.iov.size(), before.size()) << c.what;
    for (size_t i = 0; i < before.size(); i++) {
      ASSERT_EQ(a.iov[i].iov_base, before[i].iov_base) << c.what << " iov " << i;
      ASSERT_EQ(a.iov[i].iov_len, before[i].iov_len) << c.what << " iov " << i;
    }
  }
}

TEST(KernelDevice, PosixAioShortReturnOnIovecBoundary) {
  AlignedBuf buf{32768};
  aio_t a{nullptr, 7};
  make_write(a, buf, 4, 4096, 8192);
  // What a short completion left behind: advance() has to clear it, or the
  // reaper would take the resubmitted request for one that is already done.
  a.rval = 8192;

  // Two whole iovecs went out: drop them, keep the other two.
  ASSERT_TRUE(a.advance(8192, 4096));
  ASSERT_EQ(a.offset, 8192u + 8192u);
  ASSERT_EQ(a.length, 8192u);
  ASSERT_EQ(a.iov.size(), 2u);
  ASSERT_EQ(a.iov[0].iov_base, buf.base + 8192);
  ASSERT_EQ(a.iov[0].iov_len, 4096u);
  ASSERT_EQ(a.iov[1].iov_base, buf.base + 12288);
  ASSERT_EQ(iov_total(a), a.length);
  // Re-prepared, so the next completion is not mistaken for this one.
  ASSERT_EQ(a.get_return_value(), -1000);
  ASSERT_EQ(a.aio.aio_offset, (off_t)a.offset);
  ASSERT_EQ(a.aio.aio_iovcnt, 2);
  ASSERT_EQ(a.aio.aio_iov, a.iov.data());
  ASSERT_EQ(a.aio.aio_lio_opcode, LIO_WRITE);
}

TEST(KernelDevice, PosixAioShortReturnInsideIovec) {
  AlignedBuf buf{32768};
  aio_t a{nullptr, 7};
  // One 16K segment: a short return has to cut it in two.
  a.iov.push_back(iovec{buf.base, 16384});
  a.pwritev(0, 16384);

  ASSERT_TRUE(a.advance(4096, 4096));
  ASSERT_EQ(a.offset, 4096u);
  ASSERT_EQ(a.length, 12288u);
  ASSERT_EQ(a.iov.size(), 1u);
  ASSERT_EQ(a.iov[0].iov_base, buf.base + 4096);
  ASSERT_EQ(a.iov[0].iov_len, 12288u);
  ASSERT_EQ(iov_total(a), a.length);
}

TEST(KernelDevice, PosixAioShortReturnRefusesUnalignedRemainder) {
  AlignedBuf buf{32768};
  aio_t a{nullptr, 7};
  // Segments that are not a multiple of the alignment: cutting inside the
  // second one would leave its remainder starting at an unaligned address,
  // which direct IO cannot submit.
  a.iov.push_back(iovec{buf.base, 4096});
  a.iov.push_back(iovec{buf.base + 4096, 6144});
  a.pwritev(0, 10240);
  const auto before = a.iov;

  // 8192 lands 4096 into the second iovec, at buf.base + 8192: aligned, so
  // this one is allowed.
  aio_t ok{nullptr, 7};
  ok.iov = before;
  ok.pwritev(0, 10240);
  ASSERT_TRUE(ok.advance(8192, 4096));
  ASSERT_EQ(ok.iov.size(), 1u);
  ASSERT_EQ(ok.iov[0].iov_base, buf.base + 8192);
  ASSERT_EQ(ok.iov[0].iov_len, 2048u);

  // The same split with a 8192-byte alignment is not: 8192 % 8192 == 0, but
  // the remainder would start 4096 into the iovec, which is not a multiple
  // of 8192.
  ASSERT_FALSE(a.advance(8192, 8192));
  ASSERT_EQ(a.iov.size(), before.size());
  ASSERT_EQ(a.length, 10240u);
}

TEST(KernelDevice, PosixAioShortReturnRefusesUnalignedBase) {
  AlignedBuf buf{32768};
  aio_t a{nullptr, 7};
  // An iovec that does not start on an alignment boundary. The split itself
  // is a multiple of the alignment, so only checking that is not enough:
  // what matters is where the remainder would start.
  a.iov.push_back(iovec{buf.base + 512, 8192});
  a.pwritev(0, 8192);
  const auto before = a.iov;

  // 4096 % 4096 == 0, but the rest would begin at buf.base + 4608.
  ASSERT_FALSE(a.advance(4096, 4096));
  ASSERT_EQ(a.iov.size(), before.size());
  ASSERT_EQ(a.iov[0].iov_base, before[0].iov_base);
  ASSERT_EQ(a.iov[0].iov_len, before[0].iov_len);
  ASSERT_EQ(a.offset, 0u);
  ASSERT_EQ(a.length, 8192u);
}

TEST(KernelDevice, PosixAioShortReturnKeepsReadOpcode) {
  AlignedBuf buf{32768};
  aio_t a{nullptr, 7};
  for (size_t i = 0; i < 4; i++) {
    a.iov.push_back(iovec{buf.base + i * 4096, 4096});
  }
  a.preadv(4096, 16384);

  ASSERT_TRUE(a.advance(4096, 4096));
  ASSERT_EQ(a.offset, 8192u);
  ASSERT_EQ(a.length, 12288u);
  // A read that is split must stay a read.
  ASSERT_EQ(a.aio.aio_lio_opcode, LIO_READ);
}

TEST(KernelDevice, PosixAioShortReturnTwice) {
  AlignedBuf buf{32768};
  aio_t a{nullptr, 7};
  make_write(a, buf, 4, 4096, 0);

  // A device that keeps completing short must not leave the request in a
  // state where the second split loses or repeats bytes.
  ASSERT_TRUE(a.advance(4096, 4096));
  ASSERT_EQ(a.offset, 4096u);
  ASSERT_EQ(a.length, 12288u);
  ASSERT_TRUE(a.advance(8192, 4096));
  ASSERT_EQ(a.offset, 12288u);
  ASSERT_EQ(a.length, 4096u);
  ASSERT_EQ(a.iov.size(), 1u);
  ASSERT_EQ(a.iov[0].iov_base, buf.base + 12288);
  ASSERT_EQ(a.iov[0].iov_len, 4096u);
  ASSERT_EQ(iov_total(a), a.length);
  // And the last of it cannot be split again.
  ASSERT_FALSE(a.advance(4096, 4096));
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
