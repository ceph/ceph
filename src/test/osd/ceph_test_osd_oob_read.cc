// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Reads with out-of-band delivery over the libfabric tcp provider, for
 * qa/standalone/osd/osd-oob-async.sh.
 *
 *   ceph_test_osd_oob_read [-c conf] write <pool> <size> <oid>...
 *     writes each object: size bytes of a pattern its name picks
 *   ceph_test_osd_oob_read [-c conf] get <pool> <size> [--others N]
 *     [--write-behind] [--balance] <oid>...
 *     reads size bytes of each object at once, each with a delivery
 *     descriptor for a window of its own on 127.0.0.1, asking for its
 *     CRC-64/NVME. With --write-behind it then writes 4 KiB at the start
 *     of the first object, before the reads are back: the read of that
 *     object still gets the object as it was before. While the reads are
 *     out it runs N writes, stats and reads of 4 KiB, one after another,
 *     on other objects named after the first one (which, in a pool of
 *     one PG, share its PG). Prints one line for each read:
 *
 *       get <oid>: r=0 bytes=N inline=N flags=0x10 data=ok crc=ok ms=N
 *
 *     bytes is what the OSD placed, inline what came back in the reply;
 *     data checks whichever holds the object, crc the OSD's checksum of
 *     the placed bytes. Then, with --write-behind:
 *
 *       behind <oid>: r=0 ms=N after_read=1
 *
 *     after_read is 1 when the write's reply came after the read's. Then,
 *     with --others:
 *
 *       others: n=N failed=N max_ms=N total_ms=N before_reads=1
 *
 *     before_reads is 1 when the others were done before any read was.
 *
 * Ceph options come from CEPH_ARGS, and the conf from -c or CEPH_CONF.
 * Exits 0 when every read returned its object, and every other op
 * succeeded, the write behind included.
 */

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "include/rados/librados.hpp"
#include "common/crc64nvme.h"
#include "common/ofi_rma.h"

using librados::ObjectReadOperation;
using res_t = ObjectReadOperation::rdma_delivery_result;
using clock_type = std::chrono::steady_clock;

namespace {

void usage()
{
  std::fprintf(stderr,
	       "usage: ceph_test_osd_oob_read [-c conf] write <pool> <size> "
	       "<oid>...\n"
	       "       ceph_test_osd_oob_read [-c conf] get <pool> <size> "
	       "[--others N] [--write-behind] [--balance] <oid>...\n");
}

/// size bytes that only oid's name picks
std::string pattern(const std::string& oid, size_t size)
{
  uint32_t seed = 2166136261u;
  for (unsigned char c : oid) {
    seed = (seed ^ c) * 16777619u;
  }
  std::minstd_rand rng(seed ? seed : 1);
  std::string s(size, '\0');
  for (auto& c : s) {
    c = static_cast<char>(rng());
  }
  return s;
}

long ms_since(clock_type::time_point t)
{
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    clock_type::now() - t).count();
}

/// the replies in the order they came: each takes the next number
std::atomic<unsigned> replies{0};

/// a completion's callback: arg is where its reply's number goes
void note_reply(librados::completion_t, void* arg)
{
  static_cast<std::atomic<unsigned>*>(arg)->store(replies.fetch_add(1) + 1);
}

struct read_t {
  std::string oid;
  std::string expect;
  uint64_t base = 0;  ///< its window's offset
  ObjectReadOperation op;
  librados::bufferlist bl;
  int rval = 0;
  res_t res;
  librados::AioCompletion* c = nullptr;
  std::atomic<unsigned> order{0};  ///< its reply's number
  long ms = 0;
};

/**
 * n writes, stats and reads of 4 KiB, one after another, on objects
 * named after first, which in a pool of one PG share its PG.
 */
void run_others(librados::IoCtx& io, const std::string& first, unsigned n,
		unsigned* failed, long* max_ms)
{
  librados::bufferlist small;
  small.append(pattern("small", 4096));
  for (unsigned i = 0; i < n; i++) {
    const std::string oid = first + ".other." + std::to_string(i % 8);
    const auto t = clock_type::now();
    int r = 0;
    switch (i % 3) {
    case 0:
      r = io.write(oid, small, small.length(), 0);
      break;
    case 1: {
      uint64_t psize;
      time_t mtime;
      r = io.stat(oid, &psize, &mtime);
      break;
    }
    default: {
      librados::bufferlist bl;
      r = io.read(oid, bl, 4096, 0);
      break;
    }
    }
    // a stat or read of an object no write reached yet is fine
    if (r < 0 && r != -ENOENT) {
      (*failed)++;
    }
    *max_ms = std::max(*max_ms, ms_since(t));
  }
}

int do_write(librados::IoCtx& io, size_t size,
	     const std::vector<std::string>& oids)
{
  for (const auto& oid : oids) {
    librados::bufferlist bl;
    bl.append(pattern(oid, size));
    if (int r = io.write_full(oid, bl); r < 0) {
      std::fprintf(stderr, "write %s: %s\n", oid.c_str(), strerror(-r));
      return 1;
    }
  }
  return 0;
}

int do_get(librados::IoCtx& io, size_t size, unsigned others,
	   bool write_behind, bool balance,
	   const std::vector<std::string>& oids)
{
  const size_t slot = (std::max<size_t>(size, 1) + 4095) & ~size_t(4095);
  const size_t len = slot * oids.size();
  void* mem = nullptr;
  if (posix_memalign(&mem, 4096, len)) {
    return 1;
  }
  // freed after the endpoint closed
  std::unique_ptr<char, decltype(&std::free)> window(
    static_cast<char*>(mem), &std::free);
  std::memset(window.get(), 0, len);
  ofi_rma::config_t cfg;
  cfg.provider = "tcp";
  cfg.node = "127.0.0.1";
  // the OSDs' writes land while this thread waits for the replies
  cfg.progress_thread = true;
  std::string err;
  auto ep = ofi_rma::Endpoint::open(cfg, &err);
  if (!ep) {
    std::fprintf(stderr, "endpoint: %s\n", err.c_str());
    return 1;
  }
  ofi_rma::Endpoint::window_t w;
  if (ep->register_window(window.get(), len, &w) < 0) {
    std::fprintf(stderr, "register: %s\n", ep->last_error().c_str());
    return 1;
  }
  const std::string token = ep->window_token(w, 0, len);

  std::vector<std::unique_ptr<read_t>> reads;
  const auto start = clock_type::now();
  for (size_t i = 0; i < oids.size(); i++) {
    auto rd = std::make_unique<read_t>();
    rd->oid = oids[i];
    // what write wrote there: its pattern is the same for any size
    rd->expect = pattern(oids[i], size);
    rd->base = i * slot;
    rd->op.read(0, size, &rd->bl, &rd->rval);
    rd->op.set_rdma_delivery(token, rd->base,
			     ObjectReadOperation::RDMA_DELIVERY_WANT_CRC64,
			     &rd->res);
    rd->c = librados::Rados::aio_create_completion(&rd->order, note_reply);
    if (int r = io.aio_operate(rd->oid, rd->c, &rd->op,
			       balance ? librados::OPERATION_BALANCE_READS : 0,
			       nullptr); r < 0) {
      std::fprintf(stderr, "aio_operate %s: %s\n", rd->oid.c_str(),
		   strerror(-r));
      return 1;
    }
    reads.push_back(std::move(rd));
  }

  // a write to the first object behind its read, which may not change
  // what the read gets
  librados::AioCompletion* behind = nullptr;
  std::atomic<unsigned> behind_order{0};
  if (write_behind) {
    librados::bufferlist small;
    small.append(pattern("small", 4096));
    behind = librados::Rados::aio_create_completion(&behind_order, note_reply);
    if (int r = io.aio_write(oids[0], behind, small, small.length(), 0);
	r < 0) {
      std::fprintf(stderr, "aio_write %s: %s\n", oids[0].c_str(),
		   strerror(-r));
      return 1;
    }
  }

  // other ops of the same PG, while the reads are out
  unsigned failed = 0;
  long max_ms = 0;
  bool before_reads = true;
  if (others) {
    run_others(io, oids[0], others, &failed, &max_ms);
    for (const auto& rd : reads) {
      if (rd->c->is_complete()) {
	before_reads = false;
      }
    }
  }
  const long others_ms = ms_since(start);

  bool ok = failed == 0;
  for (auto& rd : reads) {
    rd->c->wait_for_complete_and_cb();
    rd->ms = ms_since(start);
  }
  int behind_r = 0;
  long behind_ms = 0;
  if (behind) {
    behind->wait_for_complete_and_cb();
    behind_ms = ms_since(start);
    behind_r = behind->get_return_value();
    behind->release();
    ok = ok && behind_r == 0;
  }
  // whatever the OSDs wrote is in the window
  ep->sync();
  for (auto& rd : reads) {
    const int r = rd->c->get_return_value();
    rd->c->release();
    bool data = false;
    bool crc = true;
    if (r == 0 && rd->rval == 0) {
      if (rd->res.bytes) {
	data = rd->res.bytes == rd->expect.size() && rd->bl.length() == 0 &&
	  std::memcmp(window.get() + rd->base, rd->expect.data(),
		      rd->expect.size()) == 0;
	if (rd->res.flags & ObjectReadOperation::rdma_delivery_result::CRC64_VALID) {
	  crc = rd->res.crc64 ==
	    ceph::crc64nvme(0, rd->expect.data(), rd->expect.size());
	}
      } else {
	data = rd->bl.length() == rd->expect.size() &&
	  std::memcmp(rd->bl.c_str(), rd->expect.data(),
		      rd->expect.size()) == 0;
      }
    }
    ok = ok && data && crc;
    std::printf("get %s: r=%d bytes=%llu inline=%u flags=0x%x data=%s "
		"crc=%s ms=%ld\n",
		rd->oid.c_str(), r, (unsigned long long)rd->res.bytes,
		rd->bl.length(), rd->res.flags, data ? "ok" : "MISMATCH",
		crc ? "ok" : "MISMATCH", rd->ms);
  }
  if (write_behind) {
    std::printf("behind %s: r=%d ms=%ld after_read=%d\n", oids[0].c_str(),
		behind_r, behind_ms,
		reads[0]->order.load() < behind_order.load() ? 1 : 0);
  }
  if (others) {
    std::printf("others: n=%u failed=%u max_ms=%ld total_ms=%ld "
		"before_reads=%d\n",
		others, failed, max_ms, others_ms, before_reads ? 1 : 0);
  }
  std::fflush(stdout);
  return ok ? 0 : 1;
}

} // anonymous namespace

int main(int argc, char** argv)
{
  std::vector<std::string> args(argv + 1, argv + argc);
  std::string conf;
  if (args.size() >= 2 && args[0] == "-c") {
    conf = args[1];
    args.erase(args.begin(), args.begin() + 2);
  }
  if (args.size() < 4) {
    usage();
    return 2;
  }
  const std::string cmd = args[0];
  const std::string pool = args[1];
  const size_t size = std::strtoull(args[2].c_str(), nullptr, 10);
  unsigned others = 0;
  bool write_behind = false;
  bool balance = false;
  std::vector<std::string> oids;
  for (size_t i = 3; i < args.size(); i++) {
    if (args[i] == "--others" && i + 1 < args.size()) {
      others = std::strtoul(args[++i].c_str(), nullptr, 10);
    } else if (args[i] == "--write-behind") {
      write_behind = true;
    } else if (args[i] == "--balance") {
      balance = true;
    } else {
      oids.push_back(args[i]);
    }
  }
  if (oids.empty() || (cmd != "write" && cmd != "get")) {
    usage();
    return 2;
  }

  librados::Rados rados;
  if (int r = rados.init(nullptr); r < 0) {
    std::fprintf(stderr, "init: %s\n", strerror(-r));
    return 1;
  }
  if (int r = rados.conf_read_file(conf.empty() ? nullptr : conf.c_str());
      r < 0 && !conf.empty()) {
    std::fprintf(stderr, "conf %s: %s\n", conf.c_str(), strerror(-r));
    return 1;
  }
  rados.conf_parse_env(nullptr);
  if (int r = rados.connect(); r < 0) {
    std::fprintf(stderr, "connect: %s\n", strerror(-r));
    return 1;
  }
  librados::IoCtx io;
  if (int r = rados.ioctx_create(pool.c_str(), io); r < 0) {
    std::fprintf(stderr, "pool %s: %s\n", pool.c_str(), strerror(-r));
    return 1;
  }
  int ret;
  if (cmd == "write") {
    ret = do_write(io, size, oids);
  } else {
    ret = do_get(io, size, others, write_behind, balance, oids);
  }
  io.close();
  rados.shutdown();
  return ret;
}
