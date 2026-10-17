// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Reads and pulled writes with out-of-band delivery over the libfabric
 * tcp provider, for qa/standalone/osd/osd-oob-async.sh.
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
 *   ceph_test_osd_oob_read [-c conf] put <pool> <size> [--others N]
 *     [--then-write] [--call] [--wait-file F] <oid>...
 *     writes each object at once, each a write_full of size bytes of its
 *     pattern that the primary pulls out of a window of its own on
 *     127.0.0.1 (OSD-direct PUT), asking for their CRC-64/NVME (another
 *     pattern than write's); --call
 *     puts a CALL of hello.say_hello ahead of each. With --then-write it
 *     then writes 4 KiB inline at the start of the first object, and
 *     reads it whole, ordered after the writes; with --wait-file it
 *     prints "pulls sent" and waits for F to exist first. --others as
 *     for get, while the writes are out. Prints its address, and one line
 *     for each write:
 *
 *       client <addr>
 *       put <oid>: r=0 bytes=N flags=0x0 crc=ok ver=N prev_ver=N ms=N
 *       write <oid>: r=0 ms=N
 *       read <oid>: r=0 data=ok ms=N
 *       check <oid>: data=ok
 *
 *     ver is the object's version the write made, prev_ver the one
 *     before it (0 when there was none); check reads every object back
 *     once all is done. A write that the client resent, and whose resend
 *     found it done, reads bytes=0 flags=0x0 crc=dup when the PG log
 *     kept no record of its pull (one larger than
 *     osd_max_write_op_reply_len): the OSD answered the resend as a dup,
 *     and pulled nothing for it.
 *
 * Ceph options come from CEPH_ARGS, and the conf from -c or CEPH_CONF.
 * Exits 0 when every read returned its object, every write was pulled
 * or found done, and every other op succeeded, the write behind a read
 * included.
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
#include <thread>
#include <vector>

#include <sys/stat.h>

#include "include/rados/librados.hpp"
#include "cls/hello/cls_hello_ops.h"
#include "common/crc64nvme.h"
#include "common/ofi_rma.h"

using librados::ObjectReadOperation;
using librados::ObjectWriteOperation;
using res_t = ObjectReadOperation::rdma_delivery_result;
using clock_type = std::chrono::steady_clock;

namespace {

void usage()
{
  std::fprintf(stderr,
	       "usage: ceph_test_osd_oob_read [-c conf] write <pool> <size> "
	       "<oid>...\n"
	       "       ceph_test_osd_oob_read [-c conf] get <pool> <size> "
	       "[--others N] [--write-behind] [--balance] <oid>...\n"
	       "       ceph_test_osd_oob_read [-c conf] put <pool> <size> "
	       "[--others N] [--then-write] [--call] [--wait-file F] "
	       "<oid>...\n");
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

struct put_t {
  std::string oid;
  std::string expect;
  uint64_t base = 0;  ///< its window's offset
  ObjectWriteOperation op;
  res_t res;
  librados::AioCompletion* c = nullptr;
  uint64_t prev_ver = 0;
  long ms = 0;
};

struct put_opts {
  unsigned others = 0;
  bool then_write = false;
  bool call = false;
  std::string wait_file;
};

int do_put(librados::Rados& rados, librados::IoCtx& io, size_t size,
	   const put_opts& o, const std::vector<std::string>& oids)
{
  std::printf("client %s\n", rados.get_addrs().c_str());
  std::fflush(stdout);
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
  // the primaries read the window while this thread waits; tcp serves
  // remote reads only when polled
  cfg.progress_thread = true;
  cfg.reads = true;
  std::string err;
  auto ep = ofi_rma::Endpoint::open(cfg, &err);
  if (!ep) {
    std::fprintf(stderr, "endpoint: %s\n", err.c_str());
    return 1;
  }
  std::vector<std::unique_ptr<put_t>> puts;
  for (size_t i = 0; i < oids.size(); i++) {
    auto pt = std::make_unique<put_t>();
    pt->oid = oids[i];
    // not what a write of the object wrote
    pt->expect = pattern("put:" + oids[i], size);
    pt->base = i * slot;
    std::memcpy(window.get() + pt->base, pt->expect.data(), size);
    // the version before the write, if the object is there
    uint64_t psize;
    time_t mtime;
    if (io.stat(pt->oid, &psize, &mtime) == 0) {
      pt->prev_ver = io.get_last_version();
    }
    puts.push_back(std::move(pt));
  }
  ofi_rma::Endpoint::window_t w;
  if (ep->register_window(window.get(), len, ofi_rma::memory_t{},
			  ofi_rma::Endpoint::remote_read, &w) != 0) {
    std::fprintf(stderr, "register: %s\n", ep->last_error().c_str());
    return 1;
  }
  const std::string token = ep->window_token(w, 0, len);

  const auto start = clock_type::now();
  for (auto& pt : puts) {
    if (o.call) {
      // an erasure-coded pool with EC optimizations runs a request with
      // a CALL in a coroutine
      librados::bufferlist in;
      in.append("pull");
      pt->op.exec(cls::hello::method::say_hello, in);
    }
    pt->op.write_full_pulled(size, token, pt->base,
			     ObjectWriteOperation::RDMA_DELIVERY_WANT_CRC64,
			     &pt->res);
    pt->c = librados::Rados::aio_create_completion();
    if (int r = io.aio_operate(pt->oid, pt->c, &pt->op); r < 0) {
      std::fprintf(stderr, "aio_operate %s: %s\n", pt->oid.c_str(),
		   strerror(-r));
      return 1;
    }
  }
  if (!o.wait_file.empty()) {
    std::printf("pulls sent\n");
    std::fflush(stdout);
    struct stat st;
    while (::stat(o.wait_file.c_str(), &st) != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }

  // a write inline and a read, ordered after the writes, of the first
  // object: each waits for the pulled write before it
  const std::string& first = puts[0]->oid;
  librados::bufferlist small;
  small.append(pattern("small", 4096));
  librados::AioCompletion* wc = nullptr;
  librados::AioCompletion* rc = nullptr;
  ObjectReadOperation rop;
  librados::bufferlist rbl;
  int rval = 0;
  if (o.then_write) {
    wc = librados::Rados::aio_create_completion();
    if (int r = io.aio_write(first, wc, small, small.length(), 0); r < 0) {
      std::fprintf(stderr, "aio_write %s: %s\n", first.c_str(), strerror(-r));
      return 1;
    }
    rop.read(0, size, &rbl, &rval);
    rc = librados::Rados::aio_create_completion();
    if (int r = io.aio_operate(first, rc, &rop,
			       librados::OPERATION_ORDER_READS_WRITES,
			       nullptr); r < 0) {
      std::fprintf(stderr, "aio_operate %s: %s\n", first.c_str(),
		   strerror(-r));
      return 1;
    }
  }

  // other ops of the same PG, while the writes are out
  unsigned failed = 0;
  long max_ms = 0;
  bool before_puts = true;
  if (o.others) {
    run_others(io, first, o.others, &failed, &max_ms);
    for (const auto& pt : puts) {
      if (pt->c->is_complete()) {
	before_puts = false;
      }
    }
  }
  const long others_ms = ms_since(start);

  bool ok = failed == 0;
  for (auto& pt : puts) {
    pt->c->wait_for_complete();
    pt->ms = ms_since(start);
  }
  for (auto& pt : puts) {
    const int r = pt->c->get_return_value();
    const uint64_t ver = pt->c->get_version64();
    pt->c->release();
    // a resend that finds the write done is answered as a dup: without
    // the record of the earlier pull, no bytes, and not declined, since
    // that pull read the window; the check below reads the data
    const bool dup = r == 0 && pt->res.bytes == 0 && pt->res.flags == 0;
    bool crc = r == 0 && pt->res.bytes == size;
    if (crc && (pt->res.flags &
		ObjectWriteOperation::rdma_delivery_result::CRC64_VALID)) {
      crc = pt->res.crc64 ==
	ceph::crc64nvme(0, pt->expect.data(), pt->expect.size());
    }
    ok = ok && r == 0 && (crc || dup);
    std::printf("put %s: r=%d bytes=%llu flags=0x%x crc=%s ver=%llu "
		"prev_ver=%llu ms=%ld\n",
		pt->oid.c_str(), r, (unsigned long long)pt->res.bytes,
		pt->res.flags, crc ? "ok" : dup ? "dup" : "MISMATCH",
		(unsigned long long)ver, (unsigned long long)pt->prev_ver,
		pt->ms);
  }
  std::string want_first = puts[0]->expect;
  if (o.then_write) {
    wc->wait_for_complete();
    const int wr = wc->get_return_value();
    wc->release();
    std::printf("write %s: r=%d ms=%ld\n", first.c_str(), wr,
		ms_since(start));
    rc->wait_for_complete();
    const int rr = rc->get_return_value();
    rc->release();
    // the read follows both writes: the pulled object, with the start
    // the inline write wrote
    want_first.replace(0, std::min<size_t>(4096, size),
		       small.to_str().substr(0, std::min<size_t>(4096, size)));
    const bool data = rr == 0 && rval == 0 && rbl.to_str() == want_first;
    std::printf("read %s: r=%d data=%s ms=%ld\n", first.c_str(), rr,
		data ? "ok" : "MISMATCH", ms_since(start));
    ok = ok && wr == 0 && data;
  }
  for (auto& pt : puts) {
    librados::bufferlist bl;
    const int r = io.read(pt->oid, bl, size, 0);
    const std::string& want = &pt == &puts[0] ? want_first : pt->expect;
    const bool data = r == int(size) && bl.to_str() == want;
    std::printf("check %s: data=%s\n", pt->oid.c_str(),
		data ? "ok" : "MISMATCH");
    ok = ok && data;
  }
  if (o.others) {
    std::printf("others: n=%u failed=%u max_ms=%ld total_ms=%ld "
		"before_puts=%d\n",
		o.others, failed, max_ms, others_ms, before_puts ? 1 : 0);
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
  put_opts po;
  std::vector<std::string> oids;
  for (size_t i = 3; i < args.size(); i++) {
    if (args[i] == "--others" && i + 1 < args.size()) {
      others = std::strtoul(args[++i].c_str(), nullptr, 10);
    } else if (args[i] == "--write-behind") {
      write_behind = true;
    } else if (args[i] == "--balance") {
      balance = true;
    } else if (args[i] == "--then-write") {
      po.then_write = true;
    } else if (args[i] == "--call") {
      po.call = true;
    } else if (args[i] == "--wait-file" && i + 1 < args.size()) {
      po.wait_file = args[++i];
    } else {
      oids.push_back(args[i]);
    }
  }
  po.others = others;
  if (oids.empty() || (cmd != "write" && cmd != "get" && cmd != "put")) {
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
  } else if (cmd == "get") {
    ret = do_get(io, size, others, write_behind, balance, oids);
  } else {
    ret = do_put(rados, io, size, po, oids);
  }
  io.close();
  rados.shutdown();
  return ret;
}
