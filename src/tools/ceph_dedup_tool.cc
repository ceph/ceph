// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Myoungwon Oh <ohmyoungwon@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "include/types.h"

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"

#include "acconfig.h"

#include "common/Cond.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/obj_bencher.h"
#include "global/global_init.h"

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <locale>
#include <memory>
#include <math.h>

#include "tools/RadosDump.h"
#include "cls/cas/cls_cas_client.h"
#include "include/stringify.h"
#include "global/signal_handler.h"
#include "common/CDC.h"

struct EstimateResult {
  std::unique_ptr<CDC> cdc;

  uint64_t chunk_size;

  ceph::mutex lock = ceph::make_mutex("EstimateResult::lock");

  // < key, <count, chunk_size> >
  map< string, pair <uint64_t, uint64_t> > chunk_statistics;
  uint64_t total_bytes = 0;

  EstimateResult(std::string alg, int chunk_size)
    : cdc(CDC::create(alg, chunk_size)),
      chunk_size(1ull << chunk_size) {}

  void add_chunk(bufferlist& chunk, const std::string& fp_algo) {
    string fp;
    if (fp_algo == "sha1") {
      sha1_digest_t sha1_val = crypto::digest<crypto::SHA1>(chunk);
      fp = sha1_val.to_str();
    } else if (fp_algo == "sha256") {
      sha256_digest_t sha256_val = crypto::digest<crypto::SHA256>(chunk);
      fp = sha256_val.to_str();
    } else if (fp_algo == "sha512") {
      sha512_digest_t sha512_val = crypto::digest<crypto::SHA512>(chunk);
      fp = sha512_val.to_str();
    } else {
      ceph_assert(0 == "no support fingerperint algorithm");
    }

    std::lock_guard l(lock);
    auto p = chunk_statistics.find(fp);
    if (p != chunk_statistics.end()) {
      p->second.first++;
      if (p->second.second != chunk.length()) {
	cerr << "warning: hash collision on " << fp
	     << ": was " << p->second.second
	     << " now " << chunk.length() << std::endl;
      }
    } else {
      chunk_statistics[fp] = make_pair(1, chunk.length());
    }
    total_bytes += chunk.length();
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("target_chunk_size", chunk_size);

    uint64_t dedup_bytes = 0;
    uint64_t dedup_objects = chunk_statistics.size();
    for (auto& j : chunk_statistics) {
      dedup_bytes += j.second.second;
    }
    double dedup_ratio = 1.0 - ((double)dedup_bytes / (double)total_bytes);
    f->dump_unsigned("dedup_bytes", dedup_bytes);
    //f->dump_unsigned("original_bytes", total_bytes);
    f->dump_float("dedup_ratio", dedup_ratio);

    uint64_t avg = total_bytes / dedup_objects;
    uint64_t sqsum = 0;
    for (auto& j : chunk_statistics) {
      sqsum += (avg - j.second.second) * (avg - j.second.second);
    }
    uint64_t stddev = sqrt(sqsum / dedup_objects);
    f->dump_unsigned("chunk_size_average", avg);
    f->dump_unsigned("chunk_size_stddev", stddev);
  }
};

map<uint64_t, EstimateResult> dedup_estimates;  // chunk size -> result

using namespace librados;
unsigned default_op_size = 1 << 26;
unsigned default_max_thread = 2;
int32_t default_report_period = 2;
ceph::mutex glock = ceph::make_mutex("glock");

void usage()
{
  cout << " usage: [--op <estimate|chunk_scrub|add_chunk_ref|get_chunk_ref>] [--pool <pool_name> ] " << std::endl;
  cout << "   --object <object_name> " << std::endl;
  cout << "   --chunk-size <size> chunk-size (byte) " << std::endl;
  cout << "   --chunk-algorithm <fixed|rabin|fastcdc> " << std::endl;
  cout << "   --fingerprint-algorithm <sha1|sha256|sha512> " << std::endl;
  cout << "   --chunk-pool <pool name> " << std::endl;
  cout << "   --max-thread <threads> " << std::endl;
  cout << "   --report-perioid <seconds> " << std::endl;
  cout << "   --max-seconds <seconds>" << std::endl;
  cout << "   --max-read-size <bytes> " << std::endl;
  exit(1);
}

template <typename I, typename T>
static int rados_sistrtoll(I &i, T *val) {
  std::string err;
  *val = strict_iecstrtoll(i->second.c_str(), &err);
  if (err != "") {
    cerr << "Invalid value for " << i->first << ": " << err << std::endl;
    return -EINVAL;
  } else {
    return 0;
  }
}

class EstimateDedupRatio;
class ChunkScrub;
class CrawlerThread : public Thread
{
  IoCtx io_ctx;
  int n;
  int m;
  ObjectCursor begin;
  ObjectCursor end;
  ceph::mutex m_lock = ceph::make_mutex("CrawlerThread::Locker");
  ceph::condition_variable m_cond;
  int32_t report_period;
  bool m_stop = false;
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;
  uint64_t max_read_size = 0;
  bool debug = false;
#define COND_WAIT_INTERVAL 10

public:
  CrawlerThread(IoCtx& io_ctx, int n, int m,
		ObjectCursor begin, ObjectCursor end, int32_t report_period,
		uint64_t num_objects, uint64_t max_read_size = default_op_size):
    io_ctx(io_ctx), n(n), m(m), begin(begin), end(end), 
    report_period(report_period), total_objects(num_objects), max_read_size(max_read_size)
  {}
  void signal(int signum) {
    std::lock_guard l{m_lock};
    m_stop = true;
    m_cond.notify_all();
  }
  virtual void print_status(Formatter *f, ostream &out) {}
  uint64_t get_examined_objects() { return examined_objects; }
  uint64_t get_examined_bytes() { return examined_bytes; }
  uint64_t get_total_bytes() { return total_bytes; }
  uint64_t get_total_objects() { return total_objects; }
  friend class EstimateDedupRatio;
  friend class ChunkScrub;
};

class EstimateDedupRatio : public CrawlerThread
{
  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  uint64_t max_seconds;

public:
  EstimateDedupRatio(
    IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end,
    string chunk_algo, string fp_algo, uint64_t chunk_size, int32_t report_period,
    uint64_t num_objects, uint64_t max_read_size,
    uint64_t max_seconds):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects,
		  max_read_size),
    chunk_algo(chunk_algo),
    fp_algo(fp_algo),
    chunk_size(chunk_size),
    max_seconds(max_seconds) {
  }

  void* entry() {
    estimate_dedup_ratio();
    return NULL;
  }
  void estimate_dedup_ratio();
};

class ChunkScrub: public CrawlerThread
{
  IoCtx chunk_io_ctx;
  int fixed_objects = 0;

public:
  ChunkScrub(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, 
	     IoCtx& chunk_io_ctx, int32_t report_period, uint64_t num_objects):
    CrawlerThread(io_ctx, n, m, begin, end, report_period, num_objects), chunk_io_ctx(chunk_io_ctx)
    { }
  void* entry() {
    chunk_scrub_common();
    return NULL;
  }
  void chunk_scrub_common();
  int get_fixed_objects() { return fixed_objects; }
  void print_status(Formatter *f, ostream &out);
};

vector<std::unique_ptr<CrawlerThread>> estimate_threads;

static void print_dedup_estimate(std::string chunk_algo, bool debug = false)
{
  /*
  uint64_t total_bytes = 0;
  uint64_t total_objects = 0;
  */
  uint64_t examined_objects = 0;
  uint64_t examined_bytes = 0;

  for (auto &et : estimate_threads) {
    examined_objects += et->get_examined_objects();
    examined_bytes += et->get_examined_bytes();
  }

  auto f = Formatter::create("json-pretty");
  f->open_object_section("results");
  f->dump_string("chunk_algo", chunk_algo);
  f->open_array_section("chunk_sizes");
  for (auto& i : dedup_estimates) {
    f->dump_object("chunker", i.second);
  }
  f->close_section();

  f->open_object_section("summary");
  f->dump_unsigned("examined_objects", examined_objects);
  f->dump_unsigned("examined_bytes", examined_bytes);
  /*
  f->dump_unsigned("total_objects", total_objects);
  f->dump_unsigned("total_bytes", total_bytes);
  f->dump_float("examined_ratio", (float)examined_bytes / (float)total_bytes);
  */
  f->close_section();
  f->close_section();
  f->flush(cout);
}

static void handle_signal(int signum) 
{
  std::lock_guard l{glock};
  for (auto &p : estimate_threads) {
    p->signal(signum);
  }
}

void EstimateDedupRatio::estimate_dedup_ratio()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;

  io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  utime_t end;
  if (max_seconds) {
    end = ceph_clock_now();
    end += max_seconds;
  }

  ObjectCursor c(shard_start);
  while (c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    unsigned op_size = max_read_size;

    for (const auto & i : result) {
      const auto &oid = i.oid;

      if (max_seconds) {
	utime_t now = ceph_clock_now();
	if (now > end) {
	  m_stop = true;
	}
      }
      if (m_stop) {
	return;
      }

      // read entire object
      bufferlist bl;
      uint64_t offset = 0;
      while (true) {
	bufferlist t;
	int ret = io_ctx.read(oid, t, op_size, offset);
	if (ret <= 0) {
	  break;
	}
	offset += ret;
	bl.claim_append(t);
      }
      examined_objects++;
      examined_bytes += bl.length();

      // do the chunking
      for (auto& i : dedup_estimates) {
	vector<pair<uint64_t, uint64_t>> chunks;
	i.second.cdc->calc_chunks(bl, &chunks);
	for (auto& p : chunks) {
	  bufferlist chunk;
	  chunk.substr_of(bl, p.first, p.second);
	  i.second.add_chunk(chunk, fp_algo);
	  if (debug) {
	    cout << " " << oid <<  " " << p.first << "~" << p.second << std::endl;
	  }
	}
      }
    }
  }
}

void ChunkScrub::chunk_scrub_common()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;
  int ret;
  utime_t cur_time = ceph_clock_now();
  Rados rados;

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     return;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     return;
  }

  chunk_io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  ObjectCursor c(shard_start);
  while(c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = chunk_io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    for (const auto & i : result) {
      std::unique_lock l{m_lock};
      if (m_stop) {
	Formatter *formatter = Formatter::create("json-pretty");
	print_status(formatter, cout);
	delete formatter;
	return;
      }
      auto oid = i.oid;
      set<hobject_t> refs;
      set<hobject_t> real_refs;
      ret = cls_chunk_refcount_read(chunk_io_ctx, oid, &refs);
      if (ret < 0) {
	continue;
      }

      for (auto pp : refs) {
	IoCtx target_io_ctx;
	ret = rados.ioctx_create2(pp.pool, target_io_ctx);
	if (ret < 0) {
	  cerr << "error opening pool "
	       << pp.pool << ": "
	       << cpp_strerror(ret) << std::endl;
	  continue;
	}

	ret = cls_chunk_has_chunk(target_io_ctx, pp.oid.name, oid);
	if (ret != -ENOENT) {
	  real_refs.insert(pp);
	} 
      }

      if (refs.size() != real_refs.size()) {
	ObjectWriteOperation op;
	cls_chunk_refcount_set(op, real_refs);
	ret = chunk_io_ctx.operate(oid, &op);
	if (ret < 0) {
	  continue;
	}
	fixed_objects++;
      }
      examined_objects++;
      m_cond.wait_for(l, std::chrono::nanoseconds(COND_WAIT_INTERVAL));
      if (cur_time + utime_t(report_period, 0) < ceph_clock_now()) {
	Formatter *formatter = Formatter::create("json-pretty");
	print_status(formatter, cout);
	delete formatter;
	cur_time = ceph_clock_now();
      }
    }
  }
}

void ChunkScrub::print_status(Formatter *f, ostream &out)
{
  if (f) {
    f->open_array_section("chunk_scrub");
    f->dump_string("PID", stringify(get_pid()));
    f->open_object_section("Status");
    f->dump_string("Total object", stringify(total_objects));
    f->dump_string("Examined objectes", stringify(examined_objects));
    f->dump_string("Fixed objectes", stringify(fixed_objects));
    f->close_section();
    f->flush(out);
    cout << std::endl;
  }
}

int estimate_dedup_ratio(const std::map < std::string, std::string > &opts,
			  std::vector<const char*> &nargs)
{
  Rados rados;
  IoCtx io_ctx;
  std::string chunk_algo;
  string fp_algo = "sha1";
  string pool_name;
  uint64_t chunk_size = 0;
  uint64_t min_chunk_size = 8192;
  uint64_t max_chunk_size = 4*1024*1024;
  unsigned max_thread = default_max_thread;
  uint32_t report_period = default_report_period;
  uint64_t max_read_size = default_op_size;
  uint64_t max_seconds = 0;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  bool debug = false;
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s; 
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;

  i = opts.find("pool");
  if (i != opts.end()) {
    pool_name = i->second.c_str();
  }
  i = opts.find("chunk-algorithm");
  if (i != opts.end()) {
    chunk_algo = i->second.c_str();
    if (!CDC::create(chunk_algo, 12)) {
      cerr << "unrecognized chunk-algorithm " << chunk_algo << std::endl;
      exit(1);
    }
  } else {
    cerr << "must specify chunk-algorithm" << std::endl;
    exit(1);
  }

  i = opts.find("fingerprint-algorithm");
  if (i != opts.end()) {
    fp_algo = i->second.c_str();
    if (fp_algo != "sha1"
	&& fp_algo != "sha256" && fp_algo != "sha512") {
      cerr << "unrecognized fingerprint-algorithm " << fp_algo << std::endl;
      exit(1);
    }
  }

  i = opts.find("chunk-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &chunk_size)) {
      return -EINVAL;
    }
  }

  i = opts.find("min-chunk-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &min_chunk_size)) {
      return -EINVAL;
    }
  }
  i = opts.find("max-chunk-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &max_chunk_size)) {
      return -EINVAL;
    }
  }

  i = opts.find("max-thread");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &max_thread)) {
      return -EINVAL;
    }
  } 

  i = opts.find("report-period");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &report_period)) {
      return -EINVAL;
    }
  }
  i = opts.find("max-seconds");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &max_seconds)) {
      return -EINVAL;
    }
  }
  i = opts.find("max-read-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &max_read_size)) {
      return -EINVAL;
    }
  } 
  i = opts.find("debug");
  if (i != opts.end()) {
    debug = true;
  }

  i = opts.find("pgid");
  boost::optional<pg_t> pgid(i != opts.end(), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  if (pool_name.empty()) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    exit(1);
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  // set up chunkers
  if (chunk_size) {
    dedup_estimates.emplace(std::piecewise_construct,
			    std::forward_as_tuple(chunk_size),
			    std::forward_as_tuple(chunk_algo, cbits(chunk_size)-1));
  } else {
    for (size_t cs = min_chunk_size; cs <= max_chunk_size; cs *= 2) {
      dedup_estimates.emplace(std::piecewise_construct,
			      std::forward_as_tuple(cs),
			      std::forward_as_tuple(chunk_algo, cbits(cs)-1));
    }
  }

  glock.lock();
  begin = io_ctx.object_list_begin();
  end = io_ctx.object_list_end();
  pool_names.push_back(pool_name);
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[pool_name];

  for (unsigned i = 0; i < max_thread; i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new EstimateDedupRatio(io_ctx, i, max_thread, begin, end,
			     chunk_algo, fp_algo, chunk_size,
			     report_period, s.num_objects, max_read_size,
			     max_seconds));
    ptr->create("estimate_thread");
    estimate_threads.push_back(move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  print_dedup_estimate(chunk_algo, debug);

 out:
  return (ret < 0) ? 1 : 0;
}

static void print_chunk_scrub()
{
  uint64_t total_objects = 0;
  uint64_t examined_objects = 0;
  int fixed_objects = 0;

  for (auto &et : estimate_threads) {
    if (!total_objects) {
      total_objects = et->get_total_objects();
    }
    examined_objects += et->get_examined_objects();
    ChunkScrub *ptr = static_cast<ChunkScrub*>(et.get());
    fixed_objects += ptr->get_fixed_objects();
  }

  cout << " Total object : " << total_objects << std::endl;
  cout << " Examined object : " << examined_objects << std::endl;
  cout << " Fixed object : " << fixed_objects << std::endl;
}

int chunk_scrub_common(const std::map < std::string, std::string > &opts,
			  std::vector<const char*> &nargs)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, target_object_name;
  string chunk_pool_name, op_name;
  int ret;
  unsigned max_thread = default_max_thread;
  std::map<std::string, std::string>::const_iterator i;
  uint32_t report_period = default_report_period;
  ObjectCursor begin;
  ObjectCursor end;
  librados::pool_stat_t s; 
  list<string> pool_names;
  map<string, librados::pool_stat_t> stats;

  i = opts.find("op_name");
  if (i != opts.end()) {
    op_name= i->second.c_str();
  } else {
    cerr << "must specify op" << std::endl;
    exit(1);
  }

  i = opts.find("chunk-pool");
  if (i != opts.end()) {
    chunk_pool_name = i->second.c_str();
  } else {
    cerr << "must specify pool" << std::endl;
    exit(1);
  }
  i = opts.find("max-thread");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &max_thread)) {
      return -EINVAL;
    }
  } 
  i = opts.find("report-period");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &report_period)) {
      return -EINVAL;
    }
  } 
  i = opts.find("pgid");
  boost::optional<pg_t> pgid(i != opts.end(), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << chunk_pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (op_name == "add-chunk-ref") {
    string target_object_name;
    uint64_t pool_id;
    i = opts.find("object");
    if (i != opts.end()) {
      object_name = i->second.c_str();
    } else {
      cerr << "must specify object" << std::endl;
      exit(1);
    }
    i = opts.find("target-ref");
    if (i != opts.end()) {
      target_object_name = i->second.c_str();
    } else {
      cerr << "must specify target ref" << std::endl;
      exit(1);
    }
    i = opts.find("target-ref-pool-id");
    if (i != opts.end()) {
      if (rados_sistrtoll(i, &pool_id)) {
	return -EINVAL;
      }
    } else {
      cerr << "must specify target-ref-pool-id" << std::endl;
      exit(1);
    }

    set<hobject_t> refs;
    ret = cls_chunk_refcount_read(chunk_io_ctx, object_name, &refs);
    if (ret < 0) {
      cerr << " cls_chunk_refcount_read fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    for (auto p : refs) {
      cout << " " << p.oid.name << " ";
    }

    uint32_t hash;
    ret = chunk_io_ctx.get_object_hash_position2(object_name, &hash);
    if (ret < 0) {
      return ret;
    }
    hobject_t oid(sobject_t(target_object_name, CEPH_NOSNAP), "", hash, pool_id, "");
    refs.insert(oid);

    ObjectWriteOperation op;
    cls_chunk_refcount_set(op, refs);
    ret = chunk_io_ctx.operate(object_name, &op);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    }

    return ret;

  } else if (op_name == "get-chunk-ref") {
    i = opts.find("object");
    if (i != opts.end()) {
      object_name = i->second.c_str();
    } else {
      cerr << "must specify object" << std::endl;
      exit(1);
    }
    set<hobject_t> refs;
    cout << " refs: " << std::endl;
    ret = cls_chunk_refcount_read(chunk_io_ctx, object_name, &refs);
    for (auto p : refs) {
      cout << " " << p.oid.name << " ";
    }
    cout << std::endl;
    return ret;
  }
  
  glock.lock();
  begin = chunk_io_ctx.object_list_begin();
  end = chunk_io_ctx.object_list_end();
  pool_names.push_back(chunk_pool_name);
  ret = rados.get_pool_stats(pool_names, stats);
  if (ret < 0) {
    cerr << "error fetching pool stats: " << cpp_strerror(ret) << std::endl;
    glock.unlock();
    return ret;
  }
  if (stats.find(chunk_pool_name) == stats.end()) {
    cerr << "stats can not find pool name: " << chunk_pool_name << std::endl;
    glock.unlock();
    return ret;
  }
  s = stats[chunk_pool_name];

  for (unsigned i = 0; i < max_thread; i++) {
    std::unique_ptr<CrawlerThread> ptr (
      new ChunkScrub(io_ctx, i, max_thread, begin, end, chunk_io_ctx,
		     report_period, s.num_objects));
    ptr->create("estimate_thread");
    estimate_threads.push_back(move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  print_chunk_scrub();

out:
  return (ret < 0) ? 1 : 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  std::string fn;
  string op_name;

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);
  std::map < std::string, std::string > opts;
  std::string val;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--op", (char*)NULL)) {
      opts["op_name"] = val;
      op_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--pool", (char*)NULL)) {
      opts["pool"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--object", (char*)NULL)) {
      opts["object"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-algorithm", (char*)NULL)) {
      opts["chunk-algorithm"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-size", (char*)NULL)) {
      opts["chunk-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--fingerprint-algorithm", (char*)NULL)) {
      opts["fingerprint-algorithm"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-pool", (char*)NULL)) {
      opts["chunk-pool"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-ref", (char*)NULL)) {
      opts["target-ref"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-ref-pool-id", (char*)NULL)) {
      opts["target-ref-pool-id"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-thread", (char*)NULL)) {
      opts["max-thread"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--report-period", (char*)NULL)) {
      opts["report-period"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-read-size", (char*)NULL)) {
      opts["max-seconds"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-seconds", (char*)NULL)) {
      opts["max-seconds"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-chunk-size", (char*)NULL)) {
      opts["min-chunk-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-chunk-size", (char*)NULL)) {
      opts["max-chunk-size"] = val;
    } else if (ceph_argparse_flag(args, i, "--debug", (char*)NULL)) {
      opts["debug"] = "true";
    } else {
      if (val[0] == '-') {
	cerr << "unrecognized option " << val << std::endl;
	exit(1);
      }
      ++i;
    }
  }

  if (op_name == "estimate") {
    return estimate_dedup_ratio(opts, args);
  } else if (op_name == "chunk-scrub") {
    return chunk_scrub_common(opts, args);
  } else if (op_name == "add-chunk-ref") {
    return chunk_scrub_common(opts, args);
  } else if (op_name == "get-chunk-ref") {
    return chunk_scrub_common(opts, args);
  } else {
    usage();
    exit(0);
  }

  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();
  
  return 0;
}
