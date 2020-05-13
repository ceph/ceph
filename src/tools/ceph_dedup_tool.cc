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

#include "tools/RadosDump.h"
#include "cls/cas/cls_cas_client.h"
#include "include/stringify.h"
#include "global/signal_handler.h"
#include "common/rabin.h"

using namespace librados;
unsigned default_op_size = 1 << 26;
unsigned default_max_thread = 2;
int32_t default_report_period = 2;
map< string, pair <uint64_t, uint64_t> > chunk_statistics; // < key, <count, chunk_size> >
ceph::mutex glock = ceph::make_mutex("chunk_statistics::Locker");

void usage()
{
  cout << " usage: [--op <estimate|chunk_scrub|add_chunk_ref|get_chunk_ref>] [--pool <pool_name> ] " << std::endl;
  cout << "   --object <object_name> " << std::endl;
  cout << "   --chunk-size <size> chunk-size (byte) " << std::endl;
  cout << "   --chunk-algorithm <fixed|rabin> " << std::endl;
  cout << "   --fingerprint-algorithm <sha1|sha256|sha512> " << std::endl;
  cout << "   --chunk-pool <pool name> " << std::endl;
  cout << "   --max-thread <threads> " << std::endl;
  cout << "   --report-perioid <seconds> " << std::endl;
  cout << "   --max-read-size <bytes> " << std::endl;
  exit(1);
}

[[noreturn]] static void usage_exit()
{
  usage();
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
class EstimateThread : public Thread 
{
  IoCtx io_ctx;
  int n;
  int m;
  ObjectCursor begin;
  ObjectCursor end;
  ceph::mutex m_lock = ceph::make_mutex("EstimateThread::Locker");
  ceph::condition_variable m_cond;
  int32_t timeout;
  bool m_stop = false;
  uint64_t total_bytes = 0;
  uint64_t examined_objects = 0;
  uint64_t total_objects;
  uint64_t max_read_size = 0;
  bool debug = false;
#define COND_WAIT_INTERVAL 10

public:
  EstimateThread(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, int32_t timeout,
		uint64_t num_objects, uint64_t max_read_size = default_op_size):
    io_ctx(io_ctx), n(n), m(m), begin(begin), end(end), 
    timeout(timeout), total_objects(num_objects), max_read_size(max_read_size)
  {}
  void signal(int signum) {
    std::lock_guard l{m_lock};
    m_stop = true;
    m_cond.notify_all();
  }
  virtual void print_status(Formatter *f, ostream &out) = 0;
  uint64_t get_examined_objects() { return examined_objects; }
  uint64_t get_total_bytes() { return total_bytes; }
  uint64_t get_total_objects() { return total_objects; }
  friend class EstimateDedupRatio;
  friend class ChunkScrub;
};

class EstimateDedupRatio : public EstimateThread
{
  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  map< string, pair <uint64_t, uint64_t> > local_chunk_statistics; // < key, <count, chunk_size> >
  RabinChunk rabin;

public:
  EstimateDedupRatio(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, 
		string chunk_algo, string fp_algo, uint64_t chunk_size, int32_t timeout,
		uint64_t num_objects, uint64_t max_read_size):
    EstimateThread(io_ctx, n, m, begin, end, timeout, num_objects, max_read_size), 
		chunk_algo(chunk_algo), fp_algo(fp_algo), chunk_size(chunk_size) { }

  void* entry() {
    estimate_dedup_ratio();
    return NULL;
  }
  void estimate_dedup_ratio();
  void print_status(Formatter *f, ostream &out);
  map< string, pair <uint64_t, uint64_t> > &get_chunk_statistics() { return local_chunk_statistics; }
  uint64_t fixed_chunk(string oid, uint64_t offset);
  uint64_t rabin_chunk(string oid, uint64_t offset);
  void add_chunk_fp_to_stat(bufferlist &chunk);
};

class ChunkScrub: public EstimateThread
{
  IoCtx chunk_io_ctx;
  int fixed_objects = 0;

public:
  ChunkScrub(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, 
	     IoCtx& chunk_io_ctx, int32_t timeout, uint64_t num_objects):
    EstimateThread(io_ctx, n, m, begin, end, timeout, num_objects), chunk_io_ctx(chunk_io_ctx)
    { }
  void* entry() {
    chunk_scrub_common();
    return NULL;
  }
  void chunk_scrub_common();
  int get_fixed_objects() { return fixed_objects; }
  void print_status(Formatter *f, ostream &out);
};

vector<std::unique_ptr<EstimateThread>> estimate_threads;

static void print_dedup_estimate(bool debug = false)
{
  uint64_t total_size = 0;
  uint64_t dedup_size = 0;
  uint64_t examined_objects = 0;
  uint64_t total_objects = 0;
  EstimateDedupRatio *ratio = NULL;
  for (auto &et : estimate_threads) {
    std::lock_guard l{glock};
    ratio = dynamic_cast<EstimateDedupRatio*>(et.get());
    assert(ratio);
    for (auto p : ratio->get_chunk_statistics()) {
      auto c = chunk_statistics.find(p.first);
      if (c != chunk_statistics.end()) {
	c->second.first += p.second.first;
      }	else {
	chunk_statistics.insert(p);
      }
    }
  }

  if (debug) {
    for (auto p : chunk_statistics) {
      cout << " -- " << std::endl;
      cout << " key: " << p.first << std::endl;
      cout << " count: " << p.second.first << std::endl;
      cout << " chunk_size: " << p.second.second << std::endl;
      dedup_size += p.second.second;
      cout << " -- " << std::endl;
    }
  } else {
    for (auto p : chunk_statistics) {
      dedup_size += p.second.second;
    }

  }

  for (auto &et : estimate_threads) {
    total_size += et->get_total_bytes();
    examined_objects += et->get_examined_objects();
    if (!total_objects) {
      total_objects = et->get_total_objects();
    }
  }

  cout << " result: " << total_size << " | " << dedup_size << " (total size | deduped size) " << std::endl;
  cout << " Dedup ratio: " << (100 - (double)(dedup_size)/total_size*100) << " % " << std::endl;
  cout << " Examined objects: " << examined_objects << std::endl;
  cout << " Total objects: " << total_objects << std::endl;
}

static void handle_signal(int signum) 
{
  std::lock_guard l{glock};
  for (auto &p : estimate_threads) {
    p->signal(signum);
  }
}

void EstimateDedupRatio::print_status(Formatter *f, ostream &out)
{
  if (f) {
    f->open_array_section("estimate_dedup_ratio");
    f->dump_string("PID", stringify(get_pid()));
    for (auto p : local_chunk_statistics) {
      f->open_object_section("fingerprint object");
      f->dump_string("fingerprint", p.first);
      f->dump_string("count", stringify(p.second.first));
      f->dump_string("chunk_size", stringify(p.second.second));
      f->close_section();
    }
    f->close_section();
    f->open_object_section("Status");
    f->dump_string("Total bytes", stringify(total_bytes));
    f->dump_string("Examined objectes", stringify(examined_objects));
    f->close_section();
    f->flush(out);
    cout << std::endl;
  }
}

void EstimateDedupRatio::estimate_dedup_ratio()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;
  utime_t cur_time = ceph_clock_now();

  io_ctx.object_list_slice(
    begin,
    end,
    n,
    m,
    &shard_start,
    &shard_end);

  ObjectCursor c(shard_start);
  while (c < shard_end)
  {
    std::vector<ObjectItem> result;
    int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    for (const auto & i : result) {
      const auto &oid = i.oid;
      uint64_t offset = 0;
      while (true) {
	std::unique_lock l{m_lock};
	if (m_stop) {
	  Formatter *formatter = Formatter::create("json-pretty");
	  print_status(formatter, cout);
	  delete formatter;
	  return;
	}

	uint64_t len;
	if (chunk_algo == "fixed") {
	  len = fixed_chunk(oid, offset);
	} else if (chunk_algo == "rabin") {
	  len = rabin_chunk(oid, offset);
	} else {
	  ceph_assert(0 == "no support chunk algorithm"); 
	}
	
	if (!len) {
	  break;
	}
	offset += len;
	m_cond.wait_for(l, std::chrono::nanoseconds(COND_WAIT_INTERVAL));
	if (cur_time + utime_t(timeout, 0) < ceph_clock_now()) {
	  Formatter *formatter = Formatter::create("json-pretty");
	  print_status(formatter, cout);
	  delete formatter;
	  cur_time = ceph_clock_now();
	}
      }
      examined_objects++;
    }
  }
}

uint64_t EstimateDedupRatio::fixed_chunk(string oid, uint64_t offset)
{
  unsigned op_size = max_read_size;
  int ret;
  bufferlist outdata;
  ret = io_ctx.read(oid, outdata, op_size, offset);
  if (ret <= 0) {
    return 0;
  }

  uint64_t c_offset = 0;
  while (c_offset < outdata.length()) {
    bufferlist chunk;
    if (outdata.length() - c_offset > chunk_size) {
      bufferptr bptr(chunk_size);
      chunk.push_back(std::move(bptr));
      chunk.begin().copy_in(chunk_size, outdata.c_str());
    } else {
      bufferptr bptr(outdata.length() - c_offset);
      chunk.push_back(std::move(bptr));
      chunk.begin().copy_in(outdata.length() - c_offset, outdata.c_str());
    }
    add_chunk_fp_to_stat(chunk);
    c_offset = c_offset + chunk_size;
  }

  if (outdata.length() < op_size) {
    return 0;
  }
  return outdata.length();
}

void EstimateDedupRatio::add_chunk_fp_to_stat(bufferlist &chunk) 
{
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
  } else if (chunk_algo == "rabin") {
    uint64_t hash = rabin.gen_rabin_hash(chunk.c_str(), 0, chunk.length());
    fp = to_string(hash);
  } else {
    ceph_assert(0 == "no support fingerperint algorithm"); 
  }

  auto p = local_chunk_statistics.find(fp);
  if (p != local_chunk_statistics.end()) {
    p->second.first++;
    if (p->second.second != chunk.length()) {
      cerr << "warning: hash collision on " << fp << ": was " << p->second.second
	   << " now " << chunk.length() << std::endl;
    }
  } else {
    local_chunk_statistics[fp] = make_pair(1, chunk.length());
  }
  total_bytes += chunk.length();
}

uint64_t EstimateDedupRatio::rabin_chunk(string oid, uint64_t offset)
{
  unsigned op_size = max_read_size;
  int ret;
  bufferlist outdata;
  ret = io_ctx.read(oid, outdata, op_size, offset);
  if (ret <= 0) {
    return 0;
  }

  vector<pair<uint64_t, uint64_t>> chunks;
  rabin.do_rabin_chunks(outdata, chunks, 0, 0); // use default value
  for (auto p : chunks) {
    bufferlist chunk;
    bufferptr c_data = buffer::create(p.second);
    c_data.zero();
    chunk.append(c_data);
    chunk.begin().copy_in(p.second, outdata.c_str() + p.first);
    add_chunk_fp_to_stat(chunk);
    cout << " oid: " << oid <<  " offset: " << p.first + offset << " length: " << p.second << std::endl;
  }

  if (outdata.length() < op_size) {
    return 0;
  }
  return outdata.length();
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
      if (cur_time + utime_t(timeout, 0) < ceph_clock_now()) {
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
  string fp_algo;
  string pool_name;
  uint64_t chunk_size = 0;
  unsigned max_thread = default_max_thread;
  uint32_t report_period = default_report_period;
  uint64_t max_read_size = default_op_size;
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
    if (chunk_algo != "fixed" && chunk_algo != "rabin") {
      usage_exit();
    }
  } else {
    usage_exit();
  }

  i = opts.find("fingerprint-algorithm");
  if (i != opts.end()) {
    fp_algo = i->second.c_str();
    if (fp_algo != "sha1" && fp_algo != "rabin"
	&& fp_algo != "sha256" && fp_algo != "sha512") {
      usage_exit();
    }
  } else {
    usage_exit();
  }

  i = opts.find("chunk-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &chunk_size)) {
      return -EINVAL;
    }
  } else {
    if (chunk_algo != "rabin") {
      usage_exit();
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
    usage_exit();
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
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
    std::unique_ptr<EstimateThread> ptr (new EstimateDedupRatio(io_ctx, i, max_thread, begin, end,
							    chunk_algo, fp_algo, chunk_size, 
							    report_period, s.num_objects, max_read_size));
    ptr->create("estimate_thread");
    estimate_threads.push_back(move(ptr));
  }
  glock.unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  print_dedup_estimate(debug);

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
    usage_exit();
  }

  i = opts.find("chunk-pool");
  if (i != opts.end()) {
    chunk_pool_name = i->second.c_str();
  } else {
    usage_exit();
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
      usage_exit();
    }
    i = opts.find("target-ref");
    if (i != opts.end()) {
      target_object_name = i->second.c_str();
    } else {
      usage_exit();
    }
    i = opts.find("target-ref-pool-id");
    if (i != opts.end()) {
      if (rados_sistrtoll(i, &pool_id)) {
	return -EINVAL;
      }
    } else {
      usage_exit();
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
      usage_exit();
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
    std::unique_ptr<EstimateThread> ptr (new ChunkScrub(io_ctx, i, max_thread, begin, end, chunk_io_ctx,
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
      opts["max-read-size"] = val;
    } else if (ceph_argparse_flag(args, i, "--debug", (char*)NULL)) {
      opts["debug"] = "true";
    } else {
      if (val[0] == '-')
        usage_exit();
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
