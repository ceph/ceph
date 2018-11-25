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

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/obj_bencher.h"

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

using namespace librados;
unsigned default_op_size = 1 << 22;
unsigned default_max_thread = 2;
int32_t default_report_period = 2;
map< string, pair <uint64_t, uint64_t> > chunk_statistics; // < key, <count, chunk_size> >
Mutex glock("chunk_statistics::Locker");

void usage()
{
  cout << " usage: [--op <estimate|chunk_scrub|add_chunk_ref|get_chunk_ref>] [--pool <pool_name> ] " << std::endl;
  cout << "   --object <object_name> " << std::endl;
  cout << "   --chunk-size <size> chunk-size (byte) " << std::endl;
  cout << "   --chunk-algorithm <fixed> " << std::endl;
  cout << "   --fingerprint-algorithm <sha1> " << std::endl;
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

static void print_dedup_estimate()
{
  uint64_t total_size = 0;
  uint64_t dedup_size = 0;
  for (auto p : chunk_statistics) {
    cout << " -- " << std::endl;
    cout << " key: " << p.first << std::endl;
    cout << " count: " << p.second.first << std::endl;
    cout << " chunk_size: " << p.second.second << std::endl;
    cout << " -- " << std::endl;
    total_size += p.second.second * p.second.first;
    dedup_size += p.second.second;
  }

  cout << " result: " << total_size << " / " << dedup_size << " (total size / dedup size) " << std::endl;
}

class EstimateThread : public Thread 
{
  void* entry() override;
  IoCtx io_ctx;
  int n;
  int m;
  ObjectCursor begin;
  ObjectCursor end;
  string chunk_algo;
  string fp_algo;
  uint64_t chunk_size;
  IoCtx chunk_io_ctx;
  map< string, pair <uint64_t, uint64_t> > local_chunk_statistics; // < key, <count, chunk_size> >
  typedef void (EstimateThread::*entry_func)();
  entry_func func;
  Mutex m_lock;
  Cond m_cond;
  int32_t timeout;
  bool m_stop = false;
#define COND_WAIT_INTERVAL 10

  public:
  EstimateThread(IoCtx& io_ctx, int n, int m, ObjectCursor begin, ObjectCursor end, 
		string chunk_algo, string fp_algo, uint64_t chunk_size, IoCtx chunk_io_ctx, int32_t timeout):
    io_ctx(io_ctx), n(n), m(m), begin(begin), end(end), chunk_algo(chunk_algo), fp_algo(fp_algo),
    chunk_size(chunk_size), chunk_io_ctx(chunk_io_ctx), m_lock("EstimateThread::Locker"),
    timeout(timeout) {}

  void set_estimate_dedup_ratio() {
    func = &EstimateThread::estimate_dedup_ratio;
  }
  void set_chunk_scrub_common() {
    func = &EstimateThread::chunk_scrub_common;
  }
  void estimate_dedup_ratio();
  void chunk_scrub_common();
  void signal(int signum) {
    Mutex::Locker l(m_lock);
    m_stop = true;
    m_cond.Signal();
  }
  void estimate_print_status(Formatter *f, ostream &out);
};

vector<std::unique_ptr<EstimateThread>> estimate_threads;

static void handle_signal(int signum) 
{
  Mutex::Locker l(glock);
  for (auto &p : estimate_threads) {
    p->signal(signum);
  }
}

void* EstimateThread::entry() {
  if (func) {
    (this->*func)();
  }
  return NULL;
}

void EstimateThread::estimate_print_status(Formatter *f, ostream &out)
{
  if (f) {
    f->open_array_section("estimate_dedup_ratio");
    f->dump_string("PID", stringify(get_pid()));
    for (auto p : local_chunk_statistics) {
      f->open_object_section("fingerprint object");
      f->dump_string("fingperint", p.first);
      f->dump_string("count", stringify(p.second.first));
      f->dump_string("chunk_size", stringify(p.second.second));
    }
    f->close_section();
    f->flush(out);
    cout << std::endl;
  }
}

void EstimateThread::estimate_dedup_ratio()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;
  unsigned op_size = default_op_size;
  int ret;
  utime_t cur_time = ceph_clock_now();

  io_ctx.object_list_slice(
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
    int r = io_ctx.object_list(c, shard_end, 12, {}, &result, &c);
    if (r < 0 ){
      cerr << "error object_list : " << cpp_strerror(r) << std::endl;
      return;
    }

    for (const auto & i : result) {
      const auto &oid = i.oid;
      uint64_t offset = 0;
      while (true) {
	Mutex::Locker l(m_lock);
	if (m_stop) {
	  Formatter *formatter = Formatter::create("json-pretty");
	  estimate_print_status(formatter, cout);
	  delete formatter;
	  return;
	}
	bufferlist outdata;
	ret = io_ctx.read(oid, outdata, op_size, offset);
	if (ret <= 0) {
	  break;
	}

	if (chunk_algo == "fixed") {
	  if (fp_algo == "sha1") {
	    uint64_t c_offset = 0;
	    while (c_offset < outdata.length()) {
	      bufferlist chunk;
	      if (outdata.length() - c_offset > chunk_size) {
		bufferptr bptr(chunk_size);
		chunk.push_back(std::move(bptr));
		chunk.copy_in(0, chunk_size, outdata.c_str());	  
	      } else {
		bufferptr bptr(outdata.length() - c_offset);
		chunk.push_back(std::move(bptr));
		chunk.copy_in(0, outdata.length() - c_offset, outdata.c_str());	  
	      }
	      sha1_digest_t sha1_val = chunk.sha1();
	      string fp = sha1_val.to_str();
	      auto p = local_chunk_statistics.find(fp);
	      if (p != local_chunk_statistics.end()) {
		uint64_t count = p->second.first;
		count++;
		local_chunk_statistics[fp] = make_pair(count, chunk.length());
	      } else {
		local_chunk_statistics[fp] = make_pair(1, chunk.length());
	      }
	      c_offset = c_offset + chunk_size;
	    }
	  } else {
	    ceph_assert(0 == "no support fingerprint algorithm"); 
	  }
	} else {
	  ceph_assert(0 == "no support chunk algorithm"); 
	}
	
	if (outdata.length() < op_size) {
	  break;
	}
	offset += outdata.length();
	m_cond.WaitInterval(m_lock,utime_t(0, COND_WAIT_INTERVAL));
	if (cur_time + utime_t(timeout, 0) < ceph_clock_now()) {
	  Formatter *formatter = Formatter::create("json-pretty");
	  estimate_print_status(formatter, cout);
	  delete formatter;
	  cur_time = ceph_clock_now();
	}
      }
    }
  }

  Mutex::Locker l(glock);
  chunk_statistics.insert(local_chunk_statistics.begin(), local_chunk_statistics.end());
}

void EstimateThread::chunk_scrub_common()
{
  ObjectCursor shard_start;
  ObjectCursor shard_end;
  int ret;
  int total_object = 0, comp_object = 0;
  utime_t cur_time = ceph_clock_now();

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
      Mutex::Locker l(m_lock);
      if (m_stop) {
	cout << " Completed object : " << comp_object << std::endl;
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
	ret = cls_chunk_has_chunk(io_ctx, pp.oid.name, oid);
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
      }
      comp_object++;
      m_cond.WaitInterval(m_lock,utime_t(0, COND_WAIT_INTERVAL));
      if (cur_time + utime_t(timeout, 0) < ceph_clock_now()) {
	cout << " Completed object : " << comp_object << std::endl;
	cur_time = ceph_clock_now();
      }
    }
    total_object++;
  }
  cout << " Total object : " << total_object << std::endl;
  cout << " Completed object : " << comp_object << std::endl;
}

int estimate_dedup_ratio(const std::map < std::string, std::string > &opts,
			  std::vector<const char*> &nargs)
{
  Rados rados;
  IoCtx io_ctx;
  std::string object_name;
  std::string chunk_algo;
  string fp_algo;
  string pool_name;
  uint64_t chunk_size = 0;
  unsigned max_thread = default_max_thread;
  uint32_t report_period = default_report_period;
  int ret;
  std::map<std::string, std::string>::const_iterator i;
  bool debug = false;

  i = opts.find("pool");
  if (i != opts.end()) {
    pool_name = i->second.c_str();
  }
  i = opts.find("object");
  if (i != opts.end()) {
    object_name = i->second.c_str();
  }
  i = opts.find("chunk-algorithm");
  if (i != opts.end()) {
    chunk_algo = i->second.c_str();
    if (chunk_algo != "fixed") {
      usage_exit();
    }
  } else {
    usage_exit();
  }

  i = opts.find("fingerprint-algorithm");
  if (i != opts.end()) {
    fp_algo = i->second.c_str();
    if (fp_algo != "sha1") {
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

  glock.Lock();
  for (unsigned i = 0; i < max_thread; i++) {
    ObjectCursor begin = io_ctx.object_list_begin();
    ObjectCursor end = io_ctx.object_list_end();
    std::unique_ptr<EstimateThread> ptr (new EstimateThread(io_ctx, i, max_thread, begin, end,
							    chunk_algo, fp_algo, chunk_size, IoCtx(), 
							    report_period));
    ptr->set_estimate_dedup_ratio();
    ptr->create("estimate_thread");
    estimate_threads.push_back(move(ptr));
  }
  glock.Unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

  if (debug) {
    print_dedup_estimate();
  }
 out:
  return (ret < 0) ? 1 : 0;
}

int chunk_scrub_common(const std::map < std::string, std::string > &opts,
			  std::vector<const char*> &nargs)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, target_object_name;
  string pool_name, chunk_pool_name, op_name;
  int ret;
  unsigned max_thread = default_max_thread;
  std::map<std::string, std::string>::const_iterator i;
  uint32_t report_period = default_report_period;

  i = opts.find("pool");
  if (i != opts.end()) {
    pool_name = i->second.c_str();
  } else {
    usage_exit();
  }
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
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << chunk_pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (op_name == "add_chunk_ref") {
    string target_object_name;
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
    hobject_t oid(sobject_t(target_object_name, CEPH_NOSNAP), "", hash, -1, "");
    refs.insert(oid);

    ObjectWriteOperation op;
    cls_chunk_refcount_set(op, refs);
    ret = chunk_io_ctx.operate(object_name, &op);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    }

    return ret;

  } else if (op_name == "get_chunk_ref") {
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
  
  glock.Lock();
  for (unsigned i = 0; i < max_thread; i++) {
    ObjectCursor begin = io_ctx.object_list_begin();
    ObjectCursor end = io_ctx.object_list_end();
    std::unique_ptr<EstimateThread> ptr (new EstimateThread(io_ctx, i, max_thread, begin, end,
							    "", "", 0, chunk_io_ctx,
							    report_period));
    ptr->set_chunk_scrub_common();
    ptr->create("estimate_thread");
    estimate_threads.push_back(move(ptr));
  }
  glock.Unlock();

  for (auto &p : estimate_threads) {
    p->join();
  }

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
    } else if (ceph_argparse_witharg(args, i, &val, "--max-thread", (char*)NULL)) {
      opts["max-thread"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--report-period", (char*)NULL)) {
      opts["report-period"] = val;
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
  } else if (op_name == "chunk_scrub") {
    return chunk_scrub_common(opts, args);
  } else if (op_name == "add_chunk_ref") {
    return chunk_scrub_common(opts, args);
  } else if (op_name == "get_chunk_ref") {
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
