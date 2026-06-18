// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OBJ_BENCHER_H
#define CEPH_OBJ_BENCHER_H

#include "common/ceph_context.h"
#include "common/Formatter.h"
#include "include/utime.h"
#include "ceph_time.h"

#include <cfloat>
#include <chrono>
#include <iosfwd>
#include <string>

using ceph::mono_clock;

struct bench_interval_data {
  double min_bandwidth = DBL_MAX;
  double max_bandwidth = 0;
  double avg_bandwidth = 0;
  int bandwidth_cycles = 0;
  double bandwidth_diff_sum = 0;
  int min_iops = INT_MAX;
  int max_iops = 0;
  double avg_iops = 0;
  int iops_cycles = 0;
  double iops_diff_sum = 0;
};

struct bench_data {
  bool done; //is the benchmark is done
  uint64_t object_size; //the size of the objects
  uint64_t op_size;     // the size of the read/write ops
  bool hints;
  // same as object_size for write tests
  int in_flight; //number of reads/writes being waited on
  int started;
  int finished;
  double min_latency;
  double max_latency;
  double avg_latency;
  struct bench_interval_data idata; // data that is updated by time intervals and not by events
  double latency_diff_sum;
  std::chrono::duration<double> cur_latency; //latency of last completed transaction - in seconds by default
  mono_time start_time; //start time for benchmark - use the monotonic clock as we'll measure the passage of time
  char *object_contents; //pointer to the contents written to each object
};

const int OP_WRITE     = 1;
const int OP_SEQ_READ  = 2;
const int OP_RAND_READ = 3;
const int OP_ROLLBACK  = 4;

// Object is composed of <oid,namespace>
typedef std::pair<std::string, std::string> Object;

class ObjBencher {
  bool show_time;
  Formatter *formatter = NULL;
  std::ostream *outstream = NULL;
public:
  CephContext *cct;
protected:
  ceph::mutex lock = ceph::make_mutex("ObjBencher::lock");

  static void *status_printer(void *bencher);

  struct bench_data data;

  int fetch_bench_metadata(const std::string& metadata_file, uint64_t* op_size,
			   uint64_t* object_size, int* num_ops, int* num_objects, int* prev_pid);

  int write_bench(int secondsToRun, int concurrentios, const std::string& run_name_meta, unsigned max_objects, int prev_pid);
  int seq_read_bench(int secondsToRun, int num_ops, int num_objects, int concurrentios, int writePid, bool no_verify=false);
  int rand_read_bench(int secondsToRun, int num_ops, int num_objects, int concurrentios, int writePid, bool no_verify=false);
  int rollback_bench(int secondsToRun, int num_ops, int num_objects, int concurrentios, int prev_pid);

  int clean_up(int num_objects, int prevPid, int concurrentios);
  bool more_objects_matching_prefix(const std::string& prefix, std::list<Object>* name);

  virtual int completions_init(int concurrentios) = 0;
  virtual void completions_done() = 0;

  virtual int create_completion(int i, void (*cb)(void *, void*), void *arg) = 0;
  virtual void release_completion(int slot) = 0;

  virtual bool completion_is_done(int slot) = 0;
  virtual int completion_wait(int slot) = 0;
  virtual int completion_ret(int slot) = 0;

  virtual int aio_read(const std::string& oid, int slot, bufferlist *pbl, size_t len, size_t offset) = 0;
  virtual int aio_write(const std::string& oid, int slot, bufferlist& bl, size_t len, size_t offset) = 0;
  virtual int aio_remove(const std::string& oid, int slot) = 0;
  virtual int sync_read(const std::string& oid, bufferlist& bl, size_t len) = 0;
  virtual int sync_write(const std::string& oid, bufferlist& bl, size_t len) = 0;
  virtual int sync_remove(const std::string& oid) = 0;
  virtual int aio_rollback(const std::string& oid, int slot) = 0;

  virtual bool get_objects(std::list< std::pair<std::string, std::string> >* objects, int num) = 0;
  virtual void set_namespace(const std::string&) {}

  std::ostream& out(std::ostream& os);
  std::ostream& out(std::ostream& os, utime_t& t);
public:
  explicit ObjBencher(CephContext *cct_) : show_time(false), cct(cct_), data() {}
  virtual ~ObjBencher() {}
  int aio_bench(
    int operation, int secondsToRun,
    int concurrentios, uint64_t op_size, uint64_t object_size, unsigned max_objects,
    bool cleanup, bool hints, const std::string& run_name, bool reuse_bench, bool no_verify=false);
  int clean_up(const std::string& prefix, int concurrentios, const std::string& run_name);

  void set_show_time(bool dt) {
    show_time = dt;
  }
  void set_formatter(Formatter *f) {
    formatter = f;
  }
  void set_outstream(std::ostream& os) {
    outstream = &os;
  }
  int clean_up_slow(const std::string& prefix, int concurrentios);
};

const std::string BENCH_LASTRUN_METADATA = "benchmark_last_metadata";
const std::string BENCH_PREFIX = "benchmark_data";
const std::string BENCH_OBJ_NAME = BENCH_PREFIX + "_%s_%d_object%d";

namespace ceph::bench {
  inline std::string generate_object_prefix_nopid(const std::string &base_prefix,
                                                  const std::string &hostname) {
    std::string name;
    name.reserve(base_prefix.size() + 1 + hostname.size());
    name.append(base_prefix);
    name.append("_");
    name.append(hostname);
    return name;
  }
  inline std::string generate_object_prefix_nopid(const std::string &hostname) {
    return ceph::bench::generate_object_prefix_nopid(BENCH_PREFIX, hostname);
  }

  inline std::string generate_object_prefix(const std::string &base_prefix,
                                            const std::string &hostname, int pid) {
    std::string name;
    name.reserve(base_prefix.size() + 1 + hostname.size() + 11);
    name.append(base_prefix);
    name.append("_");
    name.append(hostname);
    name.append("_");
    name.append(std::to_string(pid));
    return name;
  }
  inline std::string generate_object_prefix(const std::string &hostname, int pid) {
    return ceph::bench::generate_object_prefix(BENCH_PREFIX, hostname, pid);
  }

  inline std::string generate_object_name_fast(const std::string &base_prefix,
                                               const std::string &hostname,
                                               int pid, int index) {
    std::string name;
    name.reserve(base_prefix.size() + 1 + hostname.size() + 1 + 11 + 7 + 20);
    name.append(base_prefix);
    name.append("_");
    name.append(hostname);
    name.append("_");
    name.append(std::to_string(pid));
    name.append("_object");
    name.append(std::to_string(index));
    return name;
  }
  inline std::string generate_object_name_fast(const std::string &hostname,
                                               int pid, int index) {
    return ceph::bench::generate_object_name_fast(BENCH_PREFIX, hostname, pid, index);
  }

  inline std::string get_local_hostname() {
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) < 0) {
      return "localhost";
    }
    return std::string(hostname);
  }

  inline void decode_bench_metadata(const bufferlist &bl, uint64_t *object_size,
                                    int *num_ops, int *prev_pid, uint64_t *op_size) {
    using ceph::decode;
    auto p = bl.cbegin();
    decode(*object_size, p);
    decode(*num_ops, p);
    decode(*prev_pid, p);
    if (!p.end()) {
      decode(*op_size, p);
    } else {
      *op_size = *object_size;
    }
  }
  inline void encode_bench_metadata(bufferlist *bl, uint64_t object_size,
                                    int num_ops, int prev_pid, uint64_t op_size) {
    using ceph::encode;
    encode(object_size, *bl);
    encode(num_ops, *bl);
    encode(prev_pid, *bl);
    encode(op_size, *bl);
  }
} // namespace ceph::bench

#endif
