// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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

#include "common/config.h"
#include "common/Cond.h"
#include "common/ceph_context.h"
#include "common/Formatter.h"
#include <cfloat>

struct bench_interval_data {
  double min_bandwidth = DBL_MAX;
  double max_bandwidth = 0;
  int min_iops = INT_MAX;
  int max_iops = 0;
};

struct bench_history {
  vector<double> bandwidth;
  vector<double> latency;
  vector<long> iops;
};

struct bench_data {
  bool done; //is the benchmark is done
  size_t object_size; //the size of the objects
  size_t op_size;     // the size of the read/write ops
  // same as object_size for write tests
  int in_flight; //number of reads/writes being waited on
  int started;
  int finished;
  double min_latency;
  double max_latency;
  double avg_latency;
  struct bench_interval_data idata; // data that is updated by time intervals and not by events
  struct bench_history history; // data history, used to calculate stddev
  utime_t cur_latency; //latency of last completed transaction
  utime_t start_time; //start time for benchmark
  char *object_contents; //pointer to the contents written to each object
};

const int OP_WRITE     = 1;
const int OP_SEQ_READ  = 2;
const int OP_RAND_READ = 3;

// Object is composed of <oid,namespace>
typedef std::pair<std::string, std::string> Object;

class ObjBencher {
  bool show_time;
  Formatter *formatter = NULL;
  ostream *outstream = NULL;
public:
  CephContext *cct;
protected:
  Mutex lock;

  static void *status_printer(void *bencher);

  struct bench_data data;

  int fetch_bench_metadata(const std::string& metadata_file, size_t* op_size,
			   size_t* object_size, int* num_objects, int* prevPid);

  int write_bench(int secondsToRun, int concurrentios, const string& run_name_meta, unsigned max_objects);
  int seq_read_bench(int secondsToRun, int num_objects, int concurrentios, int writePid, bool no_verify=false);
  int rand_read_bench(int secondsToRun, int num_objects, int concurrentios, int writePid, bool no_verify=false);

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

  virtual bool get_objects(std::list< std::pair<std::string, std::string> >* objects, int num) = 0;
  virtual void set_namespace(const std::string&) {}

  ostream& out(ostream& os);
  ostream& out(ostream& os, utime_t& t);
public:
  explicit ObjBencher(CephContext *cct_) : show_time(false), cct(cct_), lock("ObjBencher::lock") {}
  virtual ~ObjBencher() {}
  int aio_bench(
    int operation, int secondsToRun,
    int concurrentios, size_t op_size, size_t object_size, unsigned max_objects,
    bool cleanup, const std::string& run_name, bool no_verify=false);
  int clean_up(const std::string& prefix, int concurrentios, const std::string& run_name);

  void set_show_time(bool dt) {
    show_time = dt;
  }
  void set_formatter(Formatter *f) {
    formatter = f;
  }
  void set_outstream(ostream& os) {
    outstream = &os;
  }
  int clean_up_slow(const std::string& prefix, int concurrentios);
};


#endif
