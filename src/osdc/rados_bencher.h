// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
 * Series of functions to test your rados installation. Notice
 * that this code is not terribly robust -- for instance, if you
 * try and bench on a pool you don't have permission to access
 * it will just loop forever.
 */

#ifndef CEPH_RADOS_BENCHER_H
#define CEPH_RADOS_BENCHER_H

#include "include/rados/librados.hpp"
#include "common/config.h"
#include "common/Cond.h"

struct bench_data {
  bool done; //is the benchmark is done
  int object_size; //the size of the objects
  int trans_size; //size of the write/read to perform
  // same as object_size for write tests
  int in_flight; //number of reads/writes being waited on
  int started;
  int finished;
  double min_latency;
  double max_latency;
  double avg_latency;
  utime_t cur_latency; //latency of last completed transaction
  utime_t start_time; //start time for benchmark
  char *object_contents; //pointer to the contents written to each object
};

const int OP_WRITE     = 1;
const int OP_SEQ_READ  = 2;
const int OP_RAND_READ = 3;

class RadosBencher {
  Mutex lock;
  librados::Rados& rados;
  librados::IoCtx& io_ctx;

  static void *status_printer(void *bencher);

  struct bench_data data;

  int write_bench(int secondsToRun, int concurrentios);
  int seq_read_bench(int secondsToRun, int concurrentios, int num_objects, int writePid);
public:
  RadosBencher(librados::Rados& _r, librados::IoCtx& _i) : lock("RadosBencher::lock"), rados(_r), io_ctx(_i) {}
  int aio_bench(int operation, int secondsToRun, int concurrentios, int op_size);
};


#endif
