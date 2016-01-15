/*
 * Benchmarking suite for key-value store
 *
 * September 2, 2012
 * Eleanor Cawthon
 * eleanor.cawthon@inktank.com
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef KVSTOREBENCH_H_
#define KVSTOREBENCH_H_

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "common/Clock.h"
#include "global/global_context.h"
#include "common/Mutex.h"
#include "common/Cond.h"

#include <string>
#include <climits>
#include <cfloat>
#include <iostream>

using namespace std;
using ceph::bufferlist;

/**
 * stores pairings from op type to time taken for that op (for latency), and to
 * time that op completed to the nearest second (for throughput).
 */
struct kv_bench_data {
  JSONFormatter throughput_jf;

  JSONFormatter latency_jf;
};

class KvStoreBench;

/**
 * keeps track of the number of milliseconds between two events - used to
 * measure latency
 */
struct StopWatch {
  utime_t begin_time;
  utime_t end_time;

  void start_time() {
    begin_time = ceph_clock_now(g_ceph_context);
  }
  void stop_time() {
    end_time = ceph_clock_now(g_ceph_context);
  }
  double get_time() {
    return (end_time - begin_time) * 1000;
  }
  void clear() {
    begin_time = end_time = utime_t();
  }
};

/**
 * arguments passed to the callback method when the op is being timed
 */
struct timed_args {
  StopWatch sw;
  //kv_bench_data data;
  KvStoreBench * kvsb;
  bufferlist val;
  int err;
  char op;

  timed_args ()
  : kvsb(NULL),
    err(0),
    op(' ')
  {};

  timed_args (KvStoreBench * k)
  : kvsb(k),
    err(0),
    op(' ')
  {}
};

typedef pair<string, bufferlist> (KvStoreBench::*next_gen_t)(bool new_elem);

class KvStoreBench {

protected:

  //test setup variables set from command line
  int entries; //the number of entries to write initially
  int ops; //the number of operations to time
  int clients; //the total number of clients running this test - used
	       //in the aio test to coordinate the end of the initial sets
  int key_size;//number of characters in keys to write
  int val_size;//number of characters in values to write
  int max_ops_in_flight;
  bool clear_first;//if true, remove all objects in pool before starting tests

  //variables passed to KeyValueStructure
  int k;
  int cache_size; //number of index entries to store in cache
  double cache_refresh; //cache_size / cache_refresh entries are read each time
			//the index is read
  string client_name;
  bool verbose;//if true, display debug output

  //internal
  map<int, char> probs;//map of numbers from 1 to 100 to chars representing
			//operation types - used to generate random operations
  set<string> key_set;//set of keys already in the data set
  KeyValueStructure * kvs;
  kv_bench_data data;//stores throughput and latency from completed tests
  Mutex data_lock;
  Cond op_avail;//signaled when an op completes
  int ops_in_flight;//number of operations currently in progress
  Mutex ops_in_flight_lock;
  //these are used for cleanup and setup purposes - they are NOT passed to kvs!
  librados::Rados rados;
  string rados_id;
  string pool_name;
  bool io_ctx_ready;
  librados::IoCtx io_ctx;

  /**
   * Prints JSON-formatted throughput and latency data.
   *
   * Throughput data is {'char representing the operation type':time the op
   * completed to the nearest second}
   * Latency is {'char representing the operation type':time taken by the op}
   */
  void print_time_data();

public:

  KvStoreBench();

  //after this is called, objects created by the KeyValueStructure remain.
  ~KvStoreBench();

  /**
   * parses command line arguments, sets up this rados instance, clears the
   * pool if clear_first is true and calls kvs->setup.
   */
  int setup(int argc, const char** argv);

  /**
   * Returns a string of random characters of length len
   */
  string random_string(int len);

  /**
   * Inserts entries random keys and values asynchronously.
   */
  int test_random_insertions();

  /**
   * calls test_random_insertions, then does ops randomly chosen operations
   * asynchronously, with max_ops_in_flight operations at a time.
   */
  int test_teuthology_aio(next_gen_t distr, const map<int, char> &probs);

  /**
   * calls test_random_insertions, then does ops randomly chosen operations
   * synchronously.
   */
  int test_teuthology_sync(next_gen_t distr, const map<int, char> &probs);

  /**
   * returns a key-value pair. If new_elem is true, the key is randomly
   * generated. If it is false, the key is selected from the keys currently in
   * the key set.
   */
  pair<string, bufferlist> rand_distr(bool new_elem);

  /**
   * Called when aio operations complete. Updates data.
   */
  static void aio_callback_timed(int * err, void *arg);

  /**
   * Calls test_ methods. Change to call, for example, multiple runs of a test
   * with different settings. Currently just calls test_teuthology_aio.
   */
  int teuthology_tests();

};

#endif /* KVSTOREBENCH_H_ */
