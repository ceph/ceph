/*
 * Generate latency statistics for a configurable number of object map write
 * operations of configurable size.
 *
 *  Created on: May 21, 2012
 *      Author: Eleanor Cawthon
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef OMAP_BENCH_HPP_
#define OMAP_BENCH_HPP_

#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/rados/librados.hpp"
#include <string>
#include <map>
#include <cfloat>

using ceph::bufferlist;

struct o_bench_data {
  double avg_latency;
  double min_latency;
  double max_latency;
  double total_latency;
  int started_ops;
  int completed_ops;
  std::map<int,int> freq_map;
  pair<int,int> mode;
  o_bench_data()
  : avg_latency(0.0), min_latency(DBL_MAX), max_latency(0.0),
    total_latency(0.0),
    started_ops(0), completed_ops(0)
  {}
};

class OmapBench;

typedef int (*omap_generator_t)(const int omap_entries, const int key_size,
				const int value_size,
				std::map<std::string,bufferlist> *out_omap);
typedef int (OmapBench::*test_t)(omap_generator_t omap_gen);


class Writer{
protected:
  string oid;
  utime_t begin_time;
  utime_t end_time;
  std::map<std::string,bufferlist> omap;
  OmapBench *ob;
  friend class OmapBench;
public:
  Writer(OmapBench *omap_bench);
  virtual ~Writer(){};
  virtual void start_time();
  virtual void stop_time();
  virtual double get_time();
  virtual string get_oid();
  virtual std::map<std::string,bufferlist> & get_omap();
};

class AioWriter : public Writer{
protected:
  librados::AioCompletion * aioc;
  friend class OmapBench;

public:
  AioWriter(OmapBench *omap_bench);
  ~AioWriter();
  virtual librados::AioCompletion * get_aioc();
  virtual void set_aioc(librados::callback_t complete,
      librados::callback_t safe);
};

class OmapBench{
protected:
  librados::IoCtx io_ctx;
  librados::Rados rados;
  struct o_bench_data data;
  test_t test;
  omap_generator_t omap_generator;

  //aio things
  Cond thread_is_free;
  Mutex thread_is_free_lock;
  Mutex  data_lock;
  int busythreads_count;
  librados::callback_t comp;
  librados::callback_t safe;

  string pool_name;
  string rados_id;
  string prefix;
  int threads;
  int objects;
  int entries_per_omap;
  int key_size;
  int value_size;
  double increment;

  friend class Writer;
  friend class AioWriter;

public:
  OmapBench()
    : test(&OmapBench::test_write_objects_in_parallel),
      omap_generator(generate_uniform_omap),
      thread_is_free_lock("OmapBench::thread_is_free_lock"),
      data_lock("OmapBench::data_lock"),
      busythreads_count(0),
      comp(NULL), safe(aio_is_safe),
      pool_name("rbd"),
      rados_id("admin"),
      prefix(rados_id+".obj."),
      threads(3), objects(100), entries_per_omap(10), key_size(10),
      value_size(100), increment(10)
  {}
  /**
   * Parses command line args, initializes rados and ioctx
   */
  int setup(int argc, const char** argv);

  /**
   * Callback for when an AioCompletion (called from an AioWriter)
   * is safe. deletes the AioWriter that called it,
   * Updates data, updates busythreads, and signals thread_is_free.
   *
   * @param c provided by aio_write - not used
   * @param arg the AioWriter that contains this AioCompletion
   */
  static void aio_is_safe(rados_completion_t c, void *arg);

  /**
   * Generates a random string len characters long
   */
  static string random_string(int len);

  /*
   * runs the test specified by test using the omap generator specified by
   * omap_generator
   *
   * @return error code
   */
  int run();

  /*
   * Prints all keys and values for all omap entries for all objects
   */
  int print_written_omap();

  /*
   * Displays relevant constants and the histogram generated through a test
   */
  void print_results();

  /**
   * Writes an object with the specified AioWriter.
   *
   * @param aiow the AioWriter to write with
   * @param omap the omap to write
   * @post: an asynchronous omap_set is launched
   */
  int write_omap_asynchronously(AioWriter *aiow,
      const std::map<std::string,bufferlist> &map);


  /**
   * Generates an omap with omap_entries entries, each with keys key_size
   * characters long and with string values value_size characters long.
   *
   * @param out_map pointer to the map to be created
   * @return error code
   */
  static int generate_uniform_omap(const int omap_entries, const int key_size,
      const int value_size, std::map<std::string,bufferlist> * out_omap);

  /**
   * The same as generate_uniform_omap except that string lengths are picked
   * randomly between 1 and the int arguments
   */
  static int generate_non_uniform_omap(const int omap_entries,
      const int key_size,
      const int value_size, std::map<std::string,bufferlist> * out_omap);

  static int generate_small_non_random_omap(const int omap_entries,
      const int key_size, const int value_size,
      std::map<std::string,bufferlist> * out_omap);

  /*
   * Uses aio_write to write omaps generated by omap_gen to OBJECTS objects
   * using THREADS AioWriters at a time.
   *
   * @param omap_gen the method used to generate the omaps.
   */
  int test_write_objects_in_parallel(omap_generator_t omap_gen);

};



#endif /* OMAP_BENCH_HPP_ */

