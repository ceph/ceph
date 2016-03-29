// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef BENCHERH
#define BENCHERH

#include <utility>
#include "distribution.h"
#include "stat_collector.h"
#include "backend.h"
#include <boost/scoped_ptr.hpp>
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

struct OnWriteApplied;
struct OnWriteCommit;
struct OnReadComplete;
struct Cleanup;

class Bencher : public Thread {
public:
  enum OpType {
    WRITE,
    READ
  };

private:
  boost::scoped_ptr<
    Distribution<boost::tuple<std::string,uint64_t,uint64_t, OpType> > > op_dist;
  ceph::shared_ptr<StatCollector> stat_collector;
  boost::scoped_ptr<Backend> backend;
  const uint64_t max_in_flight;
  const uint64_t max_duration;
  const uint64_t max_ops;

  Mutex lock;
  Cond open_ops_cond;
  uint64_t open_ops;
  void start_op();
  void drain_ops();
  void complete_op();
public:
  Bencher(
    Distribution<boost::tuple<std::string, uint64_t, uint64_t, OpType> > *op_gen,
    ceph::shared_ptr<StatCollector> stat_collector,
    Backend *backend,
    uint64_t max_in_flight,
    uint64_t max_duration,
    uint64_t max_ops) :
    op_dist(op_gen),
    stat_collector(stat_collector),
    backend(backend),
    max_in_flight(max_in_flight),
    max_duration(max_duration),
    max_ops(max_ops),
    lock("Bencher::lock"),
    open_ops(0)
  {}
  Bencher(
    Distribution<boost::tuple<std::string, uint64_t, uint64_t, OpType> > *op_gen,
    StatCollector *stat_collector,
    Backend *backend,
    uint64_t max_in_flight,
    uint64_t max_duration,
    uint64_t max_ops) :
    op_dist(op_gen),
    stat_collector(stat_collector),
    backend(backend),
    max_in_flight(max_in_flight),
    max_duration(max_duration),
    max_ops(max_ops),
    lock("Bencher::lock"),
    open_ops(0)
  {}
  Bencher(
    Distribution<std::string> *object_gen,
    Distribution<uint64_t> *offset_gen,
    Distribution<uint64_t> *length_gen,
    Distribution<OpType> *op_type_gen,
    StatCollector *stat_collector,
    Backend *backend,
    uint64_t max_in_flight,
    uint64_t max_duration,
    uint64_t max_ops) :
    op_dist(
      new FourTupleDist<std::string, uint64_t, uint64_t, OpType>(
	object_gen, offset_gen, length_gen, op_type_gen)),
    stat_collector(stat_collector),
    backend(backend),
    max_in_flight(max_in_flight),
    max_duration(max_duration),
    max_ops(max_ops),
    lock("Bencher::lock"),
    open_ops(0)
  {}

  void init(
    const set<std::string> &objects,
    uint64_t size,
    std::ostream *out
    );

  void run_bench();
  void *entry() {
    run_bench();
    return 0;
  }
  friend struct OnWriteApplied;
  friend struct OnWriteCommit;
  friend struct OnReadComplete;
  friend struct Cleanup;
};

class SequentialLoad :
  public Distribution<
  boost::tuple<string, uint64_t, uint64_t, Bencher::OpType> > {
  set<string> objects;
  uint64_t size;
  uint64_t length;
  set<string>::iterator object_pos;
  uint64_t cur_pos;
  boost::scoped_ptr<Distribution<Bencher::OpType> > op_dist;
  SequentialLoad(const SequentialLoad &other);
public:
  SequentialLoad(
    const set<string> &_objects, uint64_t size,
    uint64_t length,
    Distribution<Bencher::OpType> *op_dist)
    : objects(_objects), size(size), length(length),
      object_pos(objects.begin()), cur_pos(0),
      op_dist(op_dist) {}

  boost::tuple<string, uint64_t, uint64_t, Bencher::OpType>
  operator()() {
    boost::tuple<string, uint64_t, uint64_t, Bencher::OpType> ret =
      boost::make_tuple(*object_pos, cur_pos, length, (*op_dist)());
    cur_pos += length;
    if (cur_pos >= size) {
      cur_pos = 0;
      ++object_pos;
    }
    if (object_pos == objects.end())
      object_pos = objects.begin();
    return ret;
  }
};
#endif
