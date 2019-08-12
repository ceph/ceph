// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "include/rados/librados.hpp"

#ifndef TESTOPSTAT_H
#define TESTOPSTAT_H

class TestOp;

class TestOpStat {
public:
  mutable ceph::mutex stat_lock = ceph::make_mutex("TestOpStat lock");

  TestOpStat() = default;
    
  static uint64_t gettime()
  {
    timeval t;
    gettimeofday(&t,0);
    return (1000000*t.tv_sec) + t.tv_usec;
  }

  class TypeStatus {
  public:
    map<TestOp*,uint64_t> inflight;
    multiset<uint64_t> latencies;
    void begin(TestOp *in)
    {
      ceph_assert(!inflight.count(in));
      inflight[in] = gettime();
    }

    void end(TestOp *in)
    {
      ceph_assert(inflight.count(in));
      uint64_t curtime = gettime();
      latencies.insert(curtime - inflight[in]);
      inflight.erase(in);
    }

    void export_latencies(map<double,uint64_t> &in) const;
  };
  map<string,TypeStatus> stats;

  void begin(TestOp *in);
  void end(TestOp *in);
  friend std::ostream & operator<<(std::ostream &, const TestOpStat &);
};

std::ostream & operator<<(std::ostream &out, const TestOpStat &rhs);

#endif
