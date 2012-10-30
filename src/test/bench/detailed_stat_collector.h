// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef DETAILEDSTATCOLLECTERH
#define DETAILEDSTATCOLLECTERH

#include "stat_collector.h"
#include "common/Formatter.h"
#include <boost/scoped_ptr.hpp>
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/utime.h"
#include <list>
#include <map>
#include <boost/tuple/tuple.hpp>
#include <ostream>

class DetailedStatCollector : public StatCollector {
public:
  class AdditionalPrinting {
  public:
    virtual void operator()(std::ostream *) = 0;
    virtual ~AdditionalPrinting() {}
  };
private:
  struct Op {
    string type;
    utime_t start;
    double latency;
    uint64_t size;
    uint64_t seq;
    Op(
      string type,
      utime_t start,
      double latency,
      uint64_t size,
      uint64_t seq)
      : type(type), start(start), latency(latency),
	size(size), seq(seq) {}
    void dump(ostream *out, Formatter *f);
  };
  class Aggregator {
    uint64_t recent_size;
    uint64_t total_size;
    double recent_latency;
    double total_latency;
    utime_t last;
    utime_t first;
    uint64_t recent_ops;
    uint64_t total_ops;
    bool started;
  public:
    Aggregator();

    void add(const Op &op);
    void dump(Formatter *f);
  };
  const double bin_size;
  boost::scoped_ptr<Formatter> f;
  ostream *out;
  ostream *summary_out;
  boost::scoped_ptr<AdditionalPrinting> details;
  utime_t last_dump;

  Mutex lock;
  Cond cond;

  map<string, Aggregator> aggregators;

  map<uint64_t, pair<uint64_t, utime_t> > not_applied;
  map<uint64_t, pair<uint64_t, utime_t> > not_committed;
  map<uint64_t, pair<uint64_t, utime_t> > not_read;

  uint64_t cur_seq;

  void dump(
    const string &type,
    boost::tuple<utime_t, utime_t, uint64_t, uint64_t> stuff);
public:
  DetailedStatCollector(
    double bin_size,
    Formatter *formatter,
    ostream *out,
    ostream *summary_out,
    AdditionalPrinting *details = 0
    );

  uint64_t next_seq();
  void start_write(uint64_t seq, uint64_t size);
  void start_read(uint64_t seq, uint64_t size);
  void write_applied(uint64_t seq);
  void write_committed(uint64_t seq);
  void read_complete(uint64_t seq);

};

#endif
