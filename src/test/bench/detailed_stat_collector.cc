// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "detailed_stat_collector.h"
#include <sys/time.h>
#include <utility>
#include <boost/tuple/tuple.hpp>

void DetailedStatCollector::Op::dump(
  ostream *out,
  Formatter *f)
{
  if (!out)
    return;
  f->open_object_section(type.c_str());
  f->dump_string("type", type);
  f->dump_float("start", start);
  f->dump_float("latency", latency);
  f->dump_int("size", size);
  f->dump_int("seq", seq);
  f->close_section();
  f->flush(*out);
  *out << std::endl;
}

static utime_t cur_time()
{
  struct timeval tv;
  gettimeofday(&tv, 0);
  return utime_t(&tv);
}

DetailedStatCollector::Aggregator::Aggregator()
  : recent_size(0), total_size(0), recent_latency(0),
    total_latency(0), recent_ops(0), total_ops(0), started(false)
{}

void DetailedStatCollector::Aggregator::add(const Op &op)
{
  if (!started) {
    last = first = op.start;
    started = true;
  }
  ++recent_ops;
  ++total_ops;
  recent_size += op.size;
  total_size += op.size;
  recent_latency += op.latency;
  total_latency += op.latency;
}

void DetailedStatCollector::Aggregator::dump(Formatter *f)
{
  utime_t now = cur_time();
  f->dump_stream("time") << now;
  f->dump_float("avg_recent_latency", recent_latency / recent_ops);
  f->dump_float("avg_total_latency", total_latency / total_ops);
  f->dump_float("avg_recent_iops", recent_ops / (now - last));
  f->dump_float("avg_total_iops", total_ops / (now - first));
  f->dump_float("avg_recent_throughput", recent_size / (now - last));
  f->dump_float("avg_total_throughput", total_size / (now - first));
  f->dump_float("avg_recent_throughput_mb",
		(recent_size / (now - last)) / (1024*1024));
  f->dump_float("avg_total_throughput_mb",
		(total_size / (now - first)) / (1024*1024));
  f->dump_float("duration", now - last);
  last = now;
  recent_latency = 0;
  recent_size = 0;
  recent_ops = 0;
}

DetailedStatCollector::DetailedStatCollector(
  double bin_size,
  Formatter *formatter,
  ostream *out,
  ostream *summary_out,
  AdditionalPrinting *details
  ) : bin_size(bin_size), f(formatter), out(out),
      summary_out(summary_out), details(details),
      lock("Stat::lock"), cur_seq(0) {
  last_dump = cur_time();
}

uint64_t DetailedStatCollector::next_seq()
{
  Mutex::Locker l(lock);
  if (summary_out && ((cur_time() - last_dump) > bin_size)) {
    f->open_object_section("stats");
    for (map<string, Aggregator>::iterator i = aggregators.begin();
	 i != aggregators.end();
	 ++i) {
      f->open_object_section(i->first.c_str());
      i->second.dump(f.get());
      f->close_section();
    }
    f->close_section();
    f->flush(*summary_out);
    *summary_out << std::endl;
    if (details) {
      (*details)(summary_out);
      *summary_out << std::endl;
    }
    last_dump = cur_time();
  }
  return cur_seq++;
}

void DetailedStatCollector::start_write(uint64_t seq, uint64_t length)
{
  Mutex::Locker l(lock);
  utime_t now(cur_time());
  not_committed.insert(make_pair(seq, make_pair(length, now)));
  not_applied.insert(make_pair(seq, make_pair(length, now)));
}

void DetailedStatCollector::start_read(uint64_t seq, uint64_t length)
{
  Mutex::Locker l(lock);
  utime_t now(cur_time());
  not_read.insert(make_pair(seq, make_pair(length, now)));
}

void DetailedStatCollector::write_applied(uint64_t seq)
{
  Mutex::Locker l(lock);
  Op op(
    "write_applied",
    not_applied[seq].second,
    cur_time() - not_applied[seq].second,
    not_applied[seq].first,
    seq);
  op.dump(out, f.get());
  aggregators["write_applied"].add(op);
  not_applied.erase(seq);
}

void DetailedStatCollector::write_committed(uint64_t seq)
{
  Mutex::Locker l(lock);
  Op op(
    "write_committed",
    not_committed[seq].second,
    cur_time() - not_committed[seq].second,
    not_committed[seq].first,
    seq);
  op.dump(out, f.get());
  aggregators["write_committed"].add(op);
  not_committed.erase(seq);
}

void DetailedStatCollector::read_complete(uint64_t seq)
{
  Mutex::Locker l(lock);
  Op op(
    "read",
    not_read[seq].second,
    cur_time() - not_read[seq].second,
    not_read[seq].first,
    seq);
  op.dump(out, f.get());
  aggregators["read"].add(op);
  not_read.erase(seq);
}
