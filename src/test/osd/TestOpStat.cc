// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>
#include "RadosModel.h"
#include "TestOpStat.h"

void TestOpStat::begin(TestOp *in) {
  std::lock_guard l{stat_lock};
  stats[in->getType()].begin(in);
}

void TestOpStat::end(TestOp *in) {
  std::lock_guard l{stat_lock};
  stats[in->getType()].end(in);
}

void TestOpStat::TypeStatus::export_latencies(std::map<double,uint64_t> &in) const
{
  auto i = in.begin();
  auto j = latencies.begin();
  int count = 0;
  while (j != latencies.end() && i != in.end()) {
    count++;
    if ((((double)count)/((double)latencies.size())) * 100 >= i->first) {
      i->second = *j;
      ++i;
    }
    ++j;
  }
}

std::ostream & operator<<(std::ostream &out, const TestOpStat &rhs)
{
  std::lock_guard l{rhs.stat_lock};
  for (auto i = rhs.stats.begin();
       i != rhs.stats.end();
       ++i) {
    std::map<double,uint64_t> latency;
    latency[10] = 0;
    latency[50] = 0;
    latency[90] = 0;
    latency[99] = 0;
    i->second.export_latencies(latency);

    out << i->first << " latency: " << std::endl;
    for (auto j = latency.begin();
	 j != latency.end();
	 ++j) {
      if (j->second == 0) break;
      out << "\t" << j->first << "th percentile: " 
	  << j->second / 1000 << "ms" << std::endl;
    }
  }
  return out;
}
