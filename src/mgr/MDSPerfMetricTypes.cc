// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ostream>
#include "mgr/MDSPerfMetricTypes.h"

std::ostream& operator<<(std::ostream& os, const MDSPerfMetricSubKeyDescriptor &d) {
  switch (d.type) {
  case MDSPerfMetricSubKeyType::MDS_RANK:
    os << "mds_rank";
    break;
  case MDSPerfMetricSubKeyType::CLIENT_ID:
    os << "client_id";
    break;
  default:
    os << "unknown (" << static_cast<int>(d.type) << ")";
  }

  return os << "~/" << d.regex_str << "/";
}

void MDSPerformanceCounterDescriptor::pack_counter(
    const PerformanceCounter &c, bufferlist *bl) const {
  using ceph::encode;
  encode(c.first, *bl);
  encode(c.second, *bl);
  switch(type) {
  case MDSPerformanceCounterType::CAP_HIT_METRIC:
  case MDSPerformanceCounterType::READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
  case MDSPerformanceCounterType::DENTRY_LEASE_METRIC:
  case MDSPerformanceCounterType::OPENED_FILES_METRIC:
  case MDSPerformanceCounterType::PINNED_ICAPS_METRIC:
  case MDSPerformanceCounterType::OPENED_INODES_METRIC:
  case MDSPerformanceCounterType::READ_IO_SIZES_METRIC:
  case MDSPerformanceCounterType::WRITE_IO_SIZES_METRIC:
  case MDSPerformanceCounterType::AVG_READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::STDEV_READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::AVG_WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::STDEV_WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::AVG_METADATA_LATENCY_METRIC:
  case MDSPerformanceCounterType::STDEV_METADATA_LATENCY_METRIC:
    break;
  default:
    ceph_abort_msg("unknown counter type");
  }
}

void MDSPerformanceCounterDescriptor::unpack_counter(
    bufferlist::const_iterator& bl, PerformanceCounter *c) const {
  using ceph::decode;
  decode(c->first, bl);
  decode(c->second, bl);
  switch(type) {
  case MDSPerformanceCounterType::CAP_HIT_METRIC:
  case MDSPerformanceCounterType::READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
  case MDSPerformanceCounterType::DENTRY_LEASE_METRIC:
  case MDSPerformanceCounterType::OPENED_FILES_METRIC:
  case MDSPerformanceCounterType::PINNED_ICAPS_METRIC:
  case MDSPerformanceCounterType::OPENED_INODES_METRIC:
  case MDSPerformanceCounterType::READ_IO_SIZES_METRIC:
  case MDSPerformanceCounterType::WRITE_IO_SIZES_METRIC:
  case MDSPerformanceCounterType::AVG_READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::STDEV_READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::AVG_WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::STDEV_WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::AVG_METADATA_LATENCY_METRIC:
  case MDSPerformanceCounterType::STDEV_METADATA_LATENCY_METRIC:
    break;
  default:
    ceph_abort_msg("unknown counter type");
  }
}

std::ostream& operator<<(std::ostream &os, const MDSPerformanceCounterDescriptor &d) {
   switch(d.type) {
   case MDSPerformanceCounterType::CAP_HIT_METRIC:
     os << "cap_hit_metric";
     break;
   case MDSPerformanceCounterType::READ_LATENCY_METRIC:
     os << "read_latency_metric";
     break;
   case MDSPerformanceCounterType::WRITE_LATENCY_METRIC:
     os << "write_latency_metric";
     break;
   case MDSPerformanceCounterType::METADATA_LATENCY_METRIC:
     os << "metadata_latency_metric";
     break;
   case MDSPerformanceCounterType::DENTRY_LEASE_METRIC:
     os << "dentry_lease_metric";
     break;
   case MDSPerformanceCounterType::OPENED_FILES_METRIC:
     os << "opened_files_metric";
     break;
   case MDSPerformanceCounterType::PINNED_ICAPS_METRIC:
     os << "pinned_icaps_metric";
     break;
   case MDSPerformanceCounterType::OPENED_INODES_METRIC:
     os << "opened_inodes_metric";
     break;
   case MDSPerformanceCounterType::READ_IO_SIZES_METRIC:
     os << "read_io_sizes_metric";
     break;
   case MDSPerformanceCounterType::WRITE_IO_SIZES_METRIC:
     os << "write_io_sizes_metric";
     break;
   case MDSPerformanceCounterType::AVG_READ_LATENCY_METRIC:
     os << "avg_read_latency";
     break;
   case MDSPerformanceCounterType::STDEV_READ_LATENCY_METRIC:
     os << "stdev_read_latency";
     break;
   case MDSPerformanceCounterType::AVG_WRITE_LATENCY_METRIC:
     os << "avg_write_latency";
     break;
   case MDSPerformanceCounterType::STDEV_WRITE_LATENCY_METRIC:
     os << "stdev_write_latency";
     break;
   case MDSPerformanceCounterType::AVG_METADATA_LATENCY_METRIC:
     os << "avg_metadata_latency";
     break;
   case MDSPerformanceCounterType::STDEV_METADATA_LATENCY_METRIC:
     os << "stdev_metadata_latency";
     break;
   }

   return os;
}

std::ostream &operator<<(std::ostream &os, const MDSPerfMetricLimit &limit) {
  return os << "[order_by=" << limit.order_by << ", max_count=" << limit.max_count << "]";
}

void MDSPerfMetricQuery::pack_counters(const PerformanceCounters &counters,
                                       bufferlist *bl) const {
  auto it = counters.begin();
  for (auto &descriptor : performance_counter_descriptors) {
    if (it == counters.end()) {
      descriptor.pack_counter(PerformanceCounter(), bl);
    } else {
      descriptor.pack_counter(*it, bl);
      it++;
    }
  }
}

std::ostream &operator<<(std::ostream &os, const MDSPerfMetricQuery &query) {
  return os << "[key=" << query.key_descriptor << ", counter="
            << query.performance_counter_descriptors << "]";
}
