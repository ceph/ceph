// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mgr/MDSPerfMetricTypes.h"

#include <ostream>

#include "common/Formatter.h"
#include "include/container_ios.h"

void MDSPerfMetricSubKeyDescriptor::dump(ceph::Formatter *f) const {
  f->dump_unsigned("type", static_cast<uint8_t>(type));
  f->dump_string("regex_str", regex_str);
}

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

void denc_traits<MDSPerfMetricKeyDescriptor>::decode(MDSPerfMetricKeyDescriptor& v,
						     ceph::buffer::ptr::const_iterator& p) {
  unsigned num;
  denc_varint(num, p);
  v.clear();
  v.reserve(num);
  for (unsigned i=0; i < num; ++i) {
    MDSPerfMetricSubKeyDescriptor d;
    denc(d, p);
    if (!d.is_supported()) {
      v.clear();
      return;
    }
    try {
      d.regex = d.regex_str.c_str();
    } catch (const std::regex_error& e) {
      v.clear();
      return;
    }
    if (d.regex.mark_count() == 0) {
      v.clear();
      return;
    }
    v.push_back(std::move(d));
  }
}

void MDSPerformanceCounterDescriptor::dump(ceph::Formatter *f) const {
  f->dump_unsigned("type", static_cast<uint8_t>(type));
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
  case MDSPerformanceCounterType::SUBV_READ_IOPS_METRIC:
  case MDSPerformanceCounterType::SUBV_WRITE_IOPS_METRIC:
  case MDSPerformanceCounterType::SUBV_READ_THROUGHPUT_METRIC:
  case MDSPerformanceCounterType::SUBV_WRITE_THROUGHPUT_METRIC:
  case MDSPerformanceCounterType::SUBV_AVG_READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::SUBV_AVG_WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::SUBV_QUOTA_BYTES_METRIC:
  case MDSPerformanceCounterType::SUBV_USED_BYTES_METRIC:
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
  case MDSPerformanceCounterType::SUBV_READ_IOPS_METRIC:
  case MDSPerformanceCounterType::SUBV_WRITE_IOPS_METRIC:
  case MDSPerformanceCounterType::SUBV_READ_THROUGHPUT_METRIC:
  case MDSPerformanceCounterType::SUBV_WRITE_THROUGHPUT_METRIC:
  case MDSPerformanceCounterType::SUBV_AVG_READ_LATENCY_METRIC:
  case MDSPerformanceCounterType::SUBV_AVG_WRITE_LATENCY_METRIC:
  case MDSPerformanceCounterType::SUBV_QUOTA_BYTES_METRIC:
  case MDSPerformanceCounterType::SUBV_USED_BYTES_METRIC:
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
     case MDSPerformanceCounterType::SUBV_READ_IOPS_METRIC:
       os << "subv_read_iops";
       break;
     case MDSPerformanceCounterType::SUBV_WRITE_IOPS_METRIC:
       os << "subv_write_iops";
       break;
     case MDSPerformanceCounterType::SUBV_READ_THROUGHPUT_METRIC:
       os << "subv_last_read_tp_Bps";
       break;
     case MDSPerformanceCounterType::SUBV_WRITE_THROUGHPUT_METRIC:
       os << "subv_last_write_tp_Bps";
       break;
     case MDSPerformanceCounterType::SUBV_AVG_READ_LATENCY_METRIC:
       os << "subv_avg_read_latency";
       break;
     case MDSPerformanceCounterType::SUBV_AVG_WRITE_LATENCY_METRIC:
       os << "subv_avg_write_latency";
       break;
    case MDSPerformanceCounterType::SUBV_QUOTA_BYTES_METRIC:
      os << "subv_quota_bytes";
      break;
    case MDSPerformanceCounterType::SUBV_USED_BYTES_METRIC:
      os << "subv_used_bytes";
      break;
   }

   return os;
}

std::ostream &operator<<(std::ostream &os, const MDSPerfMetricLimit &limit) {
  return os << "[order_by=" << limit.order_by << ", max_count=" << limit.max_count << "]";
}

void MDSPerfMetricLimit::dump(ceph::Formatter *f) const {
  f->dump_object("order_by", order_by);
  f->dump_unsigned("max_count", max_count);
}

void MDSPerfMetricQuery::dump(ceph::Formatter *f) const {
  f->dump_stream("key_descriptor") << key_descriptor;
  f->dump_stream("performance_counter_descriptors") << performance_counter_descriptors;
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

void MDSPerfMetrics::dump(ceph::Formatter *f) const {
  f->dump_stream("performance_counter_descriptors") << performance_counter_descriptors;
  f->open_array_section("group_packed_performance_counters");
  for (auto &i : group_packed_performance_counters) {
    f->dump_stream("key") << i.first;
    f->dump_stream("value") << i.second;
  }
  f->close_section();
}

void MDSPerfMetricReport::dump(ceph::Formatter *f) const {
  f->open_array_section("reports");
  for (auto &i : reports) {
    f->open_object_section("query");
    f->dump_object("query",i.first);
    f->close_section();
    f->open_object_section("metrics");
    f->dump_object("metrics",i.second);
    f->close_section();
  }
  f->close_section();
}

std::list<MDSPerfMetricReport> MDSPerfMetricReport::generate_test_instances() {
  std::list<MDSPerfMetricReport> o;
  o.emplace_back();
  o.emplace_back();
  o.back().reports.emplace(MDSPerfMetricQuery(), MDSPerfMetrics());
  o.back().rank_metrics_delayed.insert(1);
  return o;
}

void AggregatedSubvolumeMetric::dump(ceph::Formatter* f) const {
  f->dump_string("subvolume_path", subvolume_path);
  f->dump_unsigned("read_iops", read_iops);
  f->dump_unsigned("write_iops", write_iops);
  f->dump_unsigned("read_tpBs", read_tpBs);
  f->dump_unsigned("write_tBps", write_tBps);
  f->dump_unsigned("quota_bytes", quota_bytes);
  f->dump_unsigned("used_bytes", used_bytes);

  f->dump_unsigned("min_read_latency_ns", min_read_latency);
  f->dump_unsigned("max_read_latency_ns", max_read_latency);
  f->dump_unsigned("avg_read_latency_ns", avg_read_latency);

  f->dump_unsigned("min_write_latency_ns", min_write_latency);
  f->dump_unsigned("max_write_latency_ns", max_write_latency);
  f->dump_unsigned("avg_write_latency_ns", avg_write_latency);

  f->dump_unsigned("time_window_sec_end", time_window_last_end_sec);
  f->dump_unsigned("time_window_sec_dur", time_window_last_dur_sec);
}
