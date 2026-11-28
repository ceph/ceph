// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MDSPerfMetricTypes.h"

#include <ostream>

#include "common/Formatter.h"
#include "include/container_ios.h"
#include "include/encoding_vector.h"

void CapHitMetric::dump(Formatter *f) const {
  f->dump_unsigned("hits", hits);
  f->dump_unsigned("misses", misses);
}

std::ostream& operator<<(std::ostream& os, const CapHitMetric &metric) {
  os << "{hits=" << metric.hits << ", misses=" << metric.misses << "}";
  return os;
}

void ReadLatencyMetric::dump(Formatter *f) const {
  f->dump_object("read_latency", lat);
  f->dump_object("avg_read_alatency", mean);
  f->dump_unsigned("sq_sum", sq_sum);
  f->dump_unsigned("count", count);
}

std::ostream& operator<<(std::ostream& os, const ReadLatencyMetric &metric) {
  os << "{latency=" << metric.lat << ", avg_latency=" << metric.mean
     << ", sq_sum=" << metric.sq_sum << ", count=" << metric.count << "}";
  return os;
}

void WriteLatencyMetric::dump(Formatter *f) const {
  f->dump_object("write_latency", lat);
  f->dump_object("avg_write_alatency", mean);
  f->dump_unsigned("sq_sum", sq_sum);
  f->dump_unsigned("count", count);
}

std::ostream& operator<<(std::ostream& os, const WriteLatencyMetric &metric) {
  os << "{latency=" << metric.lat << ", avg_latency=" << metric.mean
     << ", sq_sum=" << metric.sq_sum << ", count=" << metric.count  << "}";
  return os;
}

void MetadataLatencyMetric::dump(Formatter *f) const {
  f->dump_object("metadata_latency", lat);
  f->dump_object("avg_metadata_alatency", mean);
  f->dump_unsigned("sq_sum", sq_sum);
  f->dump_unsigned("count", count);
}

std::ostream& operator<<(std::ostream& os, const MetadataLatencyMetric &metric) {
  os << "{latency=" << metric.lat << ", avg_latency=" << metric.mean
     << ", sq_sum=" << metric.sq_sum << ", count=" << metric.count << "}";
  return os;
}

void DentryLeaseHitMetric::dump(Formatter *f) const {
  f->dump_unsigned("hits", hits);
  f->dump_unsigned("misses", misses);
}

std::ostream& operator<<(std::ostream& os, const DentryLeaseHitMetric &metric) {
  os << "{hits=" << metric.hits << ", misses=" << metric.misses << "}";
  return os;
}

void OpenedFilesMetric::dump(Formatter *f) const {
  f->dump_unsigned("opened_files", opened_files);
  f->dump_unsigned("total_inodes", total_inodes);
}

std::ostream& operator<<(std::ostream& os, const OpenedFilesMetric &metric) {
  os << "{opened_files=" << metric.opened_files  << ", total_inodes="
     << metric.total_inodes << "}";
  return os;
}

void PinnedIcapsMetric::dump(Formatter *f) const {
  f->dump_unsigned("pinned_icaps", pinned_icaps);
  f->dump_unsigned("total_inodes", total_inodes);
}

std::ostream& operator<<(std::ostream& os, const PinnedIcapsMetric &metric) {
  os << "{pinned_icaps=" << metric.pinned_icaps << ", total_inodes="
     << metric.total_inodes << "}";
  return os;
}

void OpenedInodesMetric::dump(Formatter *f) const {
  f->dump_unsigned("opened_inodes", opened_inodes);
  f->dump_unsigned("total_inodes", total_inodes);
}

std::ostream& operator<<(std::ostream& os, const OpenedInodesMetric &metric) {
  os << "{opened_inodes=" << metric.opened_inodes << ", total_inodes="
     << metric.total_inodes << "}";
  return os;
}

void ReadIoSizesMetric::dump(Formatter *f) const {
  f->dump_unsigned("total_ops", total_ops);
  f->dump_unsigned("total_size", total_size);
}

std::ostream& operator<<(std::ostream& os, const ReadIoSizesMetric &metric) {
  os << "{total_ops=" << metric.total_ops << ", total_size=" << metric.total_size <<"}";
  return os;
}

void WriteIoSizesMetric::dump(Formatter *f) const {
  f->dump_unsigned("total_ops", total_ops);
  f->dump_unsigned("total_size", total_size);
}

std::ostream& operator<<(std::ostream& os, const WriteIoSizesMetric &metric) {
  os << "{total_ops=" << metric.total_ops << ", total_size=" << metric.total_size <<"}";
  return os;
}

void RankPerfMetrics::dump(Formatter *f) const {
  f->dump_unsigned("cpu_usage_percent", cpu_usage_percent);
  f->dump_unsigned("open_requests", open_requests);
}

std::ostream& operator<<(std::ostream& os, const RankPerfMetrics &metric) {
  os << "{cpu_usage_percent=" << metric.cpu_usage_percent
     << ", open_requests=" << metric.open_requests << "}";
  return os;
}

void Metrics::dump(Formatter *f) const {
  f->dump_int("update_type", static_cast<uint32_t>(update_type));
  f->dump_object("cap_hit_metric", cap_hit_metric);
  f->dump_object("read_latency_metric", read_latency_metric);
  f->dump_object("write_latency_metric", write_latency_metric);
  f->dump_object("metadata_latency_metric", metadata_latency_metric);
  f->dump_object("dentry_lease_metric", dentry_lease_metric);
  f->dump_object("opened_files_metric", opened_files_metric);
  f->dump_object("pinned_icaps_metric", pinned_icaps_metric);
  f->dump_object("opened_inodes_metric", opened_inodes_metric);
  f->dump_object("read_io_sizes_metric", read_io_sizes_metric);
  f->dump_object("write_io_sizes_metric", write_io_sizes_metric);
}

std::ostream& operator<<(std::ostream& os, const Metrics& metrics) {
  os << "[update_type=" << metrics.update_type << ", metrics={"
     << "cap_hit_metric=" << metrics.cap_hit_metric
     << ", read_latency=" << metrics.read_latency_metric
     << ", write_latency=" << metrics.write_latency_metric
     << ", metadata_latency=" << metrics.metadata_latency_metric
     << ", dentry_lease=" << metrics.dentry_lease_metric
     << ", opened_files_metric=" << metrics.opened_files_metric
     << ", pinned_icaps_metric=" << metrics.pinned_icaps_metric
     << ", opened_inodes_metric=" << metrics.opened_inodes_metric
     << ", read_io_sizes_metric=" << metrics.read_io_sizes_metric
     << ", write_io_sizes_metric=" << metrics.write_io_sizes_metric
     << "}]";
  return os;
}

void metrics_message_t::encode(bufferlist &bl, uint64_t features) const {
  using ceph::encode;
  ENCODE_START(3, 1, bl);
  encode(seq, bl);
  encode(rank, bl);
  encode(client_metrics_map, bl, features);
  encode(subvolume_metrics, bl);
  encode(rank_metrics, bl);
  ENCODE_FINISH(bl);
}

void metrics_message_t::decode(bufferlist::const_iterator &iter) {
  using ceph::decode;
  DECODE_START(3, iter);
  decode(seq, iter);
  decode(rank, iter);
  decode(client_metrics_map, iter);
  if (struct_v >= 2) {
    decode(subvolume_metrics, iter);
  }
  if (struct_v >= 3) {
    decode(rank_metrics, iter);
  } else {
    rank_metrics = {};
  }
  DECODE_FINISH(iter);
}

void metrics_message_t::dump(Formatter *f) const {
  f->dump_unsigned("seq", seq);
  f->dump_int("rank", rank);
  for (auto &[client, metrics] : client_metrics_map) {
    f->dump_object("client", client);
    f->dump_object("metrics", metrics);
  }
  f->dump_object("rank_metrics", rank_metrics);
  f->open_array_section("subvolume_metrics");
  for (const auto &metric : subvolume_metrics) {
    f->open_object_section("metric");
    metric.dump(f);
    f->close_section();
  }
  f->close_section();
}

std::ostream& operator<<(std::ostream& os, const metrics_message_t &m) {
  os << "[sequence=" << m.seq << ", rank=" << m.rank
     << ", client_metrics=" << m.client_metrics_map
     << ", subvolume_metrics=" << m.subvolume_metrics;
  os << ", rank_metrics=" << m.rank_metrics;
  os << "]";
  return os;
}
