// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_PERF_METRIC_TYPES_H
#define CEPH_MDS_PERF_METRIC_TYPES_H

#include <ostream>

#include "include/denc.h"
#include "include/utime.h"
#include "mdstypes.h"

enum UpdateType : uint32_t {
  UPDATE_TYPE_REFRESH = 0,
  UPDATE_TYPE_REMOVE,
};

struct CapHitMetric {
  uint64_t hits = 0;
  uint64_t misses = 0;

  DENC(CapHitMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.hits, p);
    denc(v.misses, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("hits", hits);
    f->dump_unsigned("misses", misses);
  }

  friend std::ostream& operator<<(std::ostream& os, const CapHitMetric &metric) {
    os << "{hits=" << metric.hits << ", misses=" << metric.misses << "}";
    return os;
  }
};

struct ReadLatencyMetric {
  utime_t lat;
  bool updated = false;

  DENC(ReadLatencyMetric, v, p) {
    DENC_START(2, 1, p);
    denc(v.lat, p);
    if (struct_v >= 2)
      denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_object("read_latency", lat);
  }

  friend std::ostream& operator<<(std::ostream& os, const ReadLatencyMetric &metric) {
    os << "{latency=" << metric.lat << "}";
    return os;
  }
};

struct WriteLatencyMetric {
  utime_t lat;
  bool updated = false;

  DENC(WriteLatencyMetric, v, p) {
    DENC_START(2, 1, p);
    denc(v.lat, p);
    if (struct_v >= 2)
      denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_object("write_latency", lat);
  }

  friend std::ostream& operator<<(std::ostream& os, const WriteLatencyMetric &metric) {
    os << "{latency=" << metric.lat << "}";
    return os;
  }
};

struct MetadataLatencyMetric {
  utime_t lat;
  bool updated = false;

  DENC(MetadataLatencyMetric, v, p) {
    DENC_START(2, 1, p);
    denc(v.lat, p);
    if (struct_v >= 2)
      denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_object("metadata_latency", lat);
  }

  friend std::ostream& operator<<(std::ostream& os, const MetadataLatencyMetric &metric) {
    os << "{latency=" << metric.lat << "}";
    return os;
  }
};

struct DentryLeaseHitMetric {
  uint64_t hits = 0;
  uint64_t misses = 0;
  bool updated = false;

  DENC(DentryLeaseHitMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.hits, p);
    denc(v.misses, p);
    denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("hits", hits);
    f->dump_unsigned("misses", misses);
  }

  friend std::ostream& operator<<(std::ostream& os, const DentryLeaseHitMetric &metric) {
    os << "{hits=" << metric.hits << ", misses=" << metric.misses << "}";
    return os;
  }
};

struct OpenedFilesMetric {
  uint64_t opened_files = 0;
  uint64_t total_inodes = 0;
  bool updated = false;

  DENC(OpenedFilesMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.opened_files, p);
    denc(v.total_inodes, p);
    denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("opened_files", opened_files);
    f->dump_unsigned("total_inodes", total_inodes);
  }

  friend std::ostream& operator<<(std::ostream& os, const OpenedFilesMetric &metric) {
    os << "{opened_files=" << metric.opened_files  << ", total_inodes="
       << metric.total_inodes << "}";
    return os;
  }
};

struct PinnedIcapsMetric {
  uint64_t pinned_icaps = 0;
  uint64_t total_inodes = 0;
  bool updated = false;

  DENC(PinnedIcapsMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.pinned_icaps, p);
    denc(v.total_inodes, p);
    denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("pinned_icaps", pinned_icaps);
    f->dump_unsigned("total_inodes", total_inodes);
  }

  friend std::ostream& operator<<(std::ostream& os, const PinnedIcapsMetric &metric) {
    os << "{pinned_icaps=" << metric.pinned_icaps << ", total_inodes="
       << metric.total_inodes << "}";
    return os;
  }
};

struct OpenedInodesMetric {
  uint64_t opened_inodes = 0;
  uint64_t total_inodes = 0;
  bool updated = false;

  DENC(OpenedInodesMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.opened_inodes, p);
    denc(v.total_inodes, p);
    denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("opened_inodes", opened_inodes);
    f->dump_unsigned("total_inodes", total_inodes);
  }

  friend std::ostream& operator<<(std::ostream& os, const OpenedInodesMetric &metric) {
    os << "{opened_inodes=" << metric.opened_inodes << ", total_inodes="
       << metric.total_inodes << "}";
    return os;
  }
};

WRITE_CLASS_DENC(CapHitMetric)
WRITE_CLASS_DENC(ReadLatencyMetric)
WRITE_CLASS_DENC(WriteLatencyMetric)
WRITE_CLASS_DENC(MetadataLatencyMetric)
WRITE_CLASS_DENC(DentryLeaseHitMetric)
WRITE_CLASS_DENC(OpenedFilesMetric)
WRITE_CLASS_DENC(PinnedIcapsMetric)
WRITE_CLASS_DENC(OpenedInodesMetric)

// metrics that are forwarded to the MDS by client(s).
struct Metrics {
  // metrics
  CapHitMetric cap_hit_metric;
  ReadLatencyMetric read_latency_metric;
  WriteLatencyMetric write_latency_metric;
  MetadataLatencyMetric metadata_latency_metric;
  DentryLeaseHitMetric dentry_lease_metric;
  OpenedFilesMetric opened_files_metric;
  PinnedIcapsMetric pinned_icaps_metric;
  OpenedInodesMetric opened_inodes_metric;

  // metric update type
  uint32_t update_type = UpdateType::UPDATE_TYPE_REFRESH;

  DENC(Metrics, v, p) {
    DENC_START(3, 1, p);
    denc(v.update_type, p);
    denc(v.cap_hit_metric, p);
    denc(v.read_latency_metric, p);
    denc(v.write_latency_metric, p);
    denc(v.metadata_latency_metric, p);
    if (struct_v >= 2) {
      denc(v.dentry_lease_metric, p);
    }
    if (struct_v >= 3) {
      denc(v.opened_files_metric, p);
      denc(v.pinned_icaps_metric, p);
      denc(v.opened_inodes_metric, p);
    }
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_int("update_type", static_cast<uint32_t>(update_type));
    f->dump_object("cap_hit_metric", cap_hit_metric);
    f->dump_object("read_latency_metric", read_latency_metric);
    f->dump_object("write_latency_metric", write_latency_metric);
    f->dump_object("metadata_latency_metric", metadata_latency_metric);
    f->dump_object("dentry_lease_metric", dentry_lease_metric);
    f->dump_object("opened_files_metric", opened_files_metric);
    f->dump_object("pinned_icaps_metric", pinned_icaps_metric);
    f->dump_object("opened_inodes_metric", opened_inodes_metric);
  }

  friend std::ostream& operator<<(std::ostream& os, const Metrics& metrics) {
    os << "[update_type=" << metrics.update_type << ", metrics={"
       << "cap_hit_metric=" << metrics.cap_hit_metric
       << ", read_latency=" << metrics.read_latency_metric
       << ", write_latency=" << metrics.write_latency_metric
       << ", metadata_latency=" << metrics.metadata_latency_metric
       << ", dentry_lease =" << metrics.dentry_lease_metric
       << ", opened_files_metric =" << metrics.opened_files_metric
       << ", pinned_icaps_metric =" << metrics.pinned_icaps_metric
       << ", opened_inodes_metric =" << metrics.opened_inodes_metric
       << "}]";
    return os;
  }
};
WRITE_CLASS_DENC(Metrics)

struct metrics_message_t {
  version_t seq = 0;
  mds_rank_t rank = MDS_RANK_NONE;
  std::map<entity_inst_t, Metrics> client_metrics_map;

  metrics_message_t() {
  }
  metrics_message_t(version_t seq, mds_rank_t rank)
    : seq(seq), rank(rank) {
  }

  void encode(bufferlist &bl, uint64_t features) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(seq, bl);
    encode(rank, bl);
    encode(client_metrics_map, bl, features);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(seq, iter);
    decode(rank, iter);
    decode(client_metrics_map, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_unsigned("seq", seq);
    f->dump_int("rank", rank);
    for (auto &[client, metrics] : client_metrics_map) {
      f->dump_object("client", client);
      f->dump_object("metrics", metrics);
    }
  }

  friend std::ostream& operator<<(std::ostream& os, const metrics_message_t &metrics_message) {
    os << "[sequence=" << metrics_message.seq << ", rank=" << metrics_message.rank
       << ", metrics=" << metrics_message.client_metrics_map << "]";
    return os;
  }
};

WRITE_CLASS_ENCODER_FEATURES(metrics_message_t)

#endif // CEPH_MDS_PERF_METRIC_TYPES_H
