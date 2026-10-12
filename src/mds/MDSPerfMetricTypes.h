// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MDS_PERF_METRIC_TYPES_H
#define CEPH_MDS_PERF_METRIC_TYPES_H

#include <iosfwd>
#include <shared_mutex>

#include "include/cephfs/types.h" // for mds_rank_t
#include "include/denc.h"
#include "include/utime.h"
#include "mdstypes.h"

namespace ceph { class Formatter; }

enum UpdateType : uint32_t {
  UPDATE_TYPE_REFRESH = 0,
  UPDATE_TYPE_REMOVE,
};

inline constexpr uint32_t l_mds_rank_perf_start = 40000;
inline constexpr uint32_t l_mds_rank_perf_cpu_usage = l_mds_rank_perf_start + 1;
inline constexpr uint32_t l_mds_rank_perf_open_requests = l_mds_rank_perf_start + 2;
inline constexpr uint32_t l_mds_rank_perf_last = l_mds_rank_perf_start + 3;

struct CapHitMetric {
  uint64_t hits = 0;
  uint64_t misses = 0;

  DENC(CapHitMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.hits, p);
    denc(v.misses, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const CapHitMetric &metric);
};

struct ReadLatencyMetric {
  utime_t lat;
  utime_t mean;
  uint64_t sq_sum;
  uint64_t count;
  bool updated = false;

  DENC(ReadLatencyMetric, v, p) {
    DENC_START(3, 1, p);
    denc(v.lat, p);
    if (struct_v >= 2)
      denc(v.updated, p);
    if (struct_v >= 3) {
      denc(v.mean, p);
      denc(v.sq_sum, p);
      denc(v.count, p);
    }
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const ReadLatencyMetric &metric);
};

struct WriteLatencyMetric {
  utime_t lat;
  utime_t mean;
  uint64_t sq_sum;
  uint64_t count;
  bool updated = false;

  DENC(WriteLatencyMetric, v, p) {
    DENC_START(3, 1, p);
    denc(v.lat, p);
    if (struct_v >= 2)
      denc(v.updated, p);
    if (struct_v >= 3) {
      denc(v.mean, p);
      denc(v.sq_sum, p);
      denc(v.count, p);
    }
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const WriteLatencyMetric &metric);
};

struct MetadataLatencyMetric {
  utime_t lat;
  utime_t mean;
  uint64_t sq_sum;
  uint64_t count;
  bool updated = false;

  DENC(MetadataLatencyMetric, v, p) {
    DENC_START(3, 1, p);
    denc(v.lat, p);
    if (struct_v >= 2)
      denc(v.updated, p);
    if (struct_v >= 3) {
      denc(v.mean, p);
      denc(v.sq_sum, p);
      denc(v.count, p);
    }
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const MetadataLatencyMetric &metric);
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

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const DentryLeaseHitMetric &metric);
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

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const OpenedFilesMetric &metric);
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

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const PinnedIcapsMetric &metric);
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

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const OpenedInodesMetric &metric);
};

struct ReadIoSizesMetric {
  uint64_t total_ops = 0;
  uint64_t total_size = 0;
  bool updated = false;

  DENC(ReadIoSizesMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.total_ops, p);
    denc(v.total_size, p);
    denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const ReadIoSizesMetric &metric);
};

struct WriteIoSizesMetric {
  uint64_t total_ops = 0;
  uint64_t total_size = 0;
  bool updated = false;

  DENC(WriteIoSizesMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.total_ops, p);
    denc(v.total_size, p);
    denc(v.updated, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const WriteIoSizesMetric &metric);
};

WRITE_CLASS_DENC(CapHitMetric)
WRITE_CLASS_DENC(ReadLatencyMetric)
WRITE_CLASS_DENC(WriteLatencyMetric)
WRITE_CLASS_DENC(MetadataLatencyMetric)
WRITE_CLASS_DENC(DentryLeaseHitMetric)
WRITE_CLASS_DENC(OpenedFilesMetric)
WRITE_CLASS_DENC(PinnedIcapsMetric)
WRITE_CLASS_DENC(OpenedInodesMetric)
WRITE_CLASS_DENC(ReadIoSizesMetric)
WRITE_CLASS_DENC(WriteIoSizesMetric)
WRITE_CLASS_DENC(SubvolumeMetric)

struct RankPerfMetrics {
  uint64_t cpu_usage_percent = 0;
  uint64_t open_requests = 0;

  DENC(RankPerfMetrics, v, p) {
    DENC_START(1, 1, p);
    denc(v.cpu_usage_percent, p);
    denc(v.open_requests, p);
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const RankPerfMetrics &metric);
};
WRITE_CLASS_DENC(RankPerfMetrics)

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
  ReadIoSizesMetric read_io_sizes_metric;
  WriteIoSizesMetric write_io_sizes_metric;

  // metric update type
  uint32_t update_type = UpdateType::UPDATE_TYPE_REFRESH;

  DENC(Metrics, v, p) {
    DENC_START(4, 1, p);
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
    if (struct_v >= 4) {
      denc(v.read_io_sizes_metric, p);
      denc(v.write_io_sizes_metric, p);
    }
    DENC_FINISH(p);
  }

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const Metrics& metrics);
};
WRITE_CLASS_DENC(Metrics)

struct metrics_message_t {
  version_t seq = 0;
  mds_rank_t rank = MDS_RANK_NONE;
  std::map<entity_inst_t, Metrics> client_metrics_map;
  std::vector<SubvolumeMetric> subvolume_metrics;
  RankPerfMetrics rank_metrics;

  metrics_message_t() {
  }
  metrics_message_t(version_t seq, mds_rank_t rank)
    : seq(seq), rank(rank) {
  }

  void encode(bufferlist &bl, uint64_t features) const;
  void decode(bufferlist::const_iterator &iter);

  void dump(Formatter *f) const;
  friend std::ostream& operator<<(std::ostream& os, const metrics_message_t &m);
};

WRITE_CLASS_ENCODER_FEATURES(metrics_message_t)

#endif // CEPH_MDS_PERF_METRIC_TYPES_H
