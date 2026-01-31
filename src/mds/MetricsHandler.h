// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MDS_METRICS_HANDLER_H
#define CEPH_MDS_METRICS_HANDLER_H

#include <chrono>
#include <map>
#include <mutex>
#include <unordered_map>
#include <thread>
#include <utility>

#include "msg/Dispatcher.h"
#include "common/ceph_mutex.h"

#include "mds/MDSPerfMetricTypes.h"
#include "include/cephfs/metrics/Types.h"

#include <boost/optional.hpp>
#include <boost/variant/static_visitor.hpp>

struct CapInfoPayload;
struct ReadLatencyPayload;
struct WriteLatencyPayload;
struct MetadataLatencyPayload;
struct DentryLeasePayload;
struct OpenedFilesPayload;
struct PinnedIcapsPayload;
struct OpenedInodesPayload;
struct ReadIoSizesPayload;
struct WriteIoSizesPayload;
struct SubvolumeMetricsPayload;
struct UnknownPayload;
struct AggregatedIOMetrics;
class MClientMetrics;
class MDSMap;
class MDSRank;
class MMDSPing;
class Session;

class MetricsHandler : public Dispatcher {
public:
  MetricsHandler(CephContext *cct, MDSRank *mds);

  Dispatcher::dispatch_result_t ms_dispatch2(const ref_t<Message> &m) override;

  void ms_handle_connect(Connection *c) override {
  }
  bool ms_handle_reset(Connection *c) override {
    return false;
  }
  void ms_handle_remote_reset(Connection *c) override {
  }
  bool ms_handle_refused(Connection *c) override {
    return false;
  }

  void add_session(Session *session);
  void remove_session(Session *session);

  void init();
  void shutdown();

  void notify_mdsmap(const MDSMap &mdsmap);

  // Called from MDCache::broadcast_quota_to_client to update quota for subvolumes
  // quota_bytes: only updated if > 0, unless force_zero is true (quota removed)
  // used_bytes is fetched dynamically from inode rstat in aggregate_subvolume_metrics
  void maybe_update_subvolume_quota(inodeno_t subvol_id, uint64_t quota_bytes, uint64_t used_bytes, bool force_zero = false);

private:
  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    MetricsHandler *metrics_handler;
    Session *session;

    HandlePayloadVisitor(MetricsHandler *metrics_handler, Session *session)
      : metrics_handler(metrics_handler), session(session) {
    }

    template <typename ClientMetricPayload>
    inline void operator()(const ClientMetricPayload &payload) const {
      metrics_handler->handle_payload(session, payload);
    }
    
    // Specialization for SubvolumeMetricsPayload - should not be called
    // as it's handled specially in handle_client_metrics
    // just for the compiler to be happy with visitor pattern
    inline void operator()(const SubvolumeMetricsPayload &) const {
      ceph_abort_msg("SubvolumeMetricsPayload should be handled specially");
    }
  };

  std::unique_ptr<PerfCounters> create_subv_perf_counter(const std::string& subv_name);

  MDSRank *mds;
  // drop this lock when calling ->send_message_mds() else mds might
  // deadlock
  ceph::mutex lock = ceph::make_mutex("MetricsHandler::lock");

  // ISN sent by rank0 pinger is 1
  version_t next_seq = 0;

  // sequence number incremented on each update sent to rank 0.
  // this is nowhere related to next_seq and is completely used
  // locally to figure out if a session got added and removed
  // within an update to rank 0.
  version_t last_updated_seq = 0;

  std::thread updater;
  std::map<entity_inst_t, std::pair<version_t, Metrics>> client_metrics_map;
  // maps subvolume path -> aggregated metrics from all clients reporting to this MDS instance
  std::unordered_map<std::string, std::vector<AggregatedIOMetrics>> subvolume_metrics_map;
  uint64_t subv_window_sec = 0;

  // maps subvolume_id (inode number) -> quota info, updated when quota is broadcast to clients
  // used_bytes is fetched dynamically from inode rstat, not cached here
  struct SubvolumeQuotaInfo {
    uint64_t quota_bytes = 0;
    uint64_t used_bytes = 0;
    std::chrono::steady_clock::time_point last_activity;
  };
  std::unordered_map<inodeno_t, SubvolumeQuotaInfo> subvolume_quota;
  // address of rank 0 mds, so that the message can be sent withoutå
  // acquiring mds_lock. misdirected messages to rank 0 are taken
  // care of by rank 0.
  boost::optional<entity_addrvec_t> addr_rank0;

  bool stopping = false;

  void handle_payload(Session *session, const CapInfoPayload &payload);
  void handle_payload(Session *session, const ReadLatencyPayload &payload);
  void handle_payload(Session *session, const WriteLatencyPayload &payload);
  void handle_payload(Session *session, const MetadataLatencyPayload &payload);
  void handle_payload(Session *session, const DentryLeasePayload &payload);
  void handle_payload(Session *session, const OpenedFilesPayload &payload);
  void handle_payload(Session *session, const PinnedIcapsPayload &payload);
  void handle_payload(Session *session, const OpenedInodesPayload &payload);
  void handle_payload(Session *session, const ReadIoSizesPayload &payload);
  void handle_payload(Session *session, const WriteIoSizesPayload &payload);
  void handle_payload(Session *session, const SubvolumeMetricsPayload &payload, std::unique_lock<ceph::mutex> &lock_guard);
  void handle_payload(Session *session, const UnknownPayload &payload);

  void set_next_seq(version_t seq);
  void reset_seq();

  void handle_client_metrics(const cref_t<MClientMetrics> &m);
  void handle_mds_ping(const cref_t<MMDSPing> &m);

  void update_rank0(std::unique_lock<ceph::mutex>& locker);

  // RAII helper to temporarily unlock/relock a unique_lock
  struct UnlockGuard {
    std::unique_lock<ceph::mutex>& lk;
    explicit UnlockGuard(std::unique_lock<ceph::mutex>& l) : lk(l) { lk.unlock(); }
    ~UnlockGuard() noexcept {
      if (!lk.owns_lock()) {
        try { lk.lock(); } catch (...) {
          // avoid throwing from destructor
        }
      }
    }
  };

  void aggregate_subvolume_metrics(const std::string& subvolume_path,
                                   const std::vector<AggregatedIOMetrics>& metrics_list,
                                   const std::unordered_map<inodeno_t, uint64_t>& subvol_used_bytes,
                                   SubvolumeMetric &res);

  void sample_cpu_usage();
  void sample_open_requests();

  PerfCounters *rank_perf_counters = nullptr;
  long clk_tck = 0;
  struct RankTelemetry {
    RankPerfMetrics metrics;
    bool cpu_sample_initialized = false;
    uint64_t last_cpu_total_ticks = 0;
    std::chrono::steady_clock::time_point last_cpu_sample_time{};
  } rank_telemetry;
};

#endif // CEPH_MDS_METRICS_HANDLER_H
