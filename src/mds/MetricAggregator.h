// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MDS_METRIC_AGGREGATOR_H
#define CEPH_MDS_METRIC_AGGREGATOR_H

#include <map>
#include <set>
#include <thread>

#include "msg/msg_types.h"
#include "msg/Dispatcher.h"
#include "common/ceph_mutex.h"
#include "messages/MMDSMetrics.h"

#include "mgr/MDSPerfMetricTypes.h"

#include "mdstypes.h"
#include "MDSMap.h"
#include "MDSPinger.h"

class MDSRank;
class MgrClient;
class CephContext;

class MetricAggregator : public Dispatcher {
public:
  MetricAggregator(CephContext *cct, MDSRank *mds, MgrClient *mgrc);

  int init();
  void shutdown();

  void notify_mdsmap(const MDSMap &mdsmap);

  bool ms_can_fast_dispatch_any() const override {
    return true;
  }
  bool ms_can_fast_dispatch2(const cref_t<Message> &m) const override;
  void ms_fast_dispatch2(const ref_t<Message> &m) override;
  bool ms_dispatch2(const ref_t<Message> &m) override;

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

private:
  // drop this lock when calling ->send_message_mds() else mds might
  // deadlock
  ceph::mutex lock = ceph::make_mutex("MetricAggregator::lock");
  MDSRank *mds;
  MgrClient *mgrc;

  // maintain a map of rank to list of clients so that when a rank
  // goes away we cull metrics of clients connected to that rank.
  std::map<mds_rank_t, std::unordered_set<entity_inst_t>> clients_by_rank;

  // user query to metrics map
  std::map<MDSPerfMetricQuery, std::map<MDSPerfMetricKey, PerformanceCounters>> query_metrics_map;

  MDSPinger mds_pinger;
  std::thread pinger;

  std::map<mds_rank_t, entity_addrvec_t> active_rank_addrs;

  bool stopping = false;

  void handle_mds_metrics(const cref_t<MMDSMetrics> &m);

  void refresh_metrics_for_rank(const entity_inst_t &client, mds_rank_t rank,
                                const Metrics &metrics);
  void remove_metrics_for_rank(const entity_inst_t &client, mds_rank_t rank, bool remove);

  void cull_metrics_for_rank(mds_rank_t rank);

  void ping_all_active_ranks();
};

#endif // CEPH_MDS_METRIC_AGGREGATOR_H
