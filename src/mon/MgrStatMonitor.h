// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/Context.h"
#include "PaxosService.h"
#include "mon/PGMap.h"
#include "mgr/ServiceMap.h"

class MgrStatMonitor : public PaxosService {
  // live version
  version_t version = 0;
  PGMapDigest digest;
  ServiceMap service_map;
  std::map<std::string,ProgressEvent> progress_events;

  // pending commit
  PGMapDigest pending_digest;
  health_check_map_t pending_health_checks;
  std::map<std::string,ProgressEvent> pending_progress_events;
  bufferlist pending_service_map_bl;

public:
  MgrStatMonitor(Monitor *mn, Paxos *p, const string& service_name);
  ~MgrStatMonitor() override;

  void init() override {}
  void on_shutdown() override {}

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  bool definitely_converted_snapsets() const {
    return digest.definitely_converted_snapsets();
  }

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  bool preprocess_report(MonOpRequestRef op);
  bool prepare_report(MonOpRequestRef op);

  bool preprocess_getpoolstats(MonOpRequestRef op);
  bool preprocess_statfs(MonOpRequestRef op);

  void check_sub(Subscription *sub);
  void check_subs();
  void send_digests();

  void on_active() override;
  void tick() override;

  uint64_t get_last_osd_stat_seq(int osd) {
    return digest.get_last_osd_stat_seq(osd);
  }

  void update_logger();

  const ServiceMap& get_service_map() const {
    return service_map;
  }

  const std::map<std::string,ProgressEvent>& get_progress_events() {
    return progress_events;
  }

  // pg stat access
  const pool_stat_t* get_pool_stat(int64_t poolid) const {
    auto i = digest.pg_pool_sum.find(poolid);
    if (i != digest.pg_pool_sum.end()) {
      return &i->second;
    }
    return nullptr;
  }

  const PGMapDigest& get_digest() {
    return digest;
  }

  ceph_statfs get_statfs(OSDMap& osdmap,
			 boost::optional<int64_t> data_pool) const {
    return digest.get_statfs(osdmap, data_pool);
  }

  void print_summary(Formatter *f, ostream *out) const {
    digest.print_summary(f, out);
  }
  void dump_info(Formatter *f) const {
    digest.dump(f);
    f->dump_object("servicemap", get_service_map());
  }
  void dump_cluster_stats(stringstream *ss,
		     Formatter *f,
		     bool verbose) const {
    digest.dump_cluster_stats(ss, f, verbose);
  }
  void dump_pool_stats(const OSDMap& osdm, stringstream *ss, Formatter *f,
		       bool verbose) const {
    digest.dump_pool_stats_full(osdm, ss, f, verbose);
  }
};
