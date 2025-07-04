// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/Context.h"
#include "PaxosService.h"
#include "mon/PGMap.h"
#include "mgr/ServiceMap.h"

 class MgrStatMonitor : public PaxosService, 
                        public md_config_obs_t {
  // live version
  version_t version = 0;
  PGMapDigest digest;
  ServiceMap service_map;
  std::map<std::string,ProgressEvent> progress_events;
  std::map<uint64_t, PoolAvailability> pool_availability;

  // pending commit
  PGMapDigest pending_digest;
  health_check_map_t pending_health_checks;
  std::map<std::string,ProgressEvent> pending_progress_events;
  ceph::buffer::list pending_service_map_bl;
  std::map<uint64_t, PoolAvailability> pending_pool_availability;

public:
  MgrStatMonitor(Monitor &mn, Paxos &p, const std::string& service_name);
  ~MgrStatMonitor() override;

  ceph::mutex lock = ceph::make_mutex("MgrStatMonitor::lock");

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

  void calc_pool_availability();
  bool enable_availability_tracking = g_conf().get_val<bool>("enable_availability_tracking"); ///< tracking availability score feature 
  
  void clear_pool_availability(int64_t poolid);

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

  const std::map<uint64_t, PoolAvailability>& get_pool_availability() {
    return pool_availability;
  }

  ceph_statfs get_statfs(OSDMap& osdmap,
			 std::optional<int64_t> data_pool) const {
    return digest.get_statfs(osdmap, data_pool);
  }

  void print_summary(ceph::Formatter *f, std::ostream *out) const {
    digest.print_summary(f, out);
  }
  void dump_info(ceph::Formatter *f) const {
    digest.dump(f);
    f->dump_object("servicemap", get_service_map());
    f->dump_unsigned("mgrstat_first_committed", get_first_committed());
    f->dump_unsigned("mgrstat_last_committed", get_last_committed());
  }
  void dump_cluster_stats(std::stringstream *ss,
			  ceph::Formatter *f,
			  bool verbose) const {
    digest.dump_cluster_stats(ss, f, verbose);
  }
  void dump_pool_stats(const OSDMap& osdm, std::stringstream *ss, ceph::Formatter *f,
		       bool verbose) const {
    digest.dump_pool_stats_full(osdm, ss, f, verbose);
  }

  // config observer 
  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set <std::string> &changed) override;
};
