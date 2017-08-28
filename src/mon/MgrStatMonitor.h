// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/Context.h"
#include "PaxosService.h"
#include "mon/PGMap.h"
#include "mgr/ServiceMap.h"

class PGStatService;
class MgrPGStatService;

class MgrStatMonitor : public PaxosService,
		       public PGStatService {
  // live version
  version_t version = 0;
  PGMapDigest digest;
  ServiceMap service_map;

  // pending commit
  PGMapDigest pending_digest;
  health_check_map_t pending_health_checks;
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
  version_t get_trim_to() override;

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

  const ServiceMap& get_service_map() {
    return service_map;
  }

  // PGStatService
  const pool_stat_t* get_pool_stat(int64_t poolid) const override {
    auto i = digest.pg_pool_sum.find(poolid);
    if (i != digest.pg_pool_sum.end()) {
      return &i->second;
    }
    return nullptr;
  }

  ceph_statfs get_statfs(OSDMap& osdmap,
			 boost::optional<int64_t> data_pool) const override {
    return digest.get_statfs(osdmap, data_pool);
  }

  void print_summary(Formatter *f, ostream *out) const override {
    digest.print_summary(f, out);
  }
  void dump_info(Formatter *f) const override {
    digest.dump(f);
  }
  void dump_fs_stats(stringstream *ss,
		     Formatter *f,
		     bool verbose) const override {
    digest.dump_fs_stats(ss, f, verbose);
  }
  void dump_pool_stats(const OSDMap& osdm, stringstream *ss, Formatter *f,
		       bool verbose) const override {
    digest.dump_pool_stats_full(osdm, ss, f, verbose);
  }

  friend class C_Updated;
};
