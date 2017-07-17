// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/Context.h"
#include "PaxosService.h"
#include "mon/PGMap.h"
#include "mgr/ServiceMap.h"

class MonPGStatService;
class MgrPGStatService;

class MgrStatMonitor : public PaxosService {
  // live version
  version_t version = 0;
  PGMapDigest digest;
  ServiceMap service_map;

  // pending commit
  PGMapDigest pending_digest;
  health_check_map_t pending_health_checks;
  bufferlist pending_service_map_bl;

  std::unique_ptr<MgrPGStatService> pgservice;

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
  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;
  void tick() override;

  uint64_t get_last_osd_stat_seq(int osd) {
    return digest.get_last_osd_stat_seq(osd);
  }

  void update_logger();

  void print_summary(Formatter *f, std::ostream *ss) const;

  MonPGStatService *get_pg_stat_service();
  const ServiceMap& get_service_map() {
    return service_map;
  }

  friend class C_Updated;
};
