// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/Context.h"
#include "PaxosService.h"

class PGStatService;
class MgrPGStatService;

class MgrStatMonitor : public PaxosService {
  version_t version = 0;
  std::unique_ptr<MgrPGStatService> pgservice;
  list<pair<health_status_t,string>> health_summary, health_detail;

public:
  MgrStatMonitor(Monitor *mn, Paxos *p, const string& service_name);
  ~MgrStatMonitor() override;

  void init() override {}
  void on_shutdown() override {}

  void create_initial() override {}
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override {}
  void encode_pending(MonitorDBStore::TransactionRef t) override;

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  bool preprocess_report(MonOpRequestRef op);
  bool prepare_report(MonOpRequestRef op);

  void check_sub(Subscription *sub);
  void check_subs();
  void send_digests();

  void on_active() override;
  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;
  void tick() override;

  void print_summary(Formatter *f, std::ostream *ss) const;

  PGStatService *get_pg_stat_service();

  friend class C_Updated;
};
