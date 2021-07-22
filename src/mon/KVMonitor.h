// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <optional>

#include "mon/PaxosService.h"

class MonSession;

extern const std::string KV_PREFIX;

class KVMonitor : public PaxosService
{
  version_t version = 0;
  std::map<std::string,std::optional<ceph::buffer::list>> pending;

  bool _have_prefix(const string &prefix);

public:
  KVMonitor(Monitor &m, Paxos &p, const std::string& service_name);

  void init() override;

  void get_store_prefixes(set<string>& s) const override;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);
  
  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void on_active() override;
  void tick() override;

  int validate_osd_destroy(const int32_t id, const uuid_d& uuid);
  void do_osd_destroy(int32_t id, uuid_d& uuid);
  int validate_osd_new(
      const uuid_d& uuid,
      const std::string& dmcrypt_key,
      std::stringstream& ss);
  void do_osd_new(const uuid_d& uuid, const std::string& dmcrypt_key);

  void check_sub(MonSession *s);
  void check_sub(Subscription *sub);
  void check_all_subs();

  bool maybe_send_update(Subscription *sub);


  // used by other services to adjust kv content; note that callers MUST ensure that
  // propose_pending() is called and a commit is forced to provide atomicity and
  // proper subscriber notifications.
  void enqueue_set(const std::string& key, bufferlist &v) {
    pending[key] = v;
  }
  void enqueue_rm(const std::string& key) {
    pending[key].reset();
  }
};
