// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "ConfigMap.h"
#include "common/cmdparse.h"
#include "mon/PaxosService.h"

#include <map>
#include <optional>
#include <string>

class MonSession;
struct Subscription;

class ConfigMonitor : public PaxosService
{
  version_t version = 0;
  ConfigMap config_map;
  std::map<std::string,std::optional<ceph::buffer::list>> pending;
  std::string pending_description;
  std::map<std::string,std::optional<ceph::buffer::list>> pending_cleanup;

  std::map<std::string,ceph::buffer::list> current;

  void encode_pending_to_kvmon();

  int prepare_pool_default_set(const cmdmap_t& cmdmap, std::ostream& ss);
  void dump_pool_defaults(ceph::Formatter *f, std::ostream& out);
  std::string pool_default_source(const std::string& option);

public:
  ConfigMonitor(Monitor &m, Paxos &p, const std::string& service_name);

  // The pool default options that the configuration database sets for this
  // monitor other than by an unmasked global value, with the section that
  // sets them, as this monitor evaluates its masks.
  std::map<std::string, std::string> pool_default_db_overrides();

  void init() override;

  void load_config();
  void load_changeset(version_t v, ConfigChangeSet *ch);

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void handle_get_config(MonOpRequestRef op);

  const ConfigMap& get_config_map() const {
    return config_map;
  }

  // Fails if a configuration database section or this monitor's local
  // configuration would override the global value of one of the options.
  int check_pool_default_overrides(
    const std::map<std::string,std::string>& values, std::ostream& ss);

  // Write global values of options and propose them. The caller checks that
  // this service and KVMonitor are writeable.
  void propose_global_options(const std::map<std::string,std::string>& values,
                              const std::string& description);

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void on_active() override;
  void tick() override;

  bool refresh_config(MonSession *s);
  bool maybe_send_config(MonSession *s);
  void send_config(MonSession *s);
  void check_sub(MonSession *s);
  void check_sub(Subscription *sub);
  void check_all_subs();
};
