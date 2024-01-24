// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_MGRMONITOR_H
#define CEPH_MGRMONITOR_H

#include <map>
#include <set>

#include "include/Context.h"
#include "MgrMap.h"
#include "PaxosService.h"
#include "MonCommand.h"
#include "CommandHandler.h"

class MgrMonitor: public PaxosService, public CommandHandler
{
  MgrMap map;
  MgrMap pending_map;
  bool ever_had_active_mgr = false;

  std::map<std::string, ceph::buffer::list> pending_metadata;
  std::set<std::string> pending_metadata_rm;

  std::map<std::string,Option> mgr_module_options;
  std::list<std::string> misc_option_strings;

  utime_t first_seen_inactive;

  std::map<uint64_t, ceph::coarse_mono_clock::time_point> last_beacon;

  /**
   * If a standby is available, make it active, given that
   * there is currently no active daemon.
   *
   * @return true if a standby was promoted
   */
  bool promote_standby();

  /**
   * Drop the active daemon from the MgrMap. No promotion is performed.
   *
   * @return whether PAXOS was plugged by this method
   */
  bool drop_active();

  /**
   * Remove this gid from the list of standbys.  By default,
   * also remove metadata (i.e. forget the daemon entirely).
   *
   * Set `drop_meta` to false if you would like to keep
   * the daemon's metadata, for example if you're dropping
   * it as a standby before reinstating it as the active daemon.
   */
  void drop_standby(uint64_t gid, bool drop_meta=true);

  Context *digest_event = nullptr;
  void cancel_timer();

  std::vector<health_check_map_t> prev_health_checks;

  bool check_caps(MonOpRequestRef op, const uuid_d& fsid);

  health_status_t should_warn_about_mgr_down();

  // Command descriptions we've learned from the active mgr
  std::vector<MonCommand> command_descs;
  std::vector<MonCommand> pending_command_descs;

public:
  MgrMonitor(Monitor &mn, Paxos &p, const std::string& service_name)
    : PaxosService(mn, p, service_name)
  {}
  ~MgrMonitor() override {}

  void init() override;
  void on_shutdown() override;

  const MgrMap &get_map() const { return map; }

  const std::map<std::string,Option>& get_mgr_module_options() {
    return mgr_module_options;
  }
  const Option *find_module_option(const std::string& name);

  bool in_use() const { return map.epoch > 0; }

  version_t get_trim_to() const override;

  void prime_mgr_client();

  void create_initial() override;
  void get_store_prefixes(std::set<std::string>& s) const override;
  void update_from_paxos(bool *need_bootstrap) override;
  void post_paxos_update() override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  void check_sub(Subscription *sub);
  void check_subs();
  void send_digests();

  void on_active() override;
  void on_restart() override;

  void tick() override;

  void print_summary(ceph::Formatter *f, std::ostream *ss) const;

  const std::vector<MonCommand> &get_command_descs() const;

  int load_metadata(const std::string& name, std::map<std::string, std::string>& m,
		    std::ostream *err) const;
  int dump_metadata(const std::string& name, ceph::Formatter *f, std::ostream *err);
  void print_nodes(ceph::Formatter *f) const;
  void count_metadata(const std::string& field, ceph::Formatter *f);
  void count_metadata(const std::string& field, std::map<std::string,int> *out);
  void get_versions(std::map<std::string, std::list<std::string>> &versions);

  // When did the mon last call into our tick() method?  Used for detecting
  // when the mon was not updating us for some period (e.g. during slow
  // election) to reset last_beacon timeouts
  ceph::coarse_mono_clock::time_point last_tick;
};

#endif
