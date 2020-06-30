// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_LOGMONITOR_H
#define CEPH_LOGMONITOR_H

#include <map>
#include <set>

#include "include/types.h"
#include "PaxosService.h"

#include "common/config_fwd.h"
#include "common/LogEntry.h"
#include "include/str_map.h"

class MLog;

static const std::string LOG_META_CHANNEL = "$channel";

namespace ceph {
namespace logging {
  class Graylog;
}
}

class LogMonitor : public PaxosService,
                   public md_config_obs_t {
private:
  std::multimap<utime_t,LogEntry> pending_log;
  LogSummary pending_summary, summary;

  struct log_channel_info {

    std::map<std::string,std::string> log_to_syslog;
    std::map<std::string,std::string> syslog_level;
    std::map<std::string,std::string> syslog_facility;
    std::map<std::string,std::string> log_file;
    std::map<std::string,std::string> expanded_log_file;
    std::map<std::string,std::string> log_file_level;
    std::map<std::string,std::string> log_to_graylog;
    std::map<std::string,std::string> log_to_graylog_host;
    std::map<std::string,std::string> log_to_graylog_port;

    std::map<std::string, std::shared_ptr<ceph::logging::Graylog>> graylogs;
    uuid_d fsid;
    std::string host;

    void clear() {
      log_to_syslog.clear();
      syslog_level.clear();
      syslog_facility.clear();
      log_file.clear();
      expanded_log_file.clear();
      log_file_level.clear();
      log_to_graylog.clear();
      log_to_graylog_host.clear();
      log_to_graylog_port.clear();
      graylogs.clear();
    }

    /** expands $channel meta variable on all maps *EXCEPT* log_file
     *
     * We won't expand the log_file map meta variables here because we
     * intend to do that selectively during get_log_file()
     */
    void expand_channel_meta() {
      expand_channel_meta(log_to_syslog);
      expand_channel_meta(syslog_level);
      expand_channel_meta(syslog_facility);
      expand_channel_meta(log_file_level);
    }
    void expand_channel_meta(std::map<std::string,std::string> &m);
    std::string expand_channel_meta(const std::string &input,
				    const std::string &change_to);

    bool do_log_to_syslog(const std::string &channel);

    std::string get_facility(const std::string &channel) {
      return get_str_map_key(syslog_facility, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
    }

    std::string get_level(const std::string &channel) {
      return get_str_map_key(syslog_level, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
    }

    std::string get_log_file(const std::string &channel);

    std::string get_log_file_level(const std::string &channel) {
      return get_str_map_key(log_file_level, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
    }

    bool do_log_to_graylog(const std::string &channel) {
      return (get_str_map_key(log_to_graylog, channel,
			      &CLOG_CONFIG_DEFAULT_KEY) == "true");
    }

    std::shared_ptr<ceph::logging::Graylog> get_graylog(const std::string &channel);
  } channels;

  void update_log_channels();

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;  // prepare a new pending
  // propose pending update to peers
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  void encode_full(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;
  bool preprocess_query(MonOpRequestRef op) override;  // true if processed.
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_log(MonOpRequestRef op);
  bool prepare_log(MonOpRequestRef op);
  void _updated_log(MonOpRequestRef op);

  bool should_propose(double& delay) override;

  bool should_stash_full() override {
    // commit a LogSummary on every commit
    return true;
  }

  struct C_Log;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void _create_sub_incremental(MLog *mlog, int level, version_t sv);

 public:
  LogMonitor(Monitor *mn, Paxos *p, const std::string& service_name) 
    : PaxosService(mn, p, service_name) { }

  void init() override {
    generic_dout(10) << "LogMonitor::init" << dendl;
    g_conf().add_observer(this);
    update_log_channels();
  }
  
  void tick() override;  // check state, take actions

  void check_subs();
  void check_sub(Subscription *s);

  /**
   * translate log sub name ('log-info') to integer id
   *
   * @param n name
   * @return id, or -1 if unrecognized
   */
  int sub_name_to_id(const std::string& n);

  void on_shutdown() override {
    g_conf().remove_observer(this);
  }

  const char **get_tracked_conf_keys() const override {
    static const char* KEYS[] = {
      "mon_cluster_log_to_syslog",
      "mon_cluster_log_to_syslog_level",
      "mon_cluster_log_to_syslog_facility",
      "mon_cluster_log_file",
      "mon_cluster_log_file_level",
      "mon_cluster_log_to_graylog",
      "mon_cluster_log_to_graylog_host",
      "mon_cluster_log_to_graylog_port",
      NULL
    };
    return KEYS;
  }
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string> &changed) override;
};
#endif
