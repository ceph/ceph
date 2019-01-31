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

static const string LOG_META_CHANNEL = "$channel";

namespace ceph {
namespace logging {
  class Graylog;
}
}

class LogMonitor : public PaxosService,
                   public md_config_obs_t {
private:
  multimap<utime_t,LogEntry> pending_log;
  LogSummary pending_summary, summary;

  struct log_channel_info {

    map<string,string> log_to_syslog;
    map<string,string> syslog_level;
    map<string,string> syslog_facility;
    map<string,string> log_file;
    map<string,string> expanded_log_file;
    map<string,string> log_file_level;
    map<string,string> log_to_graylog;
    map<string,string> log_to_graylog_host;
    map<string,string> log_to_graylog_port;

    map<string, shared_ptr<ceph::logging::Graylog>> graylogs;
    uuid_d fsid;
    string host;

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
    void expand_channel_meta(map<string,string> &m);
    string expand_channel_meta(const string &input,
                               const string &change_to);

    bool do_log_to_syslog(const string &channel);

    string get_facility(const string &channel) {
      return get_str_map_key(syslog_facility, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
    }

    string get_level(const string &channel) {
      return get_str_map_key(syslog_level, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
    }

    string get_log_file(const string &channel) {
      generic_dout(25) << __func__ << " for channel '"
                       << channel << "'" << dendl;

      if (expanded_log_file.count(channel) == 0) {
        string fname = expand_channel_meta(
            get_str_map_key(log_file, channel, &CLOG_CONFIG_DEFAULT_KEY),
            channel);
        expanded_log_file[channel] = fname;

        generic_dout(20) << __func__ << " for channel '"
                         << channel << "' expanded to '"
                         << fname << "'" << dendl;
      }
      return expanded_log_file[channel];
    }

    string get_log_file_level(const string &channel) {
      return get_str_map_key(log_file_level, channel,
                             &CLOG_CONFIG_DEFAULT_KEY);
    }

    bool do_log_to_graylog(const string &channel) {
      return (get_str_map_key(log_to_graylog, channel,
			      &CLOG_CONFIG_DEFAULT_KEY) == "true");
    }

    shared_ptr<ceph::logging::Graylog> get_graylog(const string &channel);
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
  LogMonitor(Monitor *mn, Paxos *p, const string& service_name) 
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
  int sub_name_to_id(const string& n);

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
