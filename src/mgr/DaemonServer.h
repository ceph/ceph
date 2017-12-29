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

#ifndef DAEMON_SERVER_H_
#define DAEMON_SERVER_H_

#include "PyModuleRegistry.h"

#include <set>
#include <string>

#include "common/Mutex.h"
#include "common/LogClient.h"

#include <msg/Messenger.h>
#include <mon/MonClient.h>

#include "auth/AuthAuthorizeHandler.h"

#include "ServiceMap.h"
#include "MgrSession.h"
#include "DaemonState.h"

class MMgrReport;
class MMgrOpen;
class MMonMgrReport;
class MCommand;
struct MonCommand;


/**
 * Server used in ceph-mgr to communicate with Ceph daemons like
 * MDSs and OSDs.
 */
class DaemonServer : public Dispatcher, public md_config_obs_t
{
protected:
  boost::scoped_ptr<Throttle> client_byte_throttler;
  boost::scoped_ptr<Throttle> client_msg_throttler;
  boost::scoped_ptr<Throttle> osd_byte_throttler;
  boost::scoped_ptr<Throttle> osd_msg_throttler;
  boost::scoped_ptr<Throttle> mds_byte_throttler;
  boost::scoped_ptr<Throttle> mds_msg_throttler;
  boost::scoped_ptr<Throttle> mon_byte_throttler;
  boost::scoped_ptr<Throttle> mon_msg_throttler;

  Messenger *msgr;
  MonClient *monc;
  Finisher  &finisher;
  DaemonStateIndex &daemon_state;
  ClusterState &cluster_state;
  PyModuleRegistry &py_modules;
  LogChannelRef clog, audit_clog;

  AuthAuthorizeHandlerRegistry auth_registry;

  // Connections for daemons, and clients with service names set
  // (i.e. those MgrClients that are allowed to send MMgrReports)
  std::set<ConnectionRef> daemon_connections;

  /// connections for osds
  ceph::unordered_map<int,set<ConnectionRef>> osd_cons;

  ServiceMap pending_service_map;  // uncommitted

  epoch_t pending_service_map_dirty = 0;

  Mutex lock;

  static void _generate_command_map(map<string,cmd_vartype>& cmdmap,
                                    map<string,string> &param_str_map);
  static const MonCommand *_get_mgrcommand(const string &cmd_prefix,
                                           const std::vector<MonCommand> &commands);
  bool _allowed_command(
    MgrSession *s, const string &module, const string &prefix,
    const map<string,cmd_vartype>& cmdmap,
    const map<string,string>& param_str_map,
    const MonCommand *this_cmd);

private:
  friend class ReplyOnFinish;
  bool _reply(MCommand* m,
	      int ret, const std::string& s, const bufferlist& payload);

  void _prune_pending_service_map();

  utime_t started_at;
  std::atomic<bool> pgmap_ready;
  std::set<int32_t> reported_osds;
  void maybe_ready(int32_t osd_id);

public:
  int init(uint64_t gid, entity_addr_t client_addr);
  void shutdown();

  entity_addr_t get_myaddr() const;

  DaemonServer(MonClient *monc_,
               Finisher &finisher_,
	       DaemonStateIndex &daemon_state_,
	       ClusterState &cluster_state_,
	       PyModuleRegistry &py_modules_,
	       LogChannelRef cl,
	       LogChannelRef auditcl);
  ~DaemonServer() override;

  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new) override;
  bool ms_verify_authorizer(Connection *con,
      int peer_type,
      int protocol,
      ceph::bufferlist& authorizer,
      ceph::bufferlist& authorizer_reply,
      bool& isvalid,
      CryptoKey& session_key) override;

  bool handle_open(MMgrOpen *m);
  bool handle_report(MMgrReport *m);
  bool handle_command(MCommand *m);
  void send_report();
  void got_service_map();

  void _send_configure(ConnectionRef c);

  virtual const char** get_tracked_conf_keys() const override;
  virtual void handle_conf_change(const struct md_config_t *conf,
                          const std::set <std::string> &changed) override;
};

#endif

