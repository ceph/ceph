// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_MGR_H_
#define CEPH_MGR_H_

// Python.h comes first because otherwise it clobbers ceph's assert
#include <Python.h>

#include "mds/FSMap.h"
#include "messages/MFSMap.h"
#include "msg/Messenger.h"
#include "auth/Auth.h"
#include "common/Finisher.h"
#include "mon/MgrMap.h"

#include "DaemonServer.h"
#include "PyModuleRegistry.h"

#include "DaemonState.h"
#include "ClusterState.h"

class MCommand;
class MMgrDigest;
class MLog;
class MServiceMap;
class Objecter;
class Client;

class Mgr : public AdminSocketHook {
protected:
  MonClient *monc;
  Objecter  *objecter;
  Client    *client;
  Messenger *client_messenger;

  mutable ceph::mutex lock = ceph::make_mutex("Mgr::lock");
  Finisher finisher;

  // Track receipt of initial data during startup
  ceph::condition_variable fs_map_cond;
  bool digest_received;
  ceph::condition_variable digest_cond;

  PyModuleRegistry *py_module_registry;
  DaemonStateIndex daemon_state;
  ClusterState cluster_state;

  DaemonServer server;

  LogChannelRef clog;
  LogChannelRef audit_clog;

  std::map<std::string, std::string> pre_init_store;

  void load_all_metadata();
  std::map<std::string, std::string> load_store();
  void init();

  bool initialized;
  bool initializing;

public:
  Mgr(MonClient *monc_, const MgrMap& mgrmap,
      PyModuleRegistry *py_module_registry_,
      Messenger *clientm_, Objecter *objecter_,
      Client *client_, LogChannelRef clog_, LogChannelRef audit_clog_);
  ~Mgr();

  bool is_initialized() const {return initialized;}
  entity_addrvec_t get_server_addrs() const {
    return server.get_myaddrs();
  }

  void handle_mgr_digest(ceph::ref_t<MMgrDigest> m);
  void handle_fs_map(ceph::ref_t<MFSMap> m);
  void handle_osd_map();
  void handle_log(ceph::ref_t<MLog> m);
  void handle_service_map(ceph::ref_t<MServiceMap> m);
  void handle_mon_map();

  bool got_mgr_map(const MgrMap& m);

  bool ms_dispatch2(const ceph::ref_t<Message>& m);

  void background_init(Context *completion);

  std::map<std::string, std::string> get_services() const;

  int call(
    std::string_view command,
    const cmdmap_t& cmdmap,
    const bufferlist& inbl,
    Formatter *f,
    std::ostream& errss,
    ceph::buffer::list& out) override;
};

/**
 * Context for completion of metadata mon commands: take
 * the result and stash it in DaemonStateIndex
 */
class MetadataUpdate : public Context
{

private:
  DaemonStateIndex &daemon_state;
  DaemonKey key;

  std::map<std::string, std::string> defaults;

public:
  bufferlist outbl;
  std::string outs;

  MetadataUpdate(DaemonStateIndex &daemon_state_, const DaemonKey &key_)
    : daemon_state(daemon_state_), key(key_)
  {
      daemon_state.notify_updating(key);
  }

  void set_default(const std::string &k, const std::string &v)
  {
    defaults[k] = v;
  }

  void finish(int r) override;
};


#endif
