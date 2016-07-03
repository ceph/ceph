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

#include "PyModules.h"

#include <set>
#include <string>

#include "common/Mutex.h"

#include <msg/Messenger.h>
#include <mon/MonClient.h>

#include "auth/AuthAuthorizeHandler.h"

#include "DaemonState.h"

class MMgrReport;
class MMgrOpen;
class MCommand;


/**
 * Server used in ceph-mgr to communicate with Ceph daemons like
 * MDSs and OSDs.
 */
class DaemonServer : public Dispatcher
{
protected:
  Messenger *msgr;
  MonClient *monc;
  DaemonStateIndex &daemon_state;
  PyModules &py_modules;

  AuthAuthorizeHandlerRegistry auth_registry;

  Mutex lock;


public:
  int init(uint64_t gid, entity_addr_t client_addr);
  void shutdown();

  entity_addr_t get_myaddr() const;

  DaemonServer(MonClient *monc_,
      DaemonStateIndex &daemon_state_,
      PyModules &py_modules_);
  ~DaemonServer();

  bool ms_dispatch(Message *m);
  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer,
                         bool force_new);
  bool ms_verify_authorizer(Connection *con,
      int peer_type,
      int protocol,
      ceph::bufferlist& authorizer,
      ceph::bufferlist& authorizer_reply,
      bool& isvalid,
      CryptoKey& session_key);

  bool handle_open(MMgrOpen *m);
  bool handle_report(MMgrReport *m);
  bool handle_command(MCommand *m);
};

#endif

