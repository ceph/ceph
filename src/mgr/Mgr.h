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
#include "Python.h"
// Python's pyconfig-64.h conflicts with ceph's acconfig.h
#undef HAVE_SYS_WAIT_H
#undef HAVE_UNISTD_H
#undef HAVE_UTIME_H
#undef _POSIX_C_SOURCE
#undef _XOPEN_SOURCE

#include "osdc/Objecter.h"
#include "mds/FSMap.h"
#include "messages/MFSMap.h"
#include "msg/Messenger.h"
#include "auth/Auth.h"
#include "common/Finisher.h"
#include "common/Timer.h"

#include "DaemonServer.h"
#include "PyModules.h"

#include "DaemonState.h"
#include "ClusterState.h"

class MCommand;
class MMgrDigest;


class MgrPyModule;

class Mgr {
protected:
  MonClient *monc;
  Objecter  *objecter;
  Messenger *client_messenger;

  Mutex lock;
  SafeTimer timer;
  Finisher finisher;

  Context *waiting_for_fs_map;

  PyModules py_modules;
  DaemonStateIndex daemon_state;
  ClusterState cluster_state;

  DaemonServer server;

  void load_config();
  void load_all_metadata();
  void init();

  bool initialized;
  bool initializing;

public:
  Mgr(MonClient *monc_, Messenger *clientm_);
  ~Mgr();

  bool is_initialized() const {return initialized;}
  entity_addr_t get_server_addr() const { return server.get_myaddr(); }

  void handle_mgr_digest(MMgrDigest* m);
  void handle_fs_map(MFSMap* m);
  void handle_osd_map();

  bool ms_dispatch(Message *m);

  void background_init();
  void shutdown();
  int main(vector<const char *> args);
};

#endif
