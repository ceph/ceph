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


#ifndef MGR_STANDBY_H_
#define MGR_STANDBY_H_

#include "auth/Auth.h"
#include "common/Finisher.h"
#include "common/Timer.h"
#include "common/LogClient.h"

#include "client/Client.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "PyModuleRegistry.h"
#include "MgrClient.h"

class MMgrMap;
class Mgr;
class PyModuleConfig;

class MgrStandby : public Dispatcher,
		   public md_config_obs_t {
public:
  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override;

protected:
  MonClient monc;
  std::unique_ptr<Messenger> client_messenger;
  Objecter objecter;
  Client client;

  MgrClient mgrc;

  LogClient log_client;
  LogChannelRef clog, audit_clog;

  Mutex lock;
  Finisher finisher;
  SafeTimer timer;

  PyModuleRegistry py_module_registry;
  std::shared_ptr<Mgr> active_mgr;

  int orig_argc;
  const char **orig_argv;

  std::string state_str();

  void handle_mgr_map(MMgrMap *m);
  void _update_log_config();
  void send_beacon();

  bool available_in_map;

public:
  MgrStandby(int argc, const char **argv);
  ~MgrStandby() override;

  bool ms_dispatch(Message *m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer) override;
  bool ms_handle_refused(Connection *con) override;

  int init();
  void shutdown();
  void respawn();
  int main(vector<const char *> args);
  void handle_signal(int signum);
  void tick();
};

#endif

