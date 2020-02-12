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
#include "include/types.h"

#include "client/Client.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "PyModuleRegistry.h"
#include "MgrClient.h"

class MMgrMap;
class Mgr;
class PyModuleConfig;
class MMgrBeaconReply;

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

  ceph::mutex lock = ceph::make_mutex("MgrStandby::lock");
  ceph::condition_variable cond;
  Finisher finisher;
  SafeTimer timer;

  PyModuleRegistry py_module_registry;
  std::shared_ptr<Mgr> active_mgr;

  int orig_argc;
  const char **orig_argv;

  std::string state_str();

  void handle_mgr_map(ceph::ref_t<MMgrMap> m);
  void _update_log_config();
  void send_beacon();

  bool available_in_map;

public:
  MgrStandby(int argc, const char **argv);
  ~MgrStandby() override;

  bool ms_dispatch2(const ceph::ref_t<Message>& m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override;

  int init();
  void shutdown();
  void respawn();
  int main(vector<const char *> args);
  void handle_signal(int signum);
  void tick();

private:
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  // sequence number of beacon sent to monitor
  version_t last_seq = 0;

  // this could just have been a list of outstanding (unacknowleged)
  // sequence numbers since there is no consideration of a laggy mgr
  // which is derieved from ack from the monitor. however, maintaining
  // sequence timestamps *might* just help later in this respect.
  std::map<version_t, time> seq_stamp;

  bool handle_beacon_reply(const ref_t<MMgrBeaconReply>& m);
  void send_beacon_and_wait(std::unique_lock<ceph::mutex> &locker);
  void wait_for_beacon_ack(version_t seq_ack, std::unique_lock<ceph::mutex> &locker);
};

#endif

