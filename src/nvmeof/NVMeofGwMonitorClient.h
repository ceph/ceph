// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023,2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef NVMEOFGWMONITORCLIENT_H_
#define NVMEOFGWMONITORCLIENT_H_

#include "auth/Auth.h"
#include "common/async/context_pool.h"
#include "common/Finisher.h"
#include "common/Timer.h"
#include "common/LogClient.h"

#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "messages/MNVMeofGwMap.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>

class NVMeofGwMonitorClient: public Dispatcher,
		   public md_config_obs_t {
private:
  std::string name;
  std::string pool;
  std::string group;
  std::string gateway_address;
  std::string monitor_address;
  std::string server_cert;
  std::string client_key;
  std::string client_cert;
  grpc::SslCredentialsOptions
              gw_ssl_opts;  // gateway grpc ssl options
  epoch_t     osdmap_epoch; // last awaited osdmap_epoch
  epoch_t     gwmap_epoch;  // last received gw map epoch
  std::chrono::time_point<std::chrono::steady_clock>
              last_map_time; // used to panic on disconnect

  // init gw ssl opts
  void init_gw_ssl_opts();

  // returns gateway grpc credentials
  std::shared_ptr<grpc::ChannelCredentials> gw_creds();

protected:
  ceph::async::io_context_pool poolctx;
  MonClient monc;
  std::unique_ptr<Messenger> client_messenger;
  Objecter objecter;
  std::map<NvmeGroupKey, NvmeGwMonClientStates> map;
  ceph::mutex lock = ceph::make_mutex("NVMeofGw::lock");
  // allow beacons to be sent independently of handle_nvmeof_gw_map
  ceph::mutex beacon_lock = ceph::make_mutex("NVMeofGw::beacon_lock");
  SafeTimer timer;

  int orig_argc;
  const char **orig_argv;

  void send_config_beacon(); 
  void send_beacon();
 
public:
  NVMeofGwMonitorClient(int argc, const char **argv);
  ~NVMeofGwMonitorClient() override;

  // Dispatcher interface
  bool ms_dispatch2(const ceph::ref_t<Message>& m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; };

  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override {};

  int init();
  void shutdown();
  int main(std::vector<const char *> args);
  void tick();
  void disconnect_panic();

  void handle_nvmeof_gw_map(ceph::ref_t<MNVMeofGwMap> m);
};

#endif

