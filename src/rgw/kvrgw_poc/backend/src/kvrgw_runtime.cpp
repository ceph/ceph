// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "kvrgw_runtime.hpp"

#include "fdb.hpp"
#include "gc_policy.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

namespace {

int env_int(const char *name, int default_value)
{
  const char *value = std::getenv(name);
  if (value == nullptr || value[0] == '\0') {
    return default_value;
  }
  return std::atoi(value);
}

void run_network()
{
  if (fdb_error_t err = fdb_run_network()) {
    std::cerr << "fdb_run_network failed: " << fdb_get_error(err) << std::endl;
    std::abort();
  }
}

} // namespace

namespace kvrgw {

KvRgwRuntime::~KvRgwRuntime() { stop(); }

void KvRgwRuntime::request_stop() { stop_.store(true); }

bool KvRgwRuntime::stop_requested() const { return stop_.load(); }

KvRgwServiceImpl &KvRgwRuntime::service() { return *service_; }

KvStore &KvRgwRuntime::store() { return *store_; }

PerfDataStore *KvRgwRuntime::perf_data_store()
{
  if (!perf_data_store_) {
    return nullptr;
  }
  return &*perf_data_store_;
}

void KvRgwRuntime::join_workers()
{
  if (sweeper_thread_.joinable()) {
    sweeper_thread_.join();
  }
  if (gc_thread_.joinable()) {
    gc_thread_.join();
  }
  if (admin_thread_.joinable()) {
    admin_thread_.join();
  }
}

void KvRgwRuntime::stop_fdb_network()
{
  if (!network_started_) {
    return;
  }
  if (fdb_error_t e = fdb_stop_network()) {
    std::cerr << "fdb_stop_network failed: " << fdb_get_error(e) << std::endl;
  }
  if (network_thread_.joinable()) {
    network_thread_.join();
  }
  network_started_ = false;
}

void KvRgwRuntime::stop()
{
  stop_.store(true);
  join_workers();
  admin_server_.reset();
  gc_worker_.reset();
  gc_config_.reset();
  service_.reset();
  sweeper_.reset();
  tier_config_state_.reset();
  file_data_store_.reset();
  perf_data_store_.reset();
  ref_tags_.reset();
  store_.reset();
  stop_fdb_network();
  started_ = false;
}

bool KvRgwRuntime::start(const KvRgwStartOptions &opts)
{
  if (started_) {
    return false;
  }

  perf_mode_ = opts.perf_mode;
  stop_.store(false);

  std::string data_root = opts.data_root;
  if (data_root.empty()) {
    data_root = "../data";
  }
  if (const char *env_data = std::getenv("KVRGW_DATA")) {
    data_root = env_data;
  }

  const int sweeper_interval = env_int("KVRGW_SWEEPER_INTERVAL_SEC", 10);
  const int sweeper_min_age = env_int("KVRGW_SWEEPER_MIN_AGE_SEC", 60);
  const int gc_interval = env_int("KVRGW_GC_INTERVAL_SEC", 10);
  const int gc_max_objects = env_int("KVRGW_GC_MAX_OBJECTS_PER_SEC", 0);
  const int gc_max_mb = env_int("KVRGW_GC_MAX_MB_PER_SEC", 0);

  // load tier config (yaml + env)
  std::string config_file = "kvrgw.yaml";
  if (const char *env_cfg = std::getenv("KVRGW_CONFIG_FILE")) {
    config_file = env_cfg;
  }
  auto tier_config = TierConfigState::load_from_yaml(config_file);
  tier_config = TierConfigState::apply_env_overrides(tier_config);

  std::string admin_socket = "/tmp/kvrgw-admin.sock";
  if (const char *env_admin = std::getenv("KVRGW_ADMIN_SOCKET")) {
    admin_socket = env_admin;
  }

  fdb_error_t err = fdb_select_api_version(FDB_API_VERSION);
  if (err) {
    std::cerr << "fdb_select_api_version failed: " << fdb_get_error(err)
              << std::endl;
    return false;
  }

#define BLUE "\033[34m" // Foreground Blue

  // Set the client-side knob option BEFORE setting up the network.
  // We are increasing the limit from 128KB to 4MB (4194304 bytes).
  // Note: FDB_NET_OPTION_KNOB is option 0.
  const char *knob_setting = "reply_byte_limit=4194304";
  err =
      fdb_network_set_option(FDB_NET_OPTION_KNOB, (const uint8_t *)knob_setting,
                             (int)strlen(knob_setting));
  std::string_view knob_view(knob_setting, (int)strlen(knob_setting));
  if (!err) {
    std::cerr << "\033[34m" << "Set FDB_NET_OPTION_KNOB: " << knob_view
              << "\033[0m" << std::endl;
  }
  else {
    std::cerr << "Error setting client knob: " << fdb_get_error(err)
              << std::endl;
    return false;
  }

  if (fdb_error_t net_err = fdb_setup_network()) {
    std::cerr << "fdb_setup_network failed: " << fdb_get_error(net_err)
              << std::endl;
    return false;
  }
  network_thread_ = std::thread(run_network);
  network_started_ = true;

  auto store_result = KvStore::create();
  if (!store_result) {
    std::cerr << "fdb_create_database failed: "
              << fdb_get_error(store_result.error()) << std::endl;
    stop();
    return false;
  }
  store_.emplace(std::move(*store_result));

  auto rgw_id = store_->allocate_rgw_id();
  if (!rgw_id) {
    std::cerr << "allocate_rgw_id failed: " << fdb_get_error(rgw_id.error())
              << std::endl;
    stop();
    return false;
  }
  std::cout << "kv-rgw backend rgw_id=" << *rgw_id << std::endl;

  ref_tags_.emplace(*rgw_id);
  perf_data_store_.emplace();
  file_data_store_.emplace(data_root);
  DataStore &data_store = perf_mode_
                              ? static_cast<DataStore &>(*perf_data_store_)
                              : static_cast<DataStore &>(*file_data_store_);

  tier_config_state_.emplace(tier_config);
  sweeper_.emplace(*store_, stop_, sweeper_interval, sweeper_min_age);
  service_.emplace(*store_, data_store, *ref_tags_, *tier_config_state_,
                   *sweeper_);

  GcPolicy initial_gc;
  initial_gc.interval_sec = gc_interval;
  initial_gc.max_objects_per_sec = gc_max_objects;
  initial_gc.max_mb_per_sec = gc_max_mb;
  gc_config_.emplace(initial_gc);
  gc_worker_.emplace(*store_, data_store, *gc_config_, stop_);
  admin_server_.emplace(*gc_config_, *tier_config_state_, admin_socket, stop_,
                        &service_->latency_stats(), &service_->error_stats(),
                        &service_->batch_queue().stats(),
                        &service_->err_insertion(), &service_->ops_stats());

  admin_thread_ = std::thread([this]() {
    if (!admin_server_->run()) {
      std::cerr << "admin server startup failed" << std::endl;
    }
  });

  if (!perf_mode_) {
    sweeper_thread_ = std::thread([this]() { sweeper_->run(); });
    gc_thread_ = std::thread([this]() { gc_worker_->run(); });
  }

  started_ = true;
  return true;
}

} // namespace kvrgw
