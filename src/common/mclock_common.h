// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#pragma once
#include "config.h"
#include "ceph_context.h"
#include "dmclock/src/dmclock_server.h"
#ifndef WITH_CRIMSON
 #include "mon/MonClient.h"
#else
 #include "crimson/mon/MonClient.h"
#endif

// scheduler class for classic
enum class op_scheduler_class : uint8_t {
  background_recovery = 0,
  background_best_effort,
  immediate,
  client,
};

// scheduler class for Crimson
enum class scheduler_class_t : uint8_t {
  background_recovery = 0,
  background_best_effort,
  client,
  repop,
  immediate,
};

#ifdef WITH_CRIMSON
using SchedulerClass = scheduler_class_t;
using MonClient = crimson::mon::Client;
#else
using SchedulerClass = op_scheduler_class;
#endif

enum {
  l_mclock_first = 15000,
  l_mclock_immediate_queue_len,
  l_mclock_client_queue_len,
  l_mclock_recovery_queue_len,
  l_mclock_best_effort_queue_len,
  l_mclock_all_type_queue_len,
  l_mclock_last,
};

constexpr double default_min = 0.0;
constexpr double default_max = std::numeric_limits<double>::is_iec559 ?
  std::numeric_limits<double>::infinity() :
  std::numeric_limits<double>::max();

std::ostream& operator<<(std::ostream& out, const SchedulerClass& class_id);

/**
 * profile_t
 *
 * mclock profile -- 3 params for each of 3 client classes
 * 0 (min): specifies no minimum reservation
 * 0 (max): specifies no upper limit
 */

struct profile_t {
  struct client_config_t {
    double reservation;
    uint64_t weight;
    double limit;
  };
  client_config_t client;
  client_config_t background_recovery;
  client_config_t background_best_effort;

  constexpr profile_t(
    client_config_t client,
    client_config_t background_recovery,
    client_config_t background_best_effort
  ) : client(client), background_recovery(background_recovery),
      background_best_effort(background_best_effort) {}
};

/**
 * high_client_ops
 *
 * Client Allocation:
 *   reservation: 60% | weight: 2 | limit: 0 (max) |
 * Background Recovery Allocation:
 *   reservation: 40% | weight: 1 | limit: 0 (max) |
 * Background Best Effort Allocation:
 *   reservation: 0 (min) | weight: 1 | limit: 70% |
 */
constexpr profile_t HIGH_CLIENT_OPS{
  { .6, 2,  0 },
  { .4, 1,  0 },
  {  0, 1, .7 }
};

/**
 * high_recovery_ops
 *
 * Client Allocation:
 *   reservation: 30% | weight: 1 | limit: 0 (max) |
 * Background Recovery Allocation:
 *   reservation: 70% | weight: 2 | limit: 0 (max) |
 * Background Best Effort Allocation:
 *   reservation: 0 (min) | weight: 1 | limit: 0 (max) |
 */
constexpr profile_t HIGH_RECOVERY_OPS{
  { .3, 1, 0 },
  { .7, 2, 0 },
  {  0, 1, 0 }
};

/**
 * balanced
 *
 * Client Allocation:
 *   reservation: 50% | weight: 1 | limit: 0 (max) |
 * Background Recovery Allocation:
 *   reservation: 50% | weight: 1 | limit: 0 (max) |
 * Background Best Effort Allocation:
 *   reservation: 0 (min) | weight: 1 | limit: 90% |
 */
constexpr profile_t BALANCED{
  { .5, 1, 0 },
  { .5, 1, 0 },
  {  0, 1, .9 }
};

struct client_profile_id_t {
  uint64_t client_id = 0;
  uint64_t profile_id = 0;

  client_profile_id_t(uint64_t _client_id, uint64_t _profile_id) :
    client_id(_client_id),
    profile_id(_profile_id) {}

  client_profile_id_t() = default;

  auto operator<=>(const client_profile_id_t&) const = default;
  friend std::ostream& operator<<(std::ostream& out,
                                  const client_profile_id_t& client_profile);
};

struct scheduler_id_t {
  SchedulerClass class_id;
  client_profile_id_t client_profile_id;

  auto operator<=>(const scheduler_id_t&) const = default;
  friend std::ostream& operator<<(std::ostream& out,
                                  const scheduler_id_t& sched_id);
};


class ClientRegistry {
    static constexpr size_t internal_client_count =
      static_cast<size_t>(SchedulerClass::background_best_effort) + 1;
    std::vector<crimson::dmclock::ClientInfo> internal_client_infos;

    crimson::dmclock::ClientInfo default_external_client_info = {1, 1, 1};
    std::map<client_profile_id_t,
             crimson::dmclock::ClientInfo> external_client_infos;
    const crimson::dmclock::ClientInfo *get_external_client(
      const client_profile_id_t &client) const;
  public:
    ClientRegistry() {
      internal_client_infos.reserve(internal_client_count);
      // Fill array with default ClientInfo instances
      for (size_t i = 0; i < internal_client_count; ++i) {
        internal_client_infos.emplace_back(1, 1, 1);
      }
    }
    void update_from_profile(
      const profile_t &current_profile,
      const double capacity_per_shard);

    const crimson::dmclock::ClientInfo *get_info(
      const scheduler_id_t &id) const;
};

class MclockConfig final : public md_config_obs_t {
private:
  CephContext *cct;
  uint32_t num_shards;
  bool is_rotational;
  PerfCounters *logger = nullptr;
  int shard_id;
  int whoami;
  double osd_bandwidth_cost_per_io = 0.0;
  double osd_bandwidth_capacity_per_shard = 0.0;
  ClientRegistry& client_registry;

  // currently active profile, will be overridden from config on startup
  // and upon config change
  profile_t current_profile = BALANCED;
public:
  MclockConfig(CephContext *cct, ClientRegistry& creg,
               uint32_t num_shards, bool is_rotational, int shard_id,
	       int whoami):cct(cct),
                           num_shards(num_shards),
                           is_rotational(is_rotational),
			   shard_id(shard_id),
                           whoami(whoami),
	                   client_registry(creg)
  {
    cct->_conf.add_observer(this);
    set_from_config();
  }
  ~MclockConfig() final;

  void set_from_config();
  void init_logger();
  void get_mclock_counter(scheduler_id_t id);
  void put_mclock_counter(scheduler_id_t id);
  double get_cost_per_io() const;
  double get_capacity_per_shard() const;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set<std::string> &changed) final;
  std::vector<std::string> get_tracked_keys() const noexcept final {
    using namespace std::literals;
    return {
      "osd_mclock_scheduler_client_res"s,
      "osd_mclock_scheduler_client_wgt"s,
      "osd_mclock_scheduler_client_lim"s,
      "osd_mclock_scheduler_background_recovery_res"s,
      "osd_mclock_scheduler_background_recovery_wgt"s,
      "osd_mclock_scheduler_background_recovery_lim"s,
      "osd_mclock_scheduler_background_best_effort_res"s,
      "osd_mclock_scheduler_background_best_effort_wgt"s,
      "osd_mclock_scheduler_background_best_effort_lim"s,
      "osd_mclock_max_capacity_iops_hdd"s,
      "osd_mclock_max_capacity_iops_ssd"s,
      "osd_mclock_max_sequential_bandwidth_hdd"s,
      "osd_mclock_max_sequential_bandwidth_ssd"s,
      "osd_mclock_profile"s
    };
  }
  uint32_t calc_scaled_cost(int item_cost);
};
