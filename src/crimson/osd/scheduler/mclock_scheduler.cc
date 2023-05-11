// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>
#include <functional>

#include "crimson/osd/scheduler/mclock_scheduler.h"
#include "common/dout.h"

namespace dmc = crimson::dmclock;
using namespace std::placeholders;

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


namespace crimson::osd::scheduler {

mClockScheduler::mClockScheduler(ConfigProxy &conf) :
  scheduler(
    std::bind(&mClockScheduler::ClientRegistry::get_info,
	      &client_registry,
	      _1),
    dmc::AtLimit::Allow,
    conf.get_val<double>("osd_mclock_scheduler_anticipation_timeout"))
{
  conf.add_observer(this);
  client_registry.update_from_config(conf);
}

void mClockScheduler::ClientRegistry::update_from_config(const ConfigProxy &conf)
{
  default_external_client_info.update(
    conf.get_val<double>("osd_mclock_scheduler_client_res"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_client_wgt"),
    conf.get_val<double>("osd_mclock_scheduler_client_lim"));

  internal_client_infos[
    static_cast<size_t>(scheduler_class_t::background_recovery)].update(
    conf.get_val<double>("osd_mclock_scheduler_background_recovery_res"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_recovery_wgt"),
    conf.get_val<double>("osd_mclock_scheduler_background_recovery_lim"));

  internal_client_infos[
    static_cast<size_t>(scheduler_class_t::background_best_effort)].update(
    conf.get_val<double>("osd_mclock_scheduler_background_best_effort_res"),
    conf.get_val<uint64_t>("osd_mclock_scheduler_background_best_effort_wgt"),
    conf.get_val<double>("osd_mclock_scheduler_background_best_effort_lim"));
}

const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_external_client(
  const client_profile_id_t &client) const
{
  auto ret = external_client_infos.find(client);
  if (ret == external_client_infos.end())
    return &default_external_client_info;
  else
    return &(ret->second);
}

const dmc::ClientInfo *mClockScheduler::ClientRegistry::get_info(
  const scheduler_id_t &id) const {
  switch (id.class_id) {
  case scheduler_class_t::immediate:
    ceph_assert(0 == "Cannot schedule immediate");
    return (dmc::ClientInfo*)nullptr;
  case scheduler_class_t::repop:
  case scheduler_class_t::client:
    return get_external_client(id.client_profile_id);
  default:
    ceph_assert(static_cast<size_t>(id.class_id) < internal_client_infos.size());
    return &internal_client_infos[static_cast<size_t>(id.class_id)];
  }
}

void mClockScheduler::dump(ceph::Formatter &f) const
{
}

void mClockScheduler::enqueue(item_t&& item)
{
  auto id = get_scheduler_id(item);
  auto cost = item.params.cost;

  if (scheduler_class_t::immediate == item.params.klass) {
    immediate.push_front(std::move(item));
  } else {
    scheduler.add_request(
      std::move(item),
      id,
      cost);
  }
}

void mClockScheduler::enqueue_front(item_t&& item)
{
  immediate.push_back(std::move(item));
  // TODO: item may not be immediate, update mclock machinery to permit
  // putting the item back in the queue
}

item_t mClockScheduler::dequeue()
{
  if (!immediate.empty()) {
    auto ret = std::move(immediate.back());
    immediate.pop_back();
    return ret;
  } else {
    mclock_queue_t::PullReq result = scheduler.pull_request();
    if (result.is_future()) {
      ceph_assert(
	0 == "Not implemented, user would have to be able to be woken up");
      return std::move(*(item_t*)nullptr);
    } else if (result.is_none()) {
      ceph_assert(
	0 == "Impossible, must have checked empty() first");
      return std::move(*(item_t*)nullptr);
    } else {
      ceph_assert(result.is_retn());

      auto &retn = result.get_retn();
      return std::move(*retn.request);
    }
  }
}

const char** mClockScheduler::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_mclock_scheduler_client_res",
    "osd_mclock_scheduler_client_wgt",
    "osd_mclock_scheduler_client_lim",
    "osd_mclock_scheduler_background_recovery_res",
    "osd_mclock_scheduler_background_recovery_wgt",
    "osd_mclock_scheduler_background_recovery_lim",
    "osd_mclock_scheduler_background_best_effort_res",
    "osd_mclock_scheduler_background_best_effort_wgt",
    "osd_mclock_scheduler_background_best_effort_lim",
    NULL
  };
  return KEYS;
}

void mClockScheduler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  client_registry.update_from_config(conf);
}

}
