// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <string>

#include "common/ceph_time.h"
#include "include/types.h"

// mon subscriptions
class MonSub
{
public:
  // @returns true if there is any "new" subscriptions
  bool have_new() const;
  auto get_subs() const {
    return sub_new;
  }
  bool need_renew() const;
  // change the status of "new" subscriptions to "sent"
  void renewed();
  // the peer acked the subscription request
  void acked(uint32_t interval);
  void got(const std::string& what, version_t version);
  // revert the status of subscriptions from "sent" to "new"
  // @returns true if there is any pending "new" subscriptions
  bool reload();
  // add a new subscription
  bool want(const std::string& what, version_t start, unsigned flags);
  // increment the requested subscription start point. If you do increase
  // the value, apply the passed-in flags as well; otherwise do nothing.
  bool inc_want(const std::string& what, version_t start, unsigned flags);
  // cancel a subscription
  void unwant(const std::string& what);
private:
  // my subs, and current versions
  std::map<std::string,ceph_mon_subscribe_item> sub_sent;
  // unsent new subs
  std::map<std::string,ceph_mon_subscribe_item> sub_new;
  using time_point = ceph::coarse_mono_time;
  using clock = typename time_point::clock;
  time_point renew_sent;
  time_point renew_after;
};
