// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "common/async/yield_context.h"
#include "rgw_realm_reloader.h"
#include "rgw_sal_fwd.h"

class RGWPeriod;

// RGWRealmNotify payload for push coordination
using RGWZonesNeedPeriod = RGWPeriod;

/**
 * RGWPeriodPusher coordinates with other nodes via the realm watcher to manage
 * the responsibility for pushing period updates to other zones or zonegroups.
 */
class RGWPeriodPusher final : public RGWRealmWatcher::Watcher,
                              public RGWRealmReloader::Pauser {
 public:
  explicit RGWPeriodPusher(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y);
  ~RGWPeriodPusher() override;

  /// respond to realm notifications by pushing new periods to other zones
  void handle_notify(RGWRealmNotify type, bufferlist::const_iterator& p) override;

  /// avoid accessing RGWRados while dynamic reconfiguration is in progress.
  /// notifications will be enqueued until resume()
  void pause() override;

  /// continue processing notifications with a new RGWRados instance
  void resume(rgw::sal::Driver* driver) override;

 private:
  void handle_notify(RGWZonesNeedPeriod&& period);

  CephContext *const cct;
  rgw::sal::Driver* driver;

  std::mutex mutex;
  epoch_t realm_epoch{0}; //< the current realm epoch being sent
  epoch_t period_epoch{0}; //< the current period epoch being sent

  /// while paused for reconfiguration, we need to queue up notifications
  std::vector<RGWZonesNeedPeriod> pending_periods;

  class CRThread; //< contains thread, coroutine manager, http manager
  std::unique_ptr<CRThread> cr_thread; //< thread to run the push coroutines
};
