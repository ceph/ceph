// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <map>
#include <thread>

#include "rgw_period_pusher.h"
#include "rgw_cr_rest.h"
#include "rgw_zone.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"

#include "common/errno.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "rgw period pusher: ")

/// A coroutine to post the period over the given connection.
using PushCR = RGWPostRESTResourceCR<RGWPeriod, int>;

/// A coroutine that calls PushCR, and retries with backoff until success.
class PushAndRetryCR : public RGWCoroutine {
  const std::string& zone;
  RGWRESTConn *const conn;
  RGWHTTPManager *const http;
  RGWPeriod& period;
  const std::string epoch; //< epoch string for params
  double timeout; //< current interval between retries
  const double timeout_max; //< maximum interval between retries
  uint32_t counter; //< number of failures since backoff increased

 public:
  PushAndRetryCR(CephContext* cct, const std::string& zone, RGWRESTConn* conn,
                 RGWHTTPManager* http, RGWPeriod& period)
    : RGWCoroutine(cct), zone(zone), conn(conn), http(http), period(period),
      epoch(std::to_string(period.get_epoch())),
      timeout(cct->_conf->rgw_period_push_interval),
      timeout_max(cct->_conf->rgw_period_push_interval_max),
      counter(0)
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int PushAndRetryCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    for (;;) {
      yield {
        ldpp_dout(dpp, 10) << "pushing period " << period.get_id()
            << " to " << zone << dendl;
        // initialize the http params
        rgw_http_param_pair params[] = {
          { "period", period.get_id().c_str() },
          { "epoch", epoch.c_str() },
          { nullptr, nullptr }
        };
        call(new PushCR(cct, conn, http, "/admin/realm/period",
                        params, period, nullptr));
      }

      // stop on success
      if (get_ret_status() == 0) {
        ldpp_dout(dpp, 10) << "push to " << zone << " succeeded" << dendl;
        return set_cr_done();
      }

      // try each endpoint in the connection before waiting
      if (++counter < conn->get_endpoint_count())
        continue;
      counter = 0;

      // wait with exponential backoff up to timeout_max
      yield {
        utime_t dur;
        dur.set_from_double(timeout);

        ldpp_dout(dpp, 10) << "waiting " << dur << "s for retry.." << dendl;
        wait(dur);

        timeout *= 2;
        if (timeout > timeout_max)
          timeout = timeout_max;
      }
    }
  }
  return 0;
}

/**
 * PushAllCR is a coroutine that sends the period over all of the given
 * connections, retrying until they are all marked as completed.
 */
class PushAllCR : public RGWCoroutine {
  RGWHTTPManager *const http;
  RGWPeriod period; //< period object to push
  std::map<std::string, RGWRESTConn> conns; //< zones that need the period

 public:
  PushAllCR(CephContext* cct, RGWHTTPManager* http, RGWPeriod&& period,
            std::map<std::string, RGWRESTConn>&& conns)
    : RGWCoroutine(cct), http(http),
      period(std::move(period)),
      conns(std::move(conns))
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int PushAllCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    // spawn a coroutine to push the period over each connection
    yield {
      ldpp_dout(dpp, 4) << "sending " << conns.size() << " periods" << dendl;
      for (auto& c : conns)
        spawn(new PushAndRetryCR(cct, c.first, &c.second, http, period), false);
    }
    // wait for all to complete
    drain_all();
    return set_cr_done();
  }
  return 0;
}

/// A background thread to run the PushAllCR coroutine and exit.
class RGWPeriodPusher::CRThread : public DoutPrefixProvider {
  CephContext* cct;
  RGWCoroutinesManager coroutines;
  RGWHTTPManager http;
  boost::intrusive_ptr<PushAllCR> push_all;
  std::thread thread;

 public:
  CRThread(CephContext* cct, RGWPeriod&& period,
           std::map<std::string, RGWRESTConn>&& conns)
    : cct(cct), coroutines(cct, NULL),
      http(cct, coroutines.get_completion_mgr()),
      push_all(new PushAllCR(cct, &http, std::move(period), std::move(conns)))
  {
    http.start();
    // must spawn the CR thread after start
    thread = std::thread([this]() noexcept { coroutines.run(this, push_all.get()); });
  }
  ~CRThread()
  {
    push_all.reset();
    coroutines.stop();
    http.stop();
    if (thread.joinable())
      thread.join();
  }

  CephContext *get_cct() const override { return cct; }
  unsigned get_subsys() const override { return dout_subsys; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "rgw period pusher CR thread: "; }
};


RGWPeriodPusher::RGWPeriodPusher(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
				 optional_yield y)
  : cct(driver->ctx()), driver(driver)
{
  rgw::sal::Zone* zone = driver->get_zone();
  auto& realm_id = zone->get_realm_id();
  if (realm_id.empty()) // no realm configuration
    return;

  // always send out the current period on startup
  RGWPeriod period;
  // XXX dang
  int r = period.init(dpp, cct, static_cast<rgw::sal::RadosStore* >(driver)->svc()->sysobj, realm_id, y);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to load period for realm " << realm_id << dendl;
    return;
  }

  std::lock_guard<std::mutex> lock(mutex);
  handle_notify(std::move(period));
}

// destructor is here because CRThread is incomplete in the header
RGWPeriodPusher::~RGWPeriodPusher() = default;

void RGWPeriodPusher::handle_notify(RGWRealmNotify type,
                                    bufferlist::const_iterator& p)
{
  // decode the period
  RGWZonesNeedPeriod info;
  try {
    decode(info, p);
  } catch (buffer::error& e) {
    lderr(cct) << "Failed to decode the period: " << e.what() << dendl;
    return;
  }

  std::lock_guard<std::mutex> lock(mutex);

  // we can't process this notification without access to our current realm
  // configuration. queue it until resume()
  if (driver == nullptr) {
    pending_periods.emplace_back(std::move(info));
    return;
  }

  handle_notify(std::move(info));
}

// expects the caller to hold a lock on mutex
void RGWPeriodPusher::handle_notify(RGWZonesNeedPeriod&& period)
{
  if (period.get_realm_epoch() < realm_epoch) {
    ldout(cct, 10) << "period's realm epoch " << period.get_realm_epoch()
        << " is not newer than current realm epoch " << realm_epoch
        << ", discarding update" << dendl;
    return;
  }
  if (period.get_realm_epoch() == realm_epoch &&
      period.get_epoch() <= period_epoch) {
    ldout(cct, 10) << "period epoch " << period.get_epoch() << " is not newer "
        "than current epoch " << period_epoch << ", discarding update" << dendl;
    return;
  }

  // find our zonegroup in the new period
  auto& zonegroups = period.get_map().zonegroups;
  auto i = zonegroups.find(driver->get_zone()->get_zonegroup().get_id());
  if (i == zonegroups.end()) {
    lderr(cct) << "The new period does not contain my zonegroup!" << dendl;
    return;
  }
  auto& my_zonegroup = i->second;

  // if we're not a master zone, we're not responsible for pushing any updates
  if (my_zonegroup.master_zone != driver->get_zone()->get_id())
    return;

  // construct a map of the zones that need this period. the map uses the same
  // keys/ordering as the zone[group] map, so we can use a hint for insertions
  std::map<std::string, RGWRESTConn> conns;
  auto hint = conns.end();

  // are we the master zonegroup in this period?
  if (period.get_map().master_zonegroup == driver->get_zone()->get_zonegroup().get_id()) {
    // update other zonegroup endpoints
    for (auto& zg : zonegroups) {
      auto& zonegroup = zg.second;
      if (zonegroup.get_id() == driver->get_zone()->get_zonegroup().get_id())
        continue;
      if (zonegroup.endpoints.empty())
        continue;

      hint = conns.emplace_hint(
          hint, std::piecewise_construct,
          std::forward_as_tuple(zonegroup.get_id()),
          std::forward_as_tuple(cct, driver, zonegroup.get_id(), zonegroup.endpoints, zonegroup.api_name));
    }
  }

  // update other zone endpoints
  for (auto& z : my_zonegroup.zones) {
    auto& zone = z.second;
    if (zone.id == driver->get_zone()->get_id())
      continue;
    if (zone.endpoints.empty())
      continue;

    hint = conns.emplace_hint(
        hint, std::piecewise_construct,
        std::forward_as_tuple(zone.id),
        std::forward_as_tuple(cct, driver, zone.id, zone.endpoints, my_zonegroup.api_name));
  }

  if (conns.empty()) {
    ldout(cct, 4) << "No zones to update" << dendl;
    return;
  }

  realm_epoch = period.get_realm_epoch();
  period_epoch = period.get_epoch();

  ldout(cct, 4) << "Zone master pushing period " << period.get_id()
      << " epoch " << period_epoch << " to "
      << conns.size() << " other zones" << dendl;

  // spawn a new coroutine thread, destroying the previous one
  cr_thread.reset(new CRThread(cct, std::move(period), std::move(conns)));
}

void RGWPeriodPusher::pause()
{
  ldout(cct, 4) << "paused for realm update" << dendl;
  std::lock_guard<std::mutex> lock(mutex);
  driver = nullptr;
}

void RGWPeriodPusher::resume(rgw::sal::Driver* driver)
{
  std::lock_guard<std::mutex> lock(mutex);
  this->driver = driver;

  ldout(cct, 4) << "resume with " << pending_periods.size()
      << " periods pending" << dendl;

  // process notification queue
  for (auto& info : pending_periods) {
    handle_notify(std::move(info));
  }
  pending_periods.clear();
}
