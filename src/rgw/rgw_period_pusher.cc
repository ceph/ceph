// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <map>
#include <thread>

#include "rgw_period_pusher.h"
#include "rgw_cr_rest.h"
#include "common/errno.h"

#include "rgw_boost_asio_yield.h"

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

  int operate() override;
};

int PushAndRetryCR::operate()
{
  reenter(this) {
    for (;;) {
      yield {
        ldout(cct, 10) << "pushing period " << period.get_id()
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
        ldout(cct, 10) << "push to " << zone << " succeeded" << dendl;
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

        ldout(cct, 10) << "waiting " << dur << "s for retry.." << dendl;
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

  int operate() override;
};

int PushAllCR::operate()
{
  reenter(this) {
    // spawn a coroutine to push the period over each connection
    yield {
      ldout(cct, 4) << "sending " << conns.size() << " periods" << dendl;
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
class RGWPeriodPusher::CRThread {
  RGWCoroutinesManager coroutines;
  RGWHTTPManager http;
  boost::intrusive_ptr<PushAllCR> push_all;
  std::thread thread;

 public:
  CRThread(CephContext* cct, RGWPeriod&& period,
           std::map<std::string, RGWRESTConn>&& conns)
    : coroutines(cct, NULL),
      http(cct, coroutines.get_completion_mgr()),
      push_all(new PushAllCR(cct, &http, std::move(period), std::move(conns))),
      thread([this] { coroutines.run(push_all.get()); })
  {
    http.set_threaded();
  }
  ~CRThread()
  {
    push_all.reset();
    coroutines.stop();
    http.stop();
    if (thread.joinable())
      thread.join();
  }
};


RGWPeriodPusher::RGWPeriodPusher(RGWRados* store)
  : cct(store->ctx()), store(store)
{
  const auto& realm = store->realm;
  auto& realm_id = realm.get_id();
  if (realm_id.empty()) // no realm configuration
    return;

  // always send out the current period on startup
  RGWPeriod period;
  int r = period.init(cct, store, realm_id, realm.get_name());
  if (r < 0) {
    lderr(cct) << "failed to load period for realm " << realm_id << dendl;
    return;
  }

  std::lock_guard<std::mutex> lock(mutex);
  handle_notify(std::move(period));
}

// destructor is here because CRThread is incomplete in the header
RGWPeriodPusher::~RGWPeriodPusher() = default;

void RGWPeriodPusher::handle_notify(RGWRealmNotify type,
                                    bufferlist::iterator& p)
{
  // decode the period
  RGWZonesNeedPeriod info;
  try {
    ::decode(info, p);
  } catch (buffer::error& e) {
    lderr(cct) << "Failed to decode the period: " << e.what() << dendl;
    return;
  }

  std::lock_guard<std::mutex> lock(mutex);

  // we can't process this notification without access to our current realm
  // configuration. queue it until resume()
  if (store == nullptr) {
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
  auto i = zonegroups.find(store->get_zonegroup().get_id());
  if (i == zonegroups.end()) {
    lderr(cct) << "The new period does not contain my zonegroup!" << dendl;
    return;
  }
  auto& my_zonegroup = i->second;

  // if we're not a master zone, we're not responsible for pushing any updates
  if (my_zonegroup.master_zone != store->get_zone_params().get_id())
    return;

  // construct a map of the zones that need this period. the map uses the same
  // keys/ordering as the zone[group] map, so we can use a hint for insertions
  std::map<std::string, RGWRESTConn> conns;
  auto hint = conns.end();

  // are we the master zonegroup in this period?
  if (period.get_map().master_zonegroup == store->get_zonegroup().get_id()) {
    // update other zonegroup endpoints
    for (auto& zg : zonegroups) {
      auto& zonegroup = zg.second;
      if (zonegroup.get_id() == store->get_zonegroup().get_id())
        continue;
      if (zonegroup.endpoints.empty())
        continue;

      hint = conns.emplace_hint(
          hint, std::piecewise_construct,
          std::forward_as_tuple(zonegroup.get_id()),
          std::forward_as_tuple(cct, store, zonegroup.get_id(), zonegroup.endpoints));
    }
  }

  // update other zone endpoints
  for (auto& z : my_zonegroup.zones) {
    auto& zone = z.second;
    if (zone.id == store->get_zone_params().get_id())
      continue;
    if (zone.endpoints.empty())
      continue;

    hint = conns.emplace_hint(
        hint, std::piecewise_construct,
        std::forward_as_tuple(zone.id),
        std::forward_as_tuple(cct, store, zone.id, zone.endpoints));
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
  store = nullptr;
}

void RGWPeriodPusher::resume(RGWRados* store)
{
  std::lock_guard<std::mutex> lock(mutex);
  this->store = store;

  ldout(cct, 4) << "resume with " << pending_periods.size()
      << " periods pending" << dendl;

  // process notification queue
  for (auto& info : pending_periods) {
    handle_notify(std::move(info));
  }
  pending_periods.clear();
}
