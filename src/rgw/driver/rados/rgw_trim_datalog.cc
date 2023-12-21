// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <vector>
#include <string>

#include "common/errno.h"

#include "rgw_trim_datalog.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_datalog.h"
#include "rgw_data_sync.h"
#include "rgw_zone.h"
#include "rgw_bucket.h"

#include "services/svc_zone.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "data trim: ")

namespace {

class DatalogTrimImplCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
  int shard;
  std::string marker;
  std::string* last_trim_marker;

 public:
  DatalogTrimImplCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, int shard,
		    const std::string& marker, std::string* last_trim_marker)
  : RGWSimpleCoroutine(store->ctx()), dpp(dpp), store(store), shard(shard),
    marker(marker), last_trim_marker(last_trim_marker) {
    set_description() << "Datalog trim shard=" << shard
		      << " marker=" << marker;
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    set_status() << "sending request";
    cn = stack->create_completion_notifier();
    return store->svc()->datalog_rados->trim_entries(dpp, shard, marker,
						     cn->completion());
  }
  int request_complete() override {
    int r = cn->completion()->get_return_value();
    ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << "(): trim of shard=" << shard
		  << " marker=" << marker << " returned r=" << r << dendl;

    set_status() << "request complete; ret=" << r;
    if (r != -ENODATA) {
      return r;
    }
    // nothing left to trim, update last_trim_marker
    if (*last_trim_marker < marker &&
	marker != store->svc()->datalog_rados->max_marker()) {
      *last_trim_marker = marker;
    }
    return 0;
  }
};

/// return the marker that it's safe to trim up to
const std::string& get_stable_marker(const rgw_data_sync_marker& m)
{
  return m.state == m.FullSync ? m.next_step_marker : m.marker;
}

/// populate the container starting with 'dest' with the minimum stable marker
/// of each shard for all of the peers in [first, last)
template <typename IterIn, typename IterOut>
void take_min_markers(IterIn first, IterIn last, IterOut dest)
{
  if (first == last) {
    return;
  }
  for (auto p = first; p != last; ++p) {
    auto m = dest;
    for (auto &shard : p->sync_markers) {
      const auto& stable = get_stable_marker(shard.second);
      if (*m > stable) {
        *m = stable;
      }
      ++m;
    }
  }
}

} // anonymous namespace

class DataLogTrimCR : public RGWCoroutine {
  using TrimCR = DatalogTrimImplCR;
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  RGWHTTPManager *http;
  const int num_shards;
  const std::string& zone_id; //< my zone id
  std::vector<rgw_data_sync_status> peer_status; //< sync status for each peer
  std::vector<std::string> min_shard_markers; //< min marker per shard
  std::vector<std::string>& last_trim; //< last trimmed marker per shard
  int ret{0};

 public:
  DataLogTrimCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http,
                   int num_shards, std::vector<std::string>& last_trim)
    : RGWCoroutine(store->ctx()), dpp(dpp), store(store), http(http),
      num_shards(num_shards),
      zone_id(store->svc()->zone->get_zone().id),
      peer_status(store->svc()->zone->get_zone_data_notify_to_map().size()),
      min_shard_markers(num_shards,
			std::string(store->svc()->datalog_rados->max_marker())),
      last_trim(last_trim)
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int DataLogTrimCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    ldpp_dout(dpp, 10) << "fetching sync status for zone " << zone_id << dendl;
    set_status("fetching sync status");
    yield {
      // query data sync status from each sync peer
      rgw_http_param_pair params[] = {
        { "type", "data" },
        { "status", nullptr },
        { "source-zone", zone_id.c_str() },
        { nullptr, nullptr }
      };

      auto p = peer_status.begin();
      for (auto& c : store->svc()->zone->get_zone_data_notify_to_map()) {
        ldpp_dout(dpp, 20) << "query sync status from " << c.first << dendl;
        using StatusCR = RGWReadRESTResourceCR<rgw_data_sync_status>;
        spawn(new StatusCR(cct, c.second, http, "/admin/log/", params, &*p),
              false);
        ++p;
      }
    }

    // must get a successful reply from all peers to consider trimming
    ret = 0;
    while (ret == 0 && num_spawned() > 0) {
      yield wait_for_child();
      collect_next(&ret);
    }
    drain_all();

    if (ret < 0) {
      ldpp_dout(dpp, 4) << "failed to fetch sync status from all peers" << dendl;
      return set_cr_error(ret);
    }

    ldpp_dout(dpp, 10) << "trimming log shards" << dendl;
    set_status("trimming log shards");
    yield {
      // determine the minimum marker for each shard
      take_min_markers(peer_status.begin(), peer_status.end(),
                       min_shard_markers.begin());

      for (int i = 0; i < num_shards; i++) {
        const auto& m = min_shard_markers[i];
        if (m <= last_trim[i]) {
          continue;
        }
        ldpp_dout(dpp, 10) << "trimming log shard " << i
            << " at marker=" << m
            << " last_trim=" << last_trim[i] << dendl;
        spawn(new TrimCR(dpp, store, i, m, &last_trim[i]),
              true);
      }
    }
    return set_cr_done();
  }
  return 0;
}

RGWCoroutine* create_admin_data_log_trim_cr(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store,
                                            RGWHTTPManager *http,
                                            int num_shards,
                                            std::vector<std::string>& markers)
{
  return new DataLogTrimCR(dpp, store, http, num_shards, markers);
}

class DataLogTrimPollCR : public RGWCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* store;
  RGWHTTPManager *http;
  const int num_shards;
  const utime_t interval; //< polling interval
  const std::string lock_oid; //< use first data log shard for lock
  const std::string lock_cookie;
  std::vector<std::string> last_trim; //< last trimmed marker per shard

 public:
  DataLogTrimPollCR(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, RGWHTTPManager *http,
                    int num_shards, utime_t interval)
    : RGWCoroutine(store->ctx()), dpp(dpp), store(store), http(http),
      num_shards(num_shards), interval(interval),
      lock_oid(store->svc()->datalog_rados->get_oid(0, 0)),
      lock_cookie(RGWSimpleRadosLockCR::gen_random_cookie(cct)),
      last_trim(num_shards)
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int DataLogTrimPollCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    for (;;) {
      set_status("sleeping");
      wait(interval);

      // request a 'data_trim' lock that covers the entire wait interval to
      // prevent other gateways from attempting to trim for the duration
      set_status("acquiring trim lock");

      yield call(new RGWSimpleRadosLockCR(store->svc()->async_processor, store,
                                          rgw_raw_obj(store->svc()->zone->get_zone_params().log_pool, lock_oid),
                                          "data_trim", lock_cookie,
                                          // interval is a small number and unlikely to overflow
                                          // coverity[store_truncates_time_t:SUPPRESS]
                                          interval.sec()));
      if (retcode < 0) {
        // if the lock is already held, go back to sleep and try again later
        ldpp_dout(dpp, 4) << "failed to lock " << lock_oid << ", trying again in "
            << interval.sec() << "s" << dendl;
        continue;
      }

      set_status("trimming");
      yield call(new DataLogTrimCR(dpp, store, http, num_shards, last_trim));

      // note that the lock is not released. this is intentional, as it avoids
      // duplicating this work in other gateways
    }
  }
  return 0;
}

RGWCoroutine* create_data_log_trim_cr(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store,
                                      RGWHTTPManager *http,
                                      int num_shards, utime_t interval)
{
  return new DataLogTrimPollCR(dpp, store, http, num_shards, interval);
}
