// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_cloud_delete.h"

#include "common/dout.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/errno.h"
#include "rgw_perf_counters.h"
#include "rgw_sal.h"
#include "rgw_sal_filter.h"
#include "rgw_rest_conn.h"
#include "driver/rados/rgw_sal_rados.h"
#include "driver/rados/rgw_lc_tier.h"
#include "rgw_http_errors.h"
#include "include/random.h"
#include <fmt/core.h>
#include <limits>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw::cloud_delete {

namespace {

constexpr uint32_t batch_size = 100;
constexpr std::string_view cloud_delete_index_lock_name = "cloud_delete_index_lock";

// Per-tier S3 connection cache; credentials resolved from live zone config.
using ConnCache = std::map<std::string, std::unique_ptr<S3RESTConn>>;

// Filter drivers wrap placement tiers. Unwrap to the concrete backend tier.
rgw::sal::PlacementTier* unwrap_placement_tier(rgw::sal::PlacementTier* tier)
{
  while (auto* wrapped = dynamic_cast<rgw::sal::FilterPlacementTier*>(tier)) {
    tier = wrapped->get_next();
  }
  return tier;
}

/** Look up live tier config and return a cached S3 connection.
 *  Returns nullptr and sets out_ret on failure. */
S3RESTConn* get_cached_conn(const DoutPrefixProvider *dpp,
                            rgw::sal::Driver* driver,
                            const rgw_placement_rule& rule,
                            ConnCache& cache,
                            int* out_ret) {
  auto* zone = driver ? driver->get_zone() : nullptr;
  if (!zone) {
    if (out_ret) *out_ret = -EINVAL;
    return nullptr;
  }

  std::string cache_key = rule.to_str();
  if (auto it = cache.find(cache_key); it != cache.end()) {
    if (!it->second) {
      // Negative cache entry — tier previously failed lookup
      if (out_ret) *out_ret = -EAGAIN;
    }
    return it->second.get();
  }

  auto cache_negative = [&] {
    cache.emplace(std::move(cache_key), nullptr);
    if (out_ret) *out_ret = -EAGAIN;
  };

  // Runtime tier lookup — credentials come from live config, not FIFO entry
  rgw::sal::ZoneGroup& zg = zone->get_zonegroup();
  std::unique_ptr<rgw::sal::PlacementTier> tier;
  int r = zg.get_placement_tier(rule, &tier);
  if (r < 0 || !tier) {
    ldpp_dout(dpp, 1) << "ERROR: cloud delete tier lookup failed for "
                       << rule << ": " << cpp_strerror(r) << dendl;
    cache_negative();
    return nullptr;
  }
  if (!tier->is_tier_type_s3()) {
    ldpp_dout(dpp, 1) << "ERROR: cloud delete tier lookup failed for "
                       << rule << ": not an S3 tier" << dendl;
    cache_negative();
    return nullptr;
  }

  auto* resolved_tier = unwrap_placement_tier(tier.get());
  auto* rados_tier = dynamic_cast<rgw::sal::RadosPlacementTier*>(resolved_tier);
  if (!rados_tier) {
    ldpp_dout(dpp, 1) << "ERROR: cloud delete tier lookup failed for "
                       << rule << ": unsupported tier driver" << dendl;
    cache_negative();
    return nullptr;
  }
  const auto& s3 = rados_tier->get_rt().t.s3;

  auto conn = std::make_unique<S3RESTConn>(
      driver->ctx(), "cloudid", std::list<std::string>{s3.endpoint},
      s3.key, zg.get_id(), s3.region, s3.host_style);
  auto* raw = conn.get();
  cache.emplace(std::move(cache_key), std::move(conn));
  return raw;
}

bool is_terminal_success(int ret) { return ret == 0 || ret == -ENOENT; }

bool is_permanent_failure(int ret) {
  return ret == -EINVAL;
}

bool is_retryable(int ret) {
  // Retry by default. Any non-terminal error that is not known-permanent
  // should be retried with backoff to avoid dropping delete entries.
  return !is_terminal_success(ret) && !is_permanent_failure(ret);
}

/** HEAD a cloud resource and return response headers.
 *  Uses the same URL/signing as send_resource but captures response headers
 *  via the full complete_request overload. */
int head_resource(const DoutPrefixProvider* dpp,
                  S3RESTConn* conn,
                  const std::string& resource,
                  std::map<std::string, std::string>& out_headers,
                  optional_yield y) {
  std::string url;
  int ret = conn->get_url(url);
  if (ret < 0) return ret;

  param_vec_t params;
  bufferlist bl;
  RGWStreamIntoBufferlist cb(bl);

  RGWRESTStreamSendRequest req(conn->get_ctx(), "HEAD", url, &cb,
                               nullptr, &params, conn->get_api_name(),
                               conn->get_host_style());
  std::map<std::string, std::string> headers;
  ret = req.send_request(dpp, &conn->get_key(), headers, resource,
                         nullptr);
  if (ret < 0) return ret;

  return req.complete_request(dpp, y, nullptr, nullptr,
                              nullptr, nullptr, &out_headers);
}

// HEAD before DELETE: S3 DELETE returns 204 for missing keys, so we
// can't rely on ENOENT to detect whether the object exists.
//
// For null versions, the cloud key may be either "<obj>-null" (if
// transitioned as non-current) or "<obj>" (if transitioned from an
// unversioned bucket before versioning was enabled). HEAD both and
// delete whichever exists with matching etag.
int handle_entry(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
                 const CloudDeleteEntry& entry,
                 ConnCache& conn_cache, optional_yield y) {
  if (!driver) {
    ldpp_dout(dpp, 1) << "ERROR: cloud delete requires valid driver" << dendl;
    return -EINVAL;
  }
  int conn_ret = 0;
  S3RESTConn* conn = get_cached_conn(dpp, driver, entry.placement_rule,
                                     conn_cache, &conn_ret);
  if (!conn) {
    return conn_ret;
  }

  auto try_delete = [&](const std::string& name) {
    bufferlist out_bl, bl;
    std::string resource = entry.target_bucket_name + "/" + name;
    return conn->send_resource(dpp, "DELETE", resource, nullptr, nullptr,
                               out_bl, &bl, nullptr, y);
  };

  auto strip_quotes = [](std::string_view s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
      s.remove_prefix(1);
      s.remove_suffix(1);
    }
    return std::string{s};
  };

  /*
   * Generation guard: the cloud-side x-amz-meta-rgwx-source-mtime is
   * stamped at transition time to the source object's mtime. Restore
   * does not touch it; re-upload followed by re-transition bumps it.
   * An entry is safe to delete only when the cloud object pre-dates
   * our enqueue (i.e. was not re-transitioned since we queued this
   * delete). When the header is absent (foreign/legacy object that
   * wasn't transitioned by RGW), fall back to matching the remote
   * ETag against the entry's captured source etag.
   */
  auto parse_rgwx_mtime = [](std::string_view s, ceph::real_time* out) {
    auto dot = s.find('.');
    std::string_view sec_str = (dot == std::string_view::npos) ? s : s.substr(0, dot);
    std::string_view nsec_str = (dot == std::string_view::npos)
        ? std::string_view{} : s.substr(dot + 1);
    long long sec = 0, nsec = 0;
    auto parse_ll = [](std::string_view v, long long* dst) {
      if (v.empty()) { *dst = 0; return true; }
      try {
        size_t idx = 0;
        *dst = std::stoll(std::string{v}, &idx);
        return idx == v.size();
      } catch (...) { return false; }
    };
    if (!parse_ll(sec_str, &sec) || !parse_ll(nsec_str, &nsec)) return false;
    *out = ceph::real_clock::from_time_t(sec)
         + std::chrono::nanoseconds{nsec};
    return true;
  };

  auto generation_ok = [&](const std::map<std::string, std::string>& hdrs) {
    auto mtime_it = hdrs.find("X_AMZ_META_RGWX_SOURCE_MTIME");
    if (mtime_it != hdrs.end()) {
      ceph::real_time cloud_src_mtime;
      if (!parse_rgwx_mtime(mtime_it->second, &cloud_src_mtime)) {
        ldpp_dout(dpp, 5) << "cloud delete: unparseable rgwx-source-mtime='"
                          << mtime_it->second << "', vetoing" << dendl;
        return false;
      }
      // Cloud re-transitioned at or after enqueue → newer generation, veto.
      return cloud_src_mtime < entry.enqueue_time;
    }
    // No generation metadata. Fall back to remote ETag match for
    // foreign/legacy objects so we don't orphan them indefinitely.
    if (entry.src_etag.empty()) return true;
    auto etag_it = hdrs.find("ETAG");
    if (etag_it == hdrs.end()) return false;
    return strip_quotes(etag_it->second) == strip_quotes(entry.src_etag);
  };

  rgw_obj_key versioned_key = entry.src_key;
  if (versioned_key.instance.empty() && !entry.src_version_id.empty()) {
    versioned_key.set_instance(entry.src_version_id);
  }
  std::string target_name = make_target_obj_name(entry.src_bucket.name, versioned_key,
                                                  entry.target_by_bucket, false);

  // HEAD the expected name
  std::string resource = entry.target_bucket_name + "/" + target_name;
  std::map<std::string, std::string> resp_headers;
  int hret = head_resource(dpp, conn, resource, resp_headers, y);
  if (hret == 0) {
    if (generation_ok(resp_headers)) {
      return try_delete(target_name);
    }
    return 0;  // newer generation on cloud — reuploaded and re-transitioned
  }
  if (hret != -ENOENT) {
    return hret;
  }

  // for null versions, fall back to the unsuffixed head name
  if (!versioned_key.have_null_instance()) {
    return 0;  // not found, already gone
  }

  rgw_obj_key head_key = entry.src_key;
  head_key.instance.clear();
  std::string head_name = make_target_obj_name(entry.src_bucket.name, head_key,
                                                entry.target_by_bucket, true);
  if (head_name == target_name) {
    return 0;
  }

  resource = entry.target_bucket_name + "/" + head_name;
  resp_headers.clear();
  hret = head_resource(dpp, conn, resource, resp_headers, y);
  if (hret == -ENOENT) {
    return 0;
  }
  if (hret < 0) {
    return hret;
  }

  if (generation_ok(resp_headers)) {
    return try_delete(head_name);
  }

  return 0;
}

} // anonymous namespace

// CloudDelete wrapper implementation

CloudDelete::~CloudDelete() {
  stop_processor();
}

int CloudDelete::initialize(CephContext *_cct, rgw::sal::Driver* _driver) {
  cct = _cct;
  driver = _driver;

  /**
   * Seed pass_id from random space so stale pass_id values left in the FIFO
   * after restart/reload are overwhelmingly unlikely to collide with a new
   * shard pass boundary tag.
   */
  next_pass_id = ceph::util::generate_random_number<uint64_t>(
      1, std::numeric_limits<uint64_t>::max());

  max_objs = cct->_conf->rgw_cloud_delete_max_objs;
  if (max_objs > RGW_CLOUD_DELETE_MAX_SHARDS) {
    max_objs = RGW_CLOUD_DELETE_MAX_SHARDS;
  }

  obj_names.clear();
  for (int i = 0; i < max_objs; i++) {
    obj_names.push_back(fmt::format("delete.{}", i));
  }

  // Get SAL implementation from driver
  sal_cloud_delete = driver->get_cloud_delete();
  if (!sal_cloud_delete) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": failed to create SAL cloud delete" << dendl;
    return -EINVAL;
  }

  // Initialize SAL layer
  int ret = sal_cloud_delete->initialize(this, null_yield, max_objs, obj_names);
  if (ret < 0) {
    ldpp_dout(this, -1) << __PRETTY_FUNCTION__ << ": failed to initialize SAL: " << ret << dendl;
    return ret;
  }

  return 0;
}

int CloudDelete::enqueue(const DoutPrefixProvider* dpp, optional_yield y,
                         const CloudDeleteEntry& entry) {
  if (!sal_cloud_delete) return -EINVAL;

  int ret = sal_cloud_delete->enqueue(dpp, y, entry);
  if (ret < 0) return ret;

  if (::perfcounter) ::perfcounter->inc(l_rgw_cloud_delete_queued);

  return 0;
}

int CloudDelete::list_entries(const DoutPrefixProvider* dpp, optional_yield y,
                              int index, const std::string& marker,
                              std::string* out_marker, uint32_t max_entries,
                              std::vector<CloudDeleteEntry>& entries,
                              bool* truncated) {
  if (!sal_cloud_delete) return -EINVAL;
  if (index < 0 || index >= max_objs) return -EINVAL;
  return sal_cloud_delete->list_entries(dpp, y, index, marker, out_marker,
                                         max_entries, entries, truncated);
}

void CloudDelete::start_processor() {
  down_flag.store(false, std::memory_order_release);
  if (!worker) {
    worker = std::make_unique<Worker>(this);
  }
  if (!worker->is_started()) {
    worker->create("rgw_cloud_del");
  }
}

void CloudDelete::stop_processor() {
  down_flag.store(true, std::memory_order_release);
  if (worker) {
    if (worker->is_started()) {
      worker->wake();
      worker->join();
    }
    worker.reset();
  }
}

unsigned CloudDelete::get_subsys() const {
  return dout_subsys;
}

std::ostream& CloudDelete::gen_prefix(std::ostream& out) const {
  return out << "cloud_delete: ";
}

// Worker thread implementation

void CloudDelete::Worker::wake() {
  std::lock_guard l{lock};
  cond.notify_one();
}

void *CloudDelete::Worker::entry() {
  while (true) {
    ceph::real_time next_retry;
    if (int ret = parent->process(null_yield, &next_retry); ret < 0) {
      ldpp_dout(parent, 5) << "cloud delete worker process failed with " << ret << dendl;
    }
    if (parent->down_flag.load(std::memory_order_acquire)) break;

    // Calculate sleep time: use the shorter of interval or time until next retry
    auto interval = std::chrono::seconds(parent->cct->_conf->rgw_cloud_delete_interval);
    auto sleep_duration = interval;

    if (next_retry != ceph::real_time::max()) {
      auto now = ceph::real_clock::now();
      if (next_retry <= now) {
        sleep_duration = std::chrono::seconds::zero();
      } else {
        auto until_retry = std::chrono::duration_cast<std::chrono::seconds>(next_retry - now);
        if (until_retry < interval) {
          sleep_duration = until_retry;
        }
      }
    }

    std::unique_lock l{lock};
    cond.wait_for(l, sleep_duration, [this] {
      return parent->down_flag.load(std::memory_order_acquire);
    });
    if (parent->down_flag.load(std::memory_order_acquire)) break;
  }
  return nullptr;
}

// Processing logic

/* CloudDeleteLockAdapter: unique_lock expects unlock() taking no arguments,
 * but CloudDeleteSerializer::unlock() requires two. This adapter binds them.
 * Set lock_lost=true when renewal fails to suppress the unlock() call in the
 * unique_lock destructor — we no longer own the lock. */
struct CloudDeleteLockAdapter {
  rgw::sal::CloudDeleteSerializer& serializer;
  const DoutPrefixProvider* dpp = nullptr;
  optional_yield y;
  bool lock_lost{false};

  void lock() {}

  void unlock() {
    if (!lock_lost) {
      serializer.unlock(dpp, y);
    }
  }
};

int CloudDelete::process(optional_yield y, ceph::real_time* earliest_retry) {
  if (!sal_cloud_delete) return -EINVAL;

  int interval_secs = cct->_conf->rgw_cloud_delete_interval;

  /* Lock TTL is 1.25x the poll interval so that processing a shard
   * that takes slightly longer than one interval doesn't lose the lock.
   * We renew the lease inside the batch loop to handle arbitrarily
   * long processing runs. */
  int lock_secs = static_cast<int>(std::max((int64_t{interval_secs} * 5) / 4, int64_t{interval_secs} + 60));
  utime_t lock_dur(lock_secs, 0);

  ceph::real_time min_retry_time = ceph::real_time::max();
  ConnCache conn_cache;
  int overall_ret = 0;

  int start = ceph::util::generate_random_number(0, max_objs ? max_objs - 1 : 0);
  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;

    /* Acquire per-shard exclusive lock so multiple RGW instances
     * don't process the same FIFO shard concurrently. */
    std::unique_ptr<rgw::sal::CloudDeleteSerializer> serializer =
        sal_cloud_delete->get_serializer(
            std::string(cloud_delete_index_lock_name),
            std::string(obj_names[index]),
            worker ? worker->thr_name() : std::string("cloud_del"));

    int lret = serializer->try_lock(this,
                                    std::chrono::nanoseconds{lock_dur.to_nsec()},
                                    y);
    if (lret == -EBUSY || lret == -EEXIST) {
      ldpp_dout(this, 10) << "cloud delete shard " << index
                          << " locked by another processor, skipping" << dendl;
      continue;
    }
    if (lret < 0) {
      ldpp_dout(this, 1) << "WARNING: cloud delete failed to acquire lock on shard "
                         << index << ": " << lret << dendl;
      continue;
    }

    auto lock_adapter = CloudDeleteLockAdapter{*serializer, this, y};
    std::unique_lock<CloudDeleteLockAdapter> lock(lock_adapter, std::adopt_lock);
    auto lock_acquired = ceph::real_clock::now();

    /* Queue-boundary tagging: a monotonic pass_id identifies entries we
     * re-enqueue during this shard pass. When we encounter our own
     * pass_id we know we've reached our tail and must stop. */
    const uint64_t pass_id = next_pass_id++;

    std::string marker;
    bool truncated = false;
    do {
      std::vector<CloudDeleteEntry> entries;
      std::string out_marker;
      truncated = false;

      int ret = sal_cloud_delete->list_entries(this, y, index, marker, &out_marker,
                                                batch_size, entries, &truncated);
      if (ret < 0) {
        ldpp_dout(this, 1) << "WARNING: list failed for cloud delete shard " << index
                           << ": " << ret << dendl;
        if (overall_ret == 0 || ret < overall_ret) {
          overall_ret = ret;
        }
        break;
      }
      if (entries.empty()) {
        if (!out_marker.empty()) {
          ldpp_dout(this, 1) << "WARNING: all entries in batch were undecodable"
                             << " for shard " << index << ", trimming past" << dendl;
          sal_cloud_delete->trim_entries(this, y, index, out_marker);
          marker = out_marker;
          continue;
        }
        break;
      }

      std::vector<CloudDeleteEntry> backoff_entries;
      ceph::real_time now = ceph::real_clock::now();
      bool hit_boundary = false;
      bool any_attempted = false;

      for (auto& e : entries) {
        // Boundary check: stop when we reach entries we re-enqueued
        // during this same pass — they carry our pass_id.
        if (e.pass_id == pass_id) {
          hit_boundary = true;
          break;
        }

        if (e.next_retry_time > now) {
          backoff_entries.push_back(e);
          min_retry_time = std::min(min_retry_time, e.next_retry_time);
          continue;
        }

        any_attempted = true;
        int dret = handle_entry(this, driver, e, conn_cache, y);
        if (is_terminal_success(dret)) {
          if (::perfcounter) ::perfcounter->inc(l_rgw_cloud_delete_success);
          continue;
        }

        auto retry_window = std::chrono::seconds(cct->_conf->rgw_cloud_delete_retry_window);
        bool within_window = (now - e.enqueue_time) < retry_window;

        if (is_retryable(dret) && within_window) {
          e.retry_count++;
          constexpr uint32_t base_backoff_secs = 900;    // 15 minutes
          constexpr uint32_t max_backoff_secs  = 28800;  // 8 hours

          const uint32_t uncapped = base_backoff_secs
              << std::min<uint32_t>(e.retry_count - 1, 20);
          const uint32_t backoff_secs = std::min(uncapped, max_backoff_secs);
          e.next_retry_time = now + std::chrono::seconds(backoff_secs);

          ldpp_dout(this, 10) << "Cloud delete retry " << e.retry_count
                              << " for " << e.src_bucket.name << "/" << e.src_key.name
                              << " backoff=" << backoff_secs << "s"
                              << " error=" << dret << dendl;

          backoff_entries.push_back(e);
          min_retry_time = std::min(min_retry_time, e.next_retry_time);
          if (::perfcounter) ::perfcounter->inc(l_rgw_cloud_delete_retry);
          continue;
        }

        ldpp_dout(this, 1) << "WARNING: Cloud delete permanently failed"
                           << " (error=" << dret << ", retries=" << e.retry_count
                           << ", age=" << std::chrono::duration_cast<std::chrono::seconds>(
                                  now - e.enqueue_time).count() << "s"
                           << ") - remote object may be orphaned" << dendl;
        ldpp_dout(this, 10) << "Failed: bucket=" << e.src_bucket.name
                            << " key=" << e.src_key.name
                            << " version=" << e.src_version_id << dendl;
        if (::perfcounter) ::perfcounter->inc(l_rgw_cloud_delete_fail);
      }

      /* Fast path: if nothing was attempted (entire batch is sleeping)
       * and there are no more pages, leave entries in place at the FIFO
       * head — zero writes.  When truncated, fall through to the normal
       * re-enqueue + trim path to rotate sleeping entries past any due
       * entries on later pages (avoids head-of-line blocking). */
      if (!any_attempted && !hit_boundary && !truncated) {
        break;
      }

      /* Re-enqueue backoff entries with pass_id so we recognise them
       * if the scan wraps around to the FIFO tail. Re-enqueue before
       * trim so that a crash between the two only creates duplicates,
       * never drops entries (at-least-once). */
      bool reenq_failed = false;
      for (auto& be : backoff_entries) {
        be.pass_id = pass_id;  // stamp with boundary tag
        if (int rret = sal_cloud_delete->enqueue(this, y, be); rret < 0) {
          reenq_failed = true;
          ldpp_dout(this, 1) << "WARNING: failed to re-enqueue backoff entry: "
                             << be.src_bucket.name << "/" << be.src_key.name
                             << " error=" << rret
                             << " - preserving originals by skipping trim" << dendl;
          if (::perfcounter) ::perfcounter->inc(l_rgw_cloud_delete_fail);
        }
      }

      /* Trim decision:
       *  - boundary hit: batch may contain our tagged entries, skip trim
       *    to avoid deleting them.  At most batch_size-1 already-processed
       *    entries remain; they are re-processed next cycle (idempotent).
       *  - reenqueue failed: skip trim to preserve originals.
       *  - otherwise: safe to trim the entire batch. */
      if (hit_boundary) {
        ldpp_dout(this, 10) << "cloud delete shard " << index
                            << " reached pass boundary, stopping scan" << dendl;
      } else if (reenq_failed) {
        ldpp_dout(this, 1) << "WARNING: cloud delete skipped trim for shard " << index
                           << " to avoid dropping retry entries after re-enqueue failure"
                           << dendl;
      } else {
        int trim_ret = sal_cloud_delete->trim_entries(this, y, index, out_marker);
        if (trim_ret < 0) {
          ldpp_dout(this, 1) << "WARNING: trim failed for index " << index
                             << ": " << trim_ret
                             << " - retry entries may be duplicated but not lost"
                             << dendl;
        }
      }

      if (hit_boundary) {
        break; // done with this shard for this pass
      }

      /* Renew the shard lock if we've used more than half the TTL. */
      auto elapsed = ceph::real_clock::now() - lock_acquired;
      if (elapsed > std::chrono::seconds(lock_secs / 2)) {
        int rret = serializer->try_lock(this,
                                        std::chrono::nanoseconds{lock_dur.to_nsec()},
                                        y);
        if (rret < 0) {
          ldpp_dout(this, 1) << "WARNING: failed to renew cloud delete lock on shard "
                             << index << ": " << rret
                             << " - another processor may have taken over" << dendl;
          lock_adapter.lock_lost = true;
          break;
        }
        lock_acquired = ceph::real_clock::now();
        ldpp_dout(this, 20) << "renewed cloud delete lock on shard " << index << dendl;
      }

      marker = out_marker;
    } while (truncated && !down_flag.load(std::memory_order_acquire));

    if (down_flag.load(std::memory_order_acquire)) {
      if (earliest_retry) *earliest_retry = min_retry_time;
      return overall_ret;
    }
  }
  if (earliest_retry) *earliest_retry = min_retry_time;
  return overall_ret;
}

std::unique_ptr<CloudDelete> make_cloud_delete() {
  return std::make_unique<CloudDelete>();
}

} // namespace rgw::cloud_delete
