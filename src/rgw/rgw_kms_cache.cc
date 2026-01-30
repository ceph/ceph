// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "rgw_kms_cache.h"

#include <boost/asio/associated_executor.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <variant>

#include "common/async/yield_context.h"
#include "common/dout.h"
#include "common/dout_fmt.h"
#include "common/keyring.h"
#include "include/ceph_assert.h"
#include "include/function2.hpp"
#include "rgw_perf_counters.h"
#include "rgw_string.h"

#define dout_context g_ceph_context
#define dout_subsys  ceph_subsys_rgw

namespace rgw::kms {

std::jthread KMSCache::make_ttl_reaper_thread(
    CephContext* cct, KMSSecretCache& cache, std::chrono::seconds ttl) {
  return std::jthread([cct, &cache, ttl](std::stop_token stop) {
    const std::string thread_name = fmt::format("{}-ttl-reaper", cache.name());
    ceph_pthread_setname(thread_name.c_str());
    std::mutex mutex;
    std::condition_variable cond;
    std::stop_callback on_stop(stop, [&cond]() { cond.notify_all(); });
    ldout(cct, 10) << "KMS Cache: Starting TTL reaper thread " << thread_name
                   << "/" << std::hex << std::this_thread::get_id() << std::dec
                   << ", running every " << ttl << dendl;
    while (!stop.stop_requested()) {
      std::unique_lock<std::mutex> lock(mutex);
      if (cond.wait_for(lock, ttl, [&stop] { return stop.stop_requested(); })) {
        break;
      }
      const auto expired_count = cache.expire_erase();
      ldout(cct, 20) << "KMS Cache: TTL reaper thread expired " << expired_count
                     << " entries" << dendl;
    }
    ldout(cct, 10) << "KMS Cache: Stopping TTL reaper thread " << thread_name
                   << "/" << std::hex << std::this_thread::get_id() << std::dec
                   << dendl;
  });
}

void KMSCache::make_ttl_reaper_async(
    CephContext* cct, KMSSecretCache& cache, std::chrono::seconds ttl,
    const boost::asio::io_context::executor_type& executor,
    boost::asio::cancellation_signal& cancel_signal) {
  auto context = make_strand(executor);
  boost::asio::spawn(
      context,
      [cct, &cache, ttl](const boost::asio::yield_context& yield) {
        ldout(cct, 10) << "KMS Cache: Starting async TTL reaper, running every "
                       << ttl << dendl;
        boost::asio::steady_timer timer(yield.get_executor());
        while (true) {
          timer.expires_after(ttl);
          boost::system::error_code ec;
          timer.async_wait(yield[ec]);
          if (ec == boost::asio::error::operation_aborted) {
            ldout(cct, 10) << "KMS Cache: Stopping async TTL reaper" << dendl;
            break;
          }
          const auto expired_count = cache.expire_erase();
          ldout(cct, 20) << "KMS Cache: Async TTL reaper expired "
                         << expired_count << " entries" << dendl;
        }
      },
      boost::asio::bind_cancellation_slot(
          cancel_signal.slot(),
          boost::asio::bind_executor(context, boost::asio::detached)));
}

KMSCache::KMSCache(
    CephContext* _cct, std::unique_ptr<Keyring> _keyring)
    : cct(_cct), keyring(std::move(_keyring)) {
  ldout(cct, 10) << fmt::format(
                        "KMS Cache: Initializing size:{} TTL pos:{} "
                        "neg:{} err:{}",
                        cct->_conf->rgw_crypt_s3_kms_cache_max_size,
                        cct->_conf->rgw_crypt_s3_kms_cache_positive_ttl,
                        cct->_conf->rgw_crypt_s3_kms_cache_negative_ttl,
                        cct->_conf->rgw_crypt_s3_kms_cache_transient_error_ttl)
                 << dendl;
  cache = std::make_unique<KMSSecretCache>(
      cct, "kms-cache", cct->_conf->rgw_crypt_s3_kms_cache_max_size,
      std::chrono::seconds(cct->_conf->rgw_crypt_s3_kms_cache_positive_ttl));

  std::error_code ec;
  if (!keyring->supported(&ec)) {
    ldout(cct, 1) << "KMS Cache: " << keyring->name() << " unsupported (error "
                  << ec << "). Disabling Cache." << dendl;
    cct->_conf->rgw_crypt_s3_kms_cache_enabled = false;
  }
}

KMSCache::~KMSCache() {
  stop_ttl_reaper();
}

void KMSCache::initialize_ttl_reaper(
    std::optional<boost::asio::io_context::executor_type> executor) {
  ceph_assert(cache);
  if (reaper_initialized()) {
    return;
  }
  const auto min_ttl_secs = std::min(
      {cct->_conf->rgw_crypt_s3_kms_cache_positive_ttl,
       cct->_conf->rgw_crypt_s3_kms_cache_negative_ttl,
       cct->_conf->rgw_crypt_s3_kms_cache_transient_error_ttl});
  if (executor.has_value()) {
    reaper_state.emplace<boost::asio::cancellation_signal>();
    make_ttl_reaper_async(
        cct, *cache, std::chrono::seconds(min_ttl_secs), executor.value(),
        std::get<boost::asio::cancellation_signal>(reaper_state));
  } else {
    reaper_state.emplace<std::jthread>(make_ttl_reaper_thread(
        cct, *cache, std::chrono::seconds(min_ttl_secs)));
  }
}

void KMSCache::stop_ttl_reaper() {
  ceph_assert(cache);
  std::visit(
      fu2::overload(
          [](const std::monostate& mono) {},
          [this](boost::asio::cancellation_signal& signal) {
	    ceph_assert(reaper_strand);
	    boost::asio::post(*reaper_strand,
			      [&signal] { signal.emit(boost::asio::cancellation_type::all); });
          },
          [&](const std::jthread&) { reaper_state.emplace<std::monostate>(); }),
      reaper_state);
}

void KMSCache::clear_cache() const {
  cache->clear();
}

int KMSCache::do_cache(
    const DoutPrefixProvider* dpp, const std::string& key_prefix_kms,
    const std::string& key_id, const FetchFn& fetch, std::string& actual_key,
    optional_yield y) {
  static std::string_view key_prefix("rgw_sse_");
  const std::string cache_key =
      string_cat_reserve(key_prefix, key_prefix_kms, "_", key_id);
  std::shared_ptr<KMSCache::CacheValue> value =
      cache->lookup_or(key_id, std::make_shared<KMSCache::CacheValue>());
  auto result = call_once(
      *value, y,
      [&dpp, &fetch, &cache_key, &value, &key_prefix_kms, &key_id,
       this]() -> KMSCache::CacheResult {
        std::string secret;
        const int ret = fetch(secret);
        ldpp_dout(dpp, 20) << "KMS Cache: " << cache_key
                           << " call_once fetched with ret " << ret << dendl;

        if (ret == -ENOENT) {  // key does not exists, treat as permanent error
          ldpp_dout(dpp, 15)
              << "KMS Cache: " << cache_key
              << " key does not exists. treating as permanent error " << dendl;
          cache->update_ttl_if(
              cache_key, value,
              std::chrono::seconds(
                  dpp->get_cct()->_conf->rgw_crypt_s3_kms_cache_negative_ttl));
          perfcounter->inc(l_rgw_kms_error_permanent);
          return tl::unexpected(ret);
        } else if (ret < 0) {  // treat other errors as transient
          ldpp_dout(dpp, 15)
              << "KMS Cache: " << cache_key << " fetch error (" << ret << ") "
              << std::strerror(ret) << " treating as transient error " << dendl;
          cache->update_ttl_if(
              cache_key, value,
              std::chrono::seconds(
                  dpp->get_cct()
                      ->_conf->rgw_crypt_s3_kms_cache_transient_error_ttl));
          perfcounter->inc(l_rgw_kms_error_transient);
          return tl::unexpected(ret);
        }

        // This function might be in flight for the same key_id more than
        // once. The keyring key must, however, be unique to not refer
        // (and remove) the same key twice.
        uuid_d uuid;
        uuid.generate_random();
        const std::string keyring_key = string_cat_reserve(
            key_prefix, key_prefix_kms, "_", key_id, "_v", uuid.to_string());
        auto keyring_secret = keyring->add(keyring_key, secret);
        ceph::crypto::zeroize_for_security(secret.data(), secret.length());
        if (!keyring_secret) {
          ldpp_dout(dpp, 5)
              << "KMS Cache: " << cache_key << " keyring add error ("
              << keyring_secret.error()
              << "). removing from cache. disabling cache." << dendl;
          cache->remove_if(cache_key, value);
          disable_cache();
          perfcounter->inc(l_rgw_kms_error_secret_store);
          return tl::unexpected(-ERR_INTERNAL_ERROR);
        }
        return std::move(keyring_secret.value());
      });

  ldpp_dout_fmt(
      dpp, 20, "KMS Cache: {} -> {}/{}", cache_key,
      result && result.has_value() ? fmt::format("{}", *result.value()) : "-",
      !result ? result.error() : 0);

  if (result) {
    if (auto ret = result.value()->read(actual_key); ret.value() != 0) {
      ldpp_dout(dpp, 5) << "KMS Cache: " << cache_key << " keyring "
                        << *result.value() << " read error (" << ret
                        << "). removing from cache. disabling cache." << dendl;
      cache->remove_if(cache_key, value);
      disable_cache();
      perfcounter->inc(l_rgw_kms_error_secret_store);
      return -ERR_INTERNAL_ERROR;
    }
    return 0;
  } else {
    return result.error();
  }
}

}  // namespace rgw::kms
