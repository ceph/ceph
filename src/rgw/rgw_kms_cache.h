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

#pragma once

#include <sys/stat.h>

#include <chrono>
#include <thread>
#include <variant>

#include <boost/asio/executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include "common/async/call_once.h"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "common/keyring.h"
#include "common/web_cache.h"
#include "include/expected.hpp"

namespace rgw::kms {

// Cache RGW KMS Secrets
//
// Manages a WebCache instance plus TTL reaper. Adds cache stampede
// mitigation when fetching new values using (async) call once.
class KMSCache {
  CephContext* cct;

 public:
  using SharedSecret = std::shared_ptr<KeyringSecret>;
  using CacheResult = tl::expected<SharedSecret, int>;
  using CacheValue = ceph::async::once_result<CacheResult>;
  using KMSSecretCache = webcache::WebCache<std::string, CacheValue>;
  using FetchFn = std::function<int(std::string&)>;

 protected:
  std::unique_ptr<KMSSecretCache> cache;
  std::unique_ptr<Keyring> keyring;

  // The TTL Reaper is either a service thread we own or async running on
  // an executor elsewhere (where we keep a cancellation signal).
  std::variant<std::monostate, std::jthread, boost::asio::cancellation_signal>
      reaper_state;

  std::optional<boost::asio::strand<boost::asio::io_context::executor_type>> reaper_strand;

 public:
  KMSCache() = delete;
  KMSCache(const KMSCache&) = delete;
  KMSCache(KMSCache&&) = delete;
  KMSCache& operator=(const KMSCache&) = delete;
  KMSCache& operator=(KMSCache&&) = delete;
  KMSCache(CephContext* _cct, std::unique_ptr<Keyring> _keyring);
  virtual ~KMSCache();

  // Initialize a TTL reaper. Either threaded or async depending on
  // the executor parameter
  void initialize_ttl_reaper(
      std::optional<boost::asio::io_context::executor_type> executor);

  // Stop any type of TTL reaper running
  void stop_ttl_reaper();

  [[nodiscard]] bool reaper_initialized() const noexcept {
    return !std::holds_alternative<std::monostate>(reaper_state);
  }

  static std::jthread make_ttl_reaper_thread(
      CephContext* cct, KMSSecretCache& cache, std::chrono::seconds ttl);

  static void make_ttl_reaper_async(
      CephContext* cct, KMSSecretCache& cache, std::chrono::seconds ttl,
      const boost::asio::io_context::executor_type& executor,
      boost::asio::cancellation_signal& cancel_signal);

  void clear_cache() const;

  // Retrieve a cached KMS secret or fetch, cache, and return a KMS
  // secret.
  //
  // The secret is returned via actual_key and it is up to the caller
  // to properly clear the secret from memory. The cache may be shared
  // between SSE-KMS and SSE-S3, where key_kms_prefix is available to
  // partition the key namespace.
  int do_cache(
      const DoutPrefixProvider* dpp, const std::string& key_kms_prefix,
      const std::string& key_id, const FetchFn& fetch, std::string& actual_key,
      optional_yield y);

  void disable_cache() { cct->_conf->rgw_crypt_s3_kms_cache_enabled = false; }
};
}  // namespace rgw::kms
