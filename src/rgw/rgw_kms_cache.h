#pragma once

#include <sys/stat.h>

#include <chrono>
#include <thread>
#include <variant>

#include "common/async/call_once.h"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "common/web_cache.h"
#include "include/expected.hpp"

namespace ceph {
class LinuxKeyringSecret;
};

namespace rgw::kms {

class KMSCache {
  CephContext* cct;

 public:
  using SharedSecret = std::shared_ptr<LinuxKeyringSecret>;
  using CacheResult = tl::expected<SharedSecret, int>;
  using CacheValue = ceph::async::once_result<CacheResult>;
  using KMSSecretCache = webcache::WebCache<std::string, CacheValue>;
  using FetchFn = std::function<int(std::string&)>;

  std::unique_ptr<KMSSecretCache> cache;

  // The TTL Reaper is either a service thread we own or an async running on
  // an executor elsewhere where we keep a cancellation signal.
  std::variant<std::monostate, std::jthread, boost::asio::cancellation_signal>
      reaper_state;

  KMSCache(const KMSCache&) = delete;
  KMSCache(KMSCache&&) = delete;
  KMSCache& operator=(const KMSCache&) = delete;
  KMSCache& operator=(KMSCache&&) = delete;
  explicit KMSCache(CephContext* _cct);
  virtual ~KMSCache();

  void initialize_ttl_reaper(
      std::optional<boost::asio::io_context::executor_type> executor);
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

  int do_cache(
      const DoutPrefixProvider* dpp, const std::string& kms_backend,
      const std::string& key_id, const FetchFn& fetch, std::string& actual_key,
      optional_yield y);

  void disable_cache() { cct->_conf->rgw_crypt_s3_kms_cache_enabled = false; }

 private:
  friend class TestKMSCacheReaperLifecycle;
  friend class TestKMSCacheReaperLifecycle_Threaded_Test;
  friend class TestKMSCacheReaperLifecycle_Async_Test;
};
}  // namespace rgw::kms
