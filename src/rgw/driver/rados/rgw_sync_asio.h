// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <atomic>
#include <coroutine>
#include <memory>
#include <string>
#include <string_view>

#include <boost/asio/awaitable.hpp>

#include "common/dout.h"

#include "rgw_sal_rados.h"
#include "rgw_sync_common.h"

namespace rgw::sync {
namespace asio = boost::asio;

/// An Asio sync error logger
///
/// Log sync errors and messages to Omap using cls_log.
///
/// This class actually is thread safe.
class ErrorLogger : public ErrorLoggerBase {
  const neorados::IOContext ioc;

  ErrorLogger(rgw::sal::RadosStore* store,
	      neorados::IOContext ioc,
	      std::string_view oid_prefix = PREFIX,
	      int num_shards = SHARDS)
    : ErrorLoggerBase(store, oid_prefix, num_shards),
      ioc(std::move(ioc)) {}
public:

  /// Create an ErrorLogger;
  friend asio::awaitable<std::unique_ptr<ErrorLogger>> create_error_logger(
    const DoutPrefixProvider* dpp, sal::RadosStore* store);

  /// Log an error
  asio::awaitable<void> log_error(const DoutPrefixProvider* dpp,
				  std::string source_zone, std::string section,
				  std::string name, uint32_t error_code,
				  std::string message);
};

asio::awaitable<std::unique_ptr<ErrorLogger>> create_error_logger(
  const DoutPrefixProvider* dpp, sal::RadosStore* store);

/// A C++ Coroutine based Backoff class, providing exponential backoff
/// up to a given maximum
///
/// This class is not thread-safe
class Backoff : public BackoffBase {
public:
  explicit Backoff(std::chrono::seconds max = DEFAULT_MAX)
    : rgw::sync::BackoffBase(max) {}

  /// Increase wait time and wait
  ///
  /// This method is not thread-safe and must be called from a strand.
  asio::awaitable<void> backoff();
};
}
