// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "include/buffer.h"
#include "include/encoding.h"

#include "common/Formatter.h"

#include "rgw_sal_rados.h"

namespace rgw::sync {
using namespace std::literals;

namespace buffer = ceph::buffer;
using namespace std::literals;

struct error_info {
  std::string source_zone;
  std::uint32_t error_code = 0;
  std::string message;

  error_info() = default;
  error_info(std::string source_zone, std::uint32_t error_code,
	     std::string message)
    : source_zone(std::move(source_zone)), error_code(error_code),
      message(std::move(message)) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(source_zone, bl);
    encode(error_code, bl);
    encode(message, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(source_zone, bl);
    decode(error_code, bl);
    decode(message, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const;
};
WRITE_CLASS_ENCODER(error_info)


class ErrorLoggerBase {
public:
  static constexpr auto SHARDS = 32;
  static constexpr auto PREFIX = "sync.error-log"sv;
protected:
  sal::RadosStore* const store;

  const std::string& next_oid() {
    return oids[++counter % num_shards];
  }

private:
  const int num_shards;

  std::vector<std::string> oids;
  std::atomic<int64_t> counter = { 0 };

public:
  ErrorLoggerBase(rgw::sal::RadosStore* store,
		  std::string_view oid_prefix = PREFIX,
		  int num_shards = SHARDS);


  static std::string get_shard_oid(std::string_view oid_prefix,
				   int shard_id);
};

/// Base for an exponential backoff, managing the actual time values.
///
/// This class and its descendants are not thread-safe and must called
/// from a single thread or otherwise protected.
class BackoffBase {
protected:
  /// The default maximum backoff
  static constexpr auto DEFAULT_MAX = 30s;

  /// Maximum backoff
  const std::chrono::seconds max;
  /// current backoff time
  std::chrono::seconds cur_wait = 0s;

  /// Double (up to a ceiling) the backoff time, to be called from
  /// every wait function.
  void update_wait_time();
public:
  explicit BackoffBase(std::chrono::seconds max = DEFAULT_MAX)
    : max(max) {}

  /// reset wait time to 0.
  void reset() {
    cur_wait = 0s;
  }
};
}
