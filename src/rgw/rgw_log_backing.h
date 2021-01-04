// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_LOGBACKING_H
#define CEPH_RGW_LOGBACKING_H

#include <optional>
#include <iostream>
#include <string>
#include <string_view>

#include <strings.h>

#include <boost/system/error_code.hpp>

#undef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY 1
#include <fmt/format.h>

#include "include/rados/librados.hpp"
#include "include/encoding.h"
#include "include/expected.hpp"
#include "include/function2.hpp"

#include "common/async/yield_context.h"
#include "common/Formatter.h"
#include "common/strtol.h"

namespace bs = boost::system;

/// Type of log backing, stored in the mark used in the quick check,
/// and passed to checking functions.
enum class log_type {
  omap = 0,
  fifo = 1
};

inline void encode(const log_type& type, ceph::buffer::list& bl) {
  auto t = static_cast<uint8_t>(type);
  encode(t, bl);
}

inline void decode(log_type& type, bufferlist::const_iterator& bl) {
  uint8_t t;
  decode(t, bl);
  type = static_cast<log_type>(type);
}

inline std::optional<log_type> to_log_type(std::string_view s) {
  if (strncasecmp(s.data(), "omap", s.length()) == 0) {
    return log_type::omap;
  } else if (strncasecmp(s.data(), "fifo", s.length()) == 0) {
    return log_type::fifo;
  } else {
    return std::nullopt;
  }
}
inline std::ostream& operator <<(std::ostream& m, const log_type& t) {
  switch (t) {
  case log_type::omap:
    return m << "log_type::omap";
  case log_type::fifo:
    return m << "log_type::fifo";
  }

  return m << "log_type::UNKNOWN=" << static_cast<uint32_t>(t);
}

/// Look over the shards in a log and determine the type.
tl::expected<log_type, bs::error_code>
log_backing_type(librados::IoCtx& ioctx,
		 log_type def,
		 int shards, //< Total number of shards
		 /// A function taking a shard number and
		 /// returning an oid.
		 const fu2::unique_function<std::string(int) const>& get_oid,
		 optional_yield y);

/// Remove all log shards and associated parts of fifos.
bs::error_code log_remove(librados::IoCtx& ioctx,
			  int shards, //< Total number of shards
			  /// A function taking a shard number and
			  /// returning an oid.
			  const fu2::unique_function<std::string(int) const>& get_oid,
			  optional_yield y);


struct logback_generation {
  uint64_t gen_id = 0;
  log_type type;
  bool empty = false;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(gen_id, bl);
    encode(type, bl);
    encode(empty, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(gen_id, bl);
    decode(type, bl);
    decode(empty, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(logback_generation)

inline std::string gencursor(uint64_t gen_id, std::string_view cursor) {
  return (gen_id > 0 ?
	  fmt::format("G{:0>20}@{}", gen_id, cursor) :
	  std::string(cursor));
}

inline std::pair<uint64_t, std::string_view>
cursorgen(std::string_view cursor_) {
  std::string_view cursor = cursor_;
  if (cursor[0] != 'G') {
    return { 0, cursor };
  }
  cursor.remove_prefix(1);
  auto gen_id = ceph::consume<uint64_t>(cursor);
  if (!gen_id || cursor[0] != '@') {
    return { 0, cursor_ };
  }
  cursor.remove_prefix(1);
  return { *gen_id, cursor };
}

#endif
