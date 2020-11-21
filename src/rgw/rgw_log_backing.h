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

#include "include/rados/librados.hpp"
#include "include/expected.hpp"
#include "include/function2.hpp"

#include "common/async/yield_context.h"

namespace bs = boost::system;

/// Type of log backing, stored in the mark used in the quick check,
/// and passed to checking functions.
enum class log_type {
  omap = 0,
  fifo = 1
};

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


#endif
