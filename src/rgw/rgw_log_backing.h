// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_LOGBACKING_H
#define CEPH_RGW_LOGBACKING_H

#include <optional>
#include <iostream>
#include <string>
#include <string_view>

#include <strings.h>

#include "include/rados/librados.hpp"
#include "include/function2.hpp"

#include "common/async/yield_context.h"

/// Type of log backing, stored in the mark used in the quick check,
/// and passed to checking functions.
enum class log_type {
  neither = 0, //< "auto" in the config, invalid/indeterminate when returned.
  omap = 1,
  fifo = 2
};

inline std::optional<log_type> to_log_type(std::string_view s) {
  if (strncasecmp(s.data(), "auto", s.length()) == 0) {
    return log_type::neither;
  } else if (strncasecmp(s.data(), "omap", s.length()) == 0) {
    return log_type::omap;
  } else if (strncasecmp(s.data(), "fifo", s.length()) == 0) {
    return log_type::fifo;
  } else {
    return std::nullopt;
  }
}
inline std::ostream& operator <<(std::ostream& m, const log_type& t) {
  switch (t) {
  case log_type::neither:
    return m << "log_type::neither";
  case log_type::omap:
    return m << "log_type::omap";
  case log_type::fifo:
    return m << "log_type::fifo";
  }

  return m << "log_type::UNKNOWN=" << static_cast<uint32_t>(t);
}

/// Quickly check the log's backing store. Will check an OMAP value on
/// the zeroth shard.
///
/// Returns `omap` or `fifo` if the key is set, well-formed,
/// and concordant with the specified log type.
///
/// Returns `neither` otherwise
log_type log_quick_check(librados::IoCtx& ioctx,
			 log_type specified, //< Log type from configuration
			 /// A function taking a shard number and
			 /// returning an oid.
			 const fu2::unique_function<
			   std::string(int) const>& get_oid,
			 optional_yield y);

// Status from the long check
enum class log_check {
  concord = 0, //< Backing store format is compatible with that specified
  discord = 1, //< Log is well-formed, but of an incompatible format
  corruption = 2 //< Log is ill-formed. Mix of shard types, objects
		 //< exist but are valid as neither type, etc.
};
inline std::ostream& operator <<(std::ostream& m, const log_check& t) {
  switch (t) {
  case log_check::concord:
    return m << "log_check::concord";
  case log_check::discord:
    return m << "log_check::discord";
  case log_check::corruption:
    return m << "log_check::corruption";
  }

  return m << "log_check::UNKNOWN=" << static_cast<uint32_t>(t);
}

/// To be called if log_type_quick_check returns false.
///
/// This function will:
/// Scan all shards of the log. If they exist, it will determine their
/// type and whether they have any entries.
///
/// If concordant, it will then write the mark into the omap of the
/// zero'th shard.
///
/// If no shards exist, a zero'th shard is created (`specified` if it is
/// `fifo` or `omap`, `bias` if it's `neither`) and marked.
///
/// If the backing is opposite that specified, `discord` is returned
/// as the status. Both `concord` and `discord` will be accompanied by
/// a bool, true if the log is non-empty.
///
/// If the status is `corruption`, then the value of the bool is undefined.
///
/// The third argument is the found log type, will always be `omap` or
/// `fifo` unless the status is `corruption`.
std::tuple<log_check, bool, log_type>
log_setup_backing(librados::IoCtx& ioctx,
		  log_type specified, //< Log type specified in configuration
		  log_type bias, //< Log type to use if `neither` and none
		                 //< exists. //< *Must not* be `neither`.
		  int shards, //< Total number of shards
		  /// A function taking a shard number and
		  /// returning an oid.
		  const fu2::unique_function<std::string(int) const>& get_oid,
		  optional_yield y);

/// Remove all log shards and associated parts of fifos.
///
/// Return < 0 if there were unforeseen errors looking up/removing things.
int log_remove(librados::IoCtx& ioctx,
	       int shards, //< Total number of shards
	       /// A function taking a shard number and
	       /// returning an oid.
	       const fu2::unique_function<std::string(int) const>& get_oid,
	       optional_yield y);

/// Combine the above in a useful way. Try a quick check, if that
/// fails go through the long setup. If that results in discord but
/// the log is empty, remove and re-setup.
///
/// Returns the available log type, or `neither` on error.
log_type log_acquire_backing(librados::IoCtx& ioctx,
			     int shards, //< Total number of shards
			     /// Log type specified in configuration
			     log_type specified,
			     /// Log type to use if `neither` and none
			     /// exists. *Must not* be `neither`.
			     log_type bias,
			     /// A function taking a shard number and
			     /// returning an oid.
			     const fu2::unique_function<
			       std::string(int) const>& get_oid,
			     optional_yield y);

#endif 
