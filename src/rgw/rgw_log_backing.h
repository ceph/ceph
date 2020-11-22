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

#include "cls_fifo_legacy.h"

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


class LazyFIFO {
  librados::IoCtx& ioctx;
  std::string oid;
  std::mutex m;
  std::unique_ptr<rgw::cls::fifo::FIFO> fifo;

  int lazy_init(optional_yield y) {
    std::unique_lock l(m);
    if (fifo) return 0;
    auto r = rgw::cls::fifo::FIFO::create(ioctx, oid, &fifo, y);
    if (r) {
      fifo.reset();
    }
    return r;
  }

public:

  LazyFIFO(librados::IoCtx& ioctx, std::string oid)
    : ioctx(ioctx), oid(std::move(oid)) {}

  int read_meta(optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    return fifo->read_meta(y);
  }

  int meta(rados::cls::fifo::info& info, optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    info = fifo->meta();
    return 0;
  }

  int get_part_layout_info(std::uint32_t& part_header_size,
			   std::uint32_t& part_entry_overhead,
			   optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    std::tie(part_header_size, part_entry_overhead)
      = fifo->get_part_layout_info();
    return 0;
  }

  int push(const ceph::buffer::list& bl,
	   optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    return fifo->push(bl, y);
  }

  int push(ceph::buffer::list& bl,
	   librados::AioCompletion* c,
	   optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    fifo->push(bl, c);
    return 0;
  }

  int push(const std::vector<ceph::buffer::list>& data_bufs,
	   optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    return fifo->push(data_bufs, y);
  }

  int push(const std::vector<ceph::buffer::list>& data_bufs,
	    librados::AioCompletion* c,
	    optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    fifo->push(data_bufs, c);
    return 0;
  }

  int list(int max_entries, std::optional<std::string_view> markstr,
	   std::vector<rgw::cls::fifo::list_entry>* out,
	   bool* more, optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    return fifo->list(max_entries, markstr, out, more, y);
  }

  int list(int max_entries, std::optional<std::string_view> markstr,
	   std::vector<rgw::cls::fifo::list_entry>* out, bool* more,
	   librados::AioCompletion* c, optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    fifo->list(max_entries, markstr, out, more, c);
    return 0;
  }

  int trim(std::string_view markstr, bool exclusive, optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    return fifo->trim(markstr, exclusive, y);
  }

  int trim(std::string_view markstr, bool exclusive, librados::AioCompletion* c,
	   optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    fifo->trim(markstr, exclusive, c);
    return 0;
  }

  int get_part_info(int64_t part_num, rados::cls::fifo::part_header* header,
		    optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    return fifo->get_part_info(part_num, header, y);
  }

  int get_part_info(int64_t part_num, rados::cls::fifo::part_header* header,
		    librados::AioCompletion* c, optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    fifo->get_part_info(part_num, header, c);
    return 0;
  }

  int get_head_info(fu2::unique_function<
		      void(int r, rados::cls::fifo::part_header&&)>&& f,
		    librados::AioCompletion* c,
		    optional_yield y) {
    auto r = lazy_init(y);
    if (r < 0) return r;
    fifo->get_head_info(std::move(f), c);
    return 0;
  }
};

#endif
