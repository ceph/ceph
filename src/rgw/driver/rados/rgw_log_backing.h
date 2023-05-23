// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <optional>
#include <iostream>
#include <string>
#include <string_view>

#include <boost/asio/strand.hpp>

#include <boost/container/flat_map.hpp>
#include <boost/system/error_code.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"
#include "include/encoding.h"
#include "include/function2.hpp"

#include "cls/version/cls_version_types.h"

#include "common/Formatter.h"
#include "common/strtol.h"

#include "neorados/cls/fifo.h"

namespace container = boost::container;
namespace sys = boost::system;
namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;

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
  type = static_cast<log_type>(t);
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
asio::awaitable<log_type>
log_backing_type(const DoutPrefixProvider* dpp,
		 neorados::RADOS& rados,
                 const neorados::IOContext& loc,
		 log_type def,
		 int shards,
		 const fu2::unique_function<std::string(int) const>& get_oid);

/// Remove all log shards and associated parts of fifos.
asio::awaitable<void> log_remove(
  const DoutPrefixProvider *dpp,
  neorados::RADOS& rados,
  const neorados::IOContext& loc,
  int shards,
  const fu2::unique_function<std::string(int) const>& get_oid,
  bool leave_zero);

struct logback_generation {
  uint64_t gen_id = 0;
  log_type type;
  std::optional<ceph::real_time> pruned;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(gen_id, bl);
    encode(type, bl);
    encode(pruned, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(gen_id, bl);
    decode(type, bl);
    decode(pruned, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(logback_generation)
inline std::ostream& operator <<(std::ostream& m, const logback_generation& g) {
  return m << "[" << g.gen_id << "," << g.type << ","
	   << (g.pruned ? "PRUNED" : "NOT PRUNED") << "]";
}

class logback_generations {
public:
  using entries_t = container::flat_map<uint64_t, logback_generation>;

protected:
  neorados::RADOS& rados;
  neorados::IOContext loc;
  logback_generations(
    neorados::RADOS& rados,
    neorados::Object oid,
    neorados::IOContext loc,
    fu2::unique_function<std::string(uint64_t, int) const> get_oid,
    int shards) noexcept
    : rados(rados), loc(std::move(loc)), oid(oid), get_oid(std::move(get_oid)),
      shards(shards) {}

  uint64_t my_id = rados.instance_id();

private:
  const std::string oid;
  const fu2::unique_function<std::string(uint64_t, int) const> get_oid;

protected:
  const int shards;

private:

  uint64_t watchcookie = 0;

  obj_version version;
  asio::strand<neorados::RADOS::executor_type> strand{
    asio::make_strand(rados.get_executor())};
  entries_t entries;

  asio::awaitable<std::pair<entries_t, obj_version>>
  read(const DoutPrefixProvider *dpp);
  asio::awaitable<bool> write(const DoutPrefixProvider *dpp, entries_t&& e);
  asio::awaitable<void> setup(const DoutPrefixProvider *dpp, log_type def);
  asio::awaitable<void> watch();

  auto lowest_nomempty(const entries_t& es) {
    return std::find_if(es.begin(), es.end(),
			[](const auto& e) {
			  return !e.second.pruned;
			});
  }

public:

  /// For the use of watch/notify.
  void operator ()(sys::error_code ec,
		   uint64_t notify_id,
		   uint64_t cookie,
		   uint64_t notifier_id,
		   bufferlist&& bl);
  asio::awaitable<void> handle_notify(sys::error_code ec,
				      uint64_t notify_id,
				      uint64_t cookie,
				      uint64_t notifier_id,
				      bufferlist&& bl);

  /// Public interface
  virtual ~logback_generations();

  template<typename T, typename... Args>
  static asio::awaitable<std::unique_ptr<T>> init(
    const DoutPrefixProvider *dpp,
    neorados::RADOS& r_,
    const neorados::Object& oid_,
    const neorados::IOContext& loc_,
    fu2::unique_function<std::string(uint64_t, int) const>&& get_oid_,
    int shards_, log_type def,
    Args&& ...args) {
    std::unique_ptr<T> lg{new T(r_, oid_, loc_,
				std::move(get_oid_),
				shards_, std::forward<Args>(args)...)};
    co_await lg->setup(dpp, def);
    co_return lg;
  }

  asio::awaitable<void> update(const DoutPrefixProvider *dpp);

  entries_t get_entries() const {
    return entries;
  }

  asio::awaitable<void> new_backing(const DoutPrefixProvider *dpp,
				    log_type type);

  asio::awaitable<void> empty_to(const DoutPrefixProvider *dpp, uint64_t gen_id);

  asio::awaitable<void> remove_empty(const DoutPrefixProvider *dpp);

  // Callbacks, to be defined by descendant.

  /// Handle initialization on startup
  ///
  /// @param e All non-empty generations
  virtual void handle_init(entries_t e) = 0;

  /// Handle new generations.
  ///
  /// @param e Map of generations added since last update
  virtual void handle_new_gens(entries_t e) = 0;

  /// Handle generations being marked empty
  ///
  /// @param new_tail Lowest non-empty generation
  virtual void handle_empty_to(uint64_t new_tail) = 0;
};

inline std::string gencursor(uint64_t gen_id, std::string_view cursor) {
  return (gen_id > 0 ?
	  fmt::format("G{:0>20}@{}", gen_id, cursor) :
	  std::string(cursor));
}

inline std::pair<uint64_t, std::string>
cursorgen(std::optional<std::string> cursor_) {
  if (!cursor_ || cursor_->empty()) {
    return { 0, "" };
  }
  std::string_view cursor = *cursor_;
  if (cursor[0] != 'G') {
    return { 0, std::string{cursor} };
  }
  cursor.remove_prefix(1);
  auto gen_id = ceph::consume<uint64_t>(cursor);
  if (!gen_id || cursor[0] != '@') {
    return { 0, *cursor_ };
  }
  cursor.remove_prefix(1);
  return { *gen_id, std::string{cursor} };
}

class LazyFIFO {
  neorados::RADOS& r;
  const std::string oid;
  const neorados::IOContext loc;
  std::mutex m;
  std::unique_ptr<fifo::FIFO> fifo;

  asio::awaitable<void> lazy_init(const DoutPrefixProvider *dpp) {
    std::unique_lock l(m);
    if (fifo) {
      co_return;
    } else {
      l.unlock();
      // FIFO supports multiple clients by design, so it's safe to
      // race to create them.
      auto fifo_tmp = co_await fifo::FIFO::create(dpp, r, oid, loc,
						  asio::use_awaitable);
      l.lock();
      if (!fifo) {
	// We won the race
	fifo = std::move(fifo_tmp);
      }
    }
    l.unlock();
    co_return;
  }

public:

  LazyFIFO(neorados::RADOS& r,  std::string oid, neorados::IOContext loc)
    : r(r), oid(std::move(oid)), loc(std::move(loc)) {}

  template <typename... Args>
  asio::awaitable<void> push(const DoutPrefixProvider *dpp, Args&& ...args) {
    co_await lazy_init(dpp);
    co_return co_await fifo->push(dpp, std::forward<Args>(args)...,
				  asio::use_awaitable);
  }

  asio::awaitable<std::tuple<std::span<fifo::entry>, std::optional<std::string>>>
  list(const DoutPrefixProvider *dpp, std::optional<std::string> markstr,
       std::span<fifo::entry> entries) {
    co_await lazy_init(dpp);
    co_return co_await fifo->list(dpp, markstr, entries, asio::use_awaitable);
  }

  asio::awaitable<void> trim(const DoutPrefixProvider *dpp,
			     std::string markstr, bool exclusive) {
    co_await lazy_init(dpp);
    co_return co_await fifo->trim(dpp, markstr, exclusive, asio::use_awaitable);
  }

  asio::awaitable<std::tuple<std::string, ceph::real_time>>
  last_entry_info(const DoutPrefixProvider *dpp) {
    co_await lazy_init(dpp);
    co_return co_await fifo->last_entry_info(dpp, asio::use_awaitable);
  }
};
