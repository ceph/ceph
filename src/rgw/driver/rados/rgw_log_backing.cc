// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/asio/awaitable.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/detail/errc.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include "include/neorados/RADOS.hpp"

#include "common/async/blocked_completion.h"
#include "common/async/async_call.h"

#include "neorados/cls/log.h"
#include "neorados/cls/version.h"
#include "neorados/cls/fifo.h"

#include "rgw_log_backing.h"
#include "rgw_common.h"

using namespace std::chrono_literals;

namespace async = ceph::async;
namespace buffer = ceph::buffer;

namespace version = neorados::cls::version;

static constexpr auto dout_subsys = ceph_subsys_rgw;

enum class shard_check { dne, omap, fifo, corrupt };
inline std::ostream& operator <<(std::ostream& m, const shard_check& t) {
  switch (t) {
  case shard_check::dne:
    return m << "shard_check::dne";
  case shard_check::omap:
    return m << "shard_check::omap";
  case shard_check::fifo:
    return m << "shard_check::fifo";
  case shard_check::corrupt:
    return m << "shard_check::corrupt";
  }

  return m << "shard_check::UNKNOWN=" << static_cast<uint32_t>(t);
}

namespace {
/// Return the shard type, and a bool to see whether it has entries.
asio::awaitable<shard_check>
probe_shard(const DoutPrefixProvider* dpp, neorados::RADOS& rados,
	    const neorados::Object& obj, const neorados::IOContext& loc,
	    bool& fifo_unsupported)
{
  ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
		     << " probing obj=" << obj << dendl;
  if (!fifo_unsupported) {
    sys::error_code ec;
    auto fifo = co_await fifo::FIFO::open(
      dpp, rados, obj, loc,
      asio::redirect_error(asio::use_awaitable, ec),
      std::nullopt, true);
    if (!ec) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": obj=" << obj << " is FIFO"
			 << dendl;
      co_return shard_check::fifo;
    } else if (ec == sys::errc::no_message_available) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": obj=" << obj << " is empty and therefore OMAP"
			 << dendl;
      co_return shard_check::omap;
    } else if (ec == sys::errc::no_such_file_or_directory) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": obj=" << obj << " does not exist"
			 << dendl;
      co_return shard_check::dne;
    } else if (ec == sys::errc::operation_not_permitted) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": FIFO is unsupported, marking."
			 << dendl;
      fifo_unsupported = true;
      co_return shard_check::omap;
    } else {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": error probing: " << ec.message()
			 << ", obj=" << obj << dendl;
      co_return shard_check::corrupt;
    }
  } else {
    // Since FIFO is unsupported, OMAP is the only alternative
    co_return shard_check::omap;
  }
}

asio::awaitable<log_type> handle_dne(const DoutPrefixProvider *dpp,
				     neorados::RADOS& rados,
				     const neorados::Object& obj,
				     const neorados::IOContext& loc,
				     log_type def,
				     bool fifo_unsupported)
{
  if (def == log_type::fifo) {
    if (fifo_unsupported) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " WARNING: FIFO set as default but not supported by OSD. "
		 << "Falling back to OMAP." << dendl;
      co_return log_type::omap;
    }
    try {
      auto fifo = co_await fifo::FIFO::create(dpp, rados, obj, loc,
					      asio::use_awaitable);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << " error creating FIFO: " << e.what()
			 << ", obj=" << obj << dendl;
      throw;
    }
  }
  co_return def;
}
}

asio::awaitable<log_type>
log_backing_type(const DoutPrefixProvider* dpp,
		 neorados::RADOS& rados,
                 const neorados::IOContext& loc,
		 log_type def,
		 int shards,
		 const fu2::unique_function<std::string(int) const>& get_oid)
{
  auto check = shard_check::dne;
  bool fifo_unsupported = false;
  for (int i = 0; i < shards; ++i) {
    auto c = co_await probe_shard(dpp, rados, neorados::Object(get_oid(i)), loc,
				  fifo_unsupported);
    if (c == shard_check::corrupt)
      throw sys::system_error(EIO, sys::generic_category());
    if (c == shard_check::dne) continue;
    if (check == shard_check::dne) {
      check = c;
      continue;
    }
    if (check != c) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << " clashing types: check=" << check
		 << ", c=" << c << dendl;
      throw sys::system_error(EIO, sys::generic_category());
    }
  }
  if (check == shard_check::corrupt) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << " should be unreachable!" << dendl;
    throw sys::system_error(EIO, sys::system_category());
  }

  if (check == shard_check::dne)
    co_return co_await handle_dne(dpp, rados, get_oid(0), loc, def,
				  fifo_unsupported);

  co_return (check == shard_check::fifo ? log_type::fifo : log_type::omap);
}

asio::awaitable<void> log_remove(
  const DoutPrefixProvider *dpp,
  neorados::RADOS& rados,
  const neorados::IOContext& loc,
  int shards,
  const fu2::unique_function<std::string(int) const>& get_oid,
  bool leave_zero)
{
  sys::error_code ec;
  for (int i = 0; i < shards; ++i) {
    auto oid = get_oid(i);
    auto [info, part_header_size, part_entry_overhead]
      = co_await fifo::FIFO::get_meta(rados, oid, loc, std::nullopt,
				      asio::redirect_error(asio::use_awaitable,
							   ec));
    if (ec == sys::errc::no_such_file_or_directory) continue;
    if (!ec && info.head_part_num > -1) {
      for (auto j = info.tail_part_num; j <= info.head_part_num; ++j) {
	sys::error_code subec;
	co_await rados.execute(info.part_oid(j),
			       loc, neorados::WriteOp{}.remove(),
			       asio::redirect_error(asio::use_awaitable, subec));
	if (subec && subec != sys::errc::no_such_file_or_directory) {
	  if (!ec) ec = subec;
	  ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			     << ": failed removing FIFO part " << j
			     << ": " << subec.message() << dendl;
	}
      }
    }
    if (ec && ec != sys::errc::no_message_available) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": failed checking FIFO part: oid=" << oid
			 << ": " << ec.message() << dendl;
    }
    neorados::WriteOp op;
    if (i == 0 && leave_zero) {
      // Leave shard 0 in existence, but remove contents and
      // omap. cls_lock stores things in the xattrs. And sync needs to
      // rendezvous with locks on generation 0 shard 0.
      op.set_omap_header({})
	.clear_omap()
	.truncate(0);
    } else {
      op.remove();
    }
    co_await rados.execute(oid, loc, std::move(op),
			   asio::redirect_error(asio::use_awaitable, ec));
    if (ec && ec != sys::errc::no_such_file_or_directory) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": failed removing shard: oid=" << oid
			 << ": " << ec.message() << dendl;
    }
  }
  if (ec)
    throw sys::error_code(ec);
  co_return;
}

logback_generations::~logback_generations() {
  if (watchcookie > 0) {
    auto cct = rados.cct();
    sys::error_code ec;
    rados.unwatch(watchcookie, loc, async::use_blocked[ec]);
    if (ec) {
      lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
		 << ": failed unwatching oid=" << oid
		 << ", " << ec.message() << dendl;
    }
  }
}

asio::awaitable<void> logback_generations::setup(const DoutPrefixProvider *dpp,
						 log_type def)
{
  bool must_create = false;
  try {
    // First, read.
    auto [es, v] = co_await read(dpp);
    co_await async::async_dispatch(
      strand,
      [&] {
	entries = std::move(es);
	version = std::move(v);
      }, asio::use_awaitable);
  } catch (const sys::system_error& e) {
    if (e.code() != sys::errc::no_such_file_or_directory) {
      throw;
    }
    // No co_awaiting in a try block.
    must_create = true;
  }
  if (must_create) {
    // Are we the first? Then create generation 0 and the generations
    // metadata.
    auto type = co_await log_backing_type(dpp, rados, loc, def, shards,
					  [this](int shard) {
					    return this->get_oid(0, shard);
					  });
    auto op = co_await async::async_dispatch(
      strand,
      [this, type] {
	neorados::WriteOp op;
	logback_generation l;
	l.type = type;
	version.ver = 1;
	static constexpr auto TAG_LEN = 24;
	version.tag.clear();
	append_rand_alpha(rados.cct(), version.tag, version.tag, TAG_LEN);
	buffer::list bl;
	entries.emplace(0, std::move(l));
	encode(entries, bl);
	op.create(true)
	  .exec(version::set(version))
	  .write_full(std::move(bl));
	return op;
      }, asio::use_awaitable);

    sys::error_code ec;
    co_await rados.execute(oid, loc, std::move(op),
			   asio::redirect_error(asio::use_awaitable, ec));
    if (ec && ec != sys::errc::file_exists) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": failed writing oid=" << oid
			 << ", " << ec.message() << dendl;
      throw sys::system_error(ec);
    }
    // Did someone race us? Then re-read.
    if (ec == sys::errc::file_exists) {
      auto [es, v] = co_await read(dpp);
      if (es.empty()) {
	throw sys::system_error{
	  EIO, sys::generic_category(),
	  "Generations metadata object exists, but empty."};
      }
      auto l = es.begin()->second;
      // In the unlikely event that someone raced us, created
      // generation zero, incremented, then erased generation zero,
      // don't leave generation zero lying around.
      if (l.gen_id != 0) {
	co_await log_remove(dpp, rados, loc, shards,
			    [this](int shard) {
			      return this->get_oid(0, shard);
			    }, true);
      }
      co_await async::async_dispatch(
	strand,
	[&] {
	  entries = std::move(es);
	  version = std::move(v);
	}, asio::use_awaitable);
    }
  }

  // Pass all non-empty generations to the handler
  auto e = co_await async::async_dispatch(
    strand,
    [&] {
      auto i = lowest_nomempty(entries);
      entries_t e;
      std::copy(i, entries.cend(),
		std::inserter(e, e.end()));
      return e;
    }, asio::use_awaitable);
  try {
    co_await watch();
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << ": failed to re-establish watch, unsafe to continue: oid="
		       << oid << ", " << e.what() << dendl;
    throw;
  }
  handle_init(std::move(e));
  co_return;
}

asio::awaitable<void> logback_generations::update(const DoutPrefixProvider *dpp)
{
  auto [es, v] = co_await read(dpp);
  auto [do_nothing, highest_empty, new_entries] =
    co_await async::async_dispatch(
      strand,
      [&]() -> std::tuple<bool, std::optional<std::uint64_t>, entries_t> {
	if (v == version) {
	  // Nothing to do!
	  return {true, {}, {}};
	}
	// Check consistency and prepare update
	if (es.empty()) {
	  throw sys::system_error{
	    EFAULT, sys::generic_category(),
	    "logback_generations::update: INCONSISTENCY! Read empty update."};
	}
	auto cur_lowest = lowest_nomempty(entries);
	// Straight up can't happen
	assert(cur_lowest != entries.cend());
	auto new_lowest = lowest_nomempty(es);
	if (new_lowest == es.cend()) {
	  throw sys::system_error{
	    EFAULT, sys::generic_category(),
	    "logback_generations::update: INCONSISTENCY! Read update with no "
	    "active head!"};
	}
	if (new_lowest->first < cur_lowest->first) {
	  throw sys::system_error{
	    EFAULT, sys::generic_category(),
	    "logback_generations::update: INCONSISTENCY! Tail moved wrong way."};
	}
	std::optional<uint64_t> highest_empty;
	if (new_lowest->first > cur_lowest->first && new_lowest != es.begin()) {
	  --new_lowest;
	  highest_empty = new_lowest->first;
	}
	entries_t new_entries;

	if ((es.end() - 1)->first < (entries.end() - 1)->first) {
	  throw sys::system_error{
	    EFAULT, sys::generic_category(),
	    "logback_generations::update: INCONSISTENCY! Head moved wrong way."};
	}
	if ((es.end() - 1)->first > (entries.end() - 1)->first) {
	  auto ei = es.lower_bound((entries.end() - 1)->first + 1);
	  std::copy(ei, es.end(), std::inserter(new_entries, new_entries.end()));
	}
	// Everything checks out!
	version = std::move(v);
	entries = std::move(es);
	return {false, highest_empty, new_entries};
      }, asio::use_awaitable);

  if (do_nothing) {
    co_return;
  }
  if (highest_empty) {
    handle_empty_to(*highest_empty);
  }

  if (!new_entries.empty()) {
    handle_new_gens(std::move(new_entries));
  }
  co_return;
}

auto logback_generations::read(const DoutPrefixProvider *dpp)
  -> asio::awaitable<std::pair<entries_t, obj_version>>
{
  neorados::ReadOp op;
  op.exec(version::check(co_await async::async_dispatch(
			   strand,
			   [this] {
			     return version;
			   }, asio::use_awaitable),
			 VER_COND_GE));
  obj_version v2;
  op.exec(version::read(&v2));
  buffer::list bl;
  op.read(0, 0, &bl, nullptr);
  sys::error_code ec;
  co_await rados.execute(oid, loc, std::move(op), nullptr,
			 asio::redirect_error(asio::use_awaitable, ec));
  if (ec) {
    if (ec == sys::errc::no_such_file_or_directory) {
      ldpp_dout(dpp, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
			<< ": oid=" << oid
			<< " not found" << dendl;
    } else {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": failed reading oid=" << oid
			 << ", " << ec.message() << dendl;
    }
    throw sys::system_error(ec);
  }
  auto bi = bl.cbegin();
  entries_t e;
  decode(e, bi);
  co_return std::pair{std::move(e), std::move(v2)};
}

// This *must* be executed in logback_generations::strand
//
// The return value must be used. If true, run update and retry.
asio::awaitable<bool> logback_generations::write(const DoutPrefixProvider *dpp,
						 entries_t&& e)
{
  ceph_assert(co_await asio::this_coro::executor == strand);
  bool canceled = false;
  buffer::list bl;
  encode(e, bl);
  neorados::WriteOp op;
  op.exec(version::check(version, VER_COND_GE))
    .write_full(std::move(bl))
    .exec(version::inc());
  sys::error_code ec;
  co_await rados.execute(oid, loc, std::move(op),
			 asio::redirect_error(asio::use_awaitable, ec));
  if (!ec) {
    entries = std::move(e);
    version.inc();
  } else if (ec == sys::errc::operation_canceled) {
    canceled = true;
  } else {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << ": failed reading oid=" << oid
		       << ", " << ec.message() << dendl;
    throw sys::system_error(ec);
  }
  co_return canceled;
}


asio::awaitable<void> logback_generations::watch()
{
  watchcookie = co_await rados.watch(oid, loc, std::nullopt, std::ref(*this),
				     asio::use_awaitable);
  co_return;
}

asio::awaitable<void>
logback_generations::new_backing(const DoutPrefixProvider* dpp, log_type type) {
  static constexpr auto max_tries = 10;
  auto tries = 0;
  entries_t new_entries;
  bool canceled = false;
  do {
    co_await update(dpp);
    canceled =
      co_await asio::co_spawn(
	strand,
	[](logback_generations& l, log_type type, entries_t& new_entries,
	   const DoutPrefixProvider* dpp)
	-> asio::awaitable<bool> {
	  auto last = l.entries.end() - 1;
	  if (last->second.type == type) {
	    // Nothing to be done
	    co_return false;
	  }
	  auto newgenid = last->first + 1;
	  logback_generation newgen;
	  newgen.gen_id = newgenid;
	  newgen.type = type;
	  new_entries.emplace(newgenid, newgen);
	  auto es = l.entries;
	  es.emplace(newgenid, std::move(newgen));
	  co_return co_await l.write(dpp, std::move(es));
	}(*this, type, new_entries, dpp),
	asio::use_awaitable);
    ++tries;
  } while (canceled && tries < max_tries);
  if (tries >= max_tries) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << ": exhausted retry attempts." << dendl;
    throw sys::system_error(ECANCELED, sys::generic_category(),
			    "Exhausted retry attempts");
  }

  co_await rados.notify(oid, loc, {}, 10s, asio::use_awaitable);
  handle_new_gens(new_entries);
  co_return;
}

asio::awaitable<void>
logback_generations::empty_to(const DoutPrefixProvider* dpp,
			      uint64_t gen_id) {
  static constexpr auto max_tries = 10;
  auto tries = 0;
  uint64_t newtail = 0;
  bool canceled = false;
  do {
    co_await update(dpp);
    canceled =
      co_await asio::co_spawn(
	strand,
	[](logback_generations& l, uint64_t gen_id,
	   uint64_t& newtail, const DoutPrefixProvider* dpp)
	-> asio::awaitable<bool> {
	  {
	    auto last = l.entries.end() - 1;
	    if (gen_id >= last->first) {
	      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
				 << ": Attempt to trim beyond the possible."
				 << dendl;
	      throw sys::system_error{EINVAL, sys::system_category()};
	    }
	  }
	  auto es = l.entries;
	  auto ei = es.upper_bound(gen_id);
	  if (ei == es.begin()) {
	    // Nothing to be done.
	    co_return false;
	  }
	  for (auto i = es.begin(); i < ei; ++i) {
	    newtail = i->first;
	    i->second.pruned = ceph::real_clock::now();
	  }
	  co_return co_await l.write(dpp, std::move(es));
	}(*this, gen_id, newtail, dpp),
	asio::use_awaitable);
    if (canceled) {
      co_await update(dpp);
    }
    ++tries;
  } while (canceled && tries < max_tries);
  if (tries >= max_tries) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << ": exhausted retry attempts." << dendl;
    throw sys::system_error(ECANCELED, sys::generic_category(),
			    "Exhausted retry attempts");
  }
  co_await rados.notify(oid, loc, {}, 10s, asio::use_awaitable);
  handle_empty_to(newtail);
  co_return;
}

asio::awaitable<void>
logback_generations::remove_empty(const DoutPrefixProvider* dpp) {
  static constexpr auto max_tries = 10;
  co_await update(dpp);
  auto tries = 0;
  entries_t new_entries;
  bool nothing_to_do =
    co_await async::async_dispatch(
      strand,
      [this] {
	ceph_assert(!entries.empty());
	auto i = lowest_nomempty(entries);
	return i == entries.begin();
      }, asio::use_awaitable);
  if (nothing_to_do)
    co_return;
  entries_t es;
  auto now = ceph::real_clock::now();
  bool canceled = false;
  do {
    entries_t es2;
    co_await async::async_dispatch(
      strand,
      [this, now, &es, &es2] {
	std::copy_if(entries.cbegin(), entries.cend(),
		     std::inserter(es, es.end()),
		     [now](const auto& e) {
		       if (!e.second.pruned)
			 return false;
		       auto pruned = *e.second.pruned;
		       return (now - pruned) >= 1h;
		     });
	es2 = entries;
      }, asio::use_awaitable);
    for (const auto& [gen_id, e] : es) {
      ceph_assert(e.pruned);
      co_await log_remove(dpp, rados, loc, shards,
			  [this, gen_id = gen_id](int shard) {
			    return this->get_oid(gen_id, shard);
			  }, (gen_id == 0));
      if (auto i = es2.find(gen_id); i != es2.end()) {
	es2.erase(i);
      }
    }
    es.clear();
    canceled = co_await co_spawn(
      strand,
      write(dpp, std::move(es2)),
      asio::use_awaitable);
    if (canceled) {
      co_await update(dpp);
    }
    ++tries;
  } while (canceled && tries < max_tries);
  if (tries >= max_tries) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		       << ": exhausted retry attempts." << dendl;
    throw sys::system_error(ECANCELED, sys::generic_category(),
			    "Exhausted retry attempts");
  }
  co_await rados.notify(oid, loc, {}, 10s, asio::use_awaitable);
}

void logback_generations::operator ()(sys::error_code ec,
				      uint64_t notify_id,
				      uint64_t cookie,
				      uint64_t notifier_id,
				      bufferlist&& bl) {
  co_spawn(rados.get_executor(),
	   handle_notify(ec, notify_id, cookie, notifier_id, std::move(bl)),
	   asio::detached);
}

asio::awaitable<void>
logback_generations::handle_notify(sys::error_code ec,
				   uint64_t notify_id,
				   uint64_t cookie,
				   uint64_t notifier_id,
				   bufferlist&& bl) {
  const DoutPrefix dp(rados.cct(), dout_subsys,
		      "logback generations handle_notify: ");
  if (!ec) {
    if (notifier_id != my_id) {
      try {
	co_await update(&dp);
      } catch (const std::exception& e) {
	ldpp_dout(&dp, -1)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": update failed (" << e.what()
	  << "), no one to report to and no safe way to continue."
	  << dendl;
	std::terminate();
      }
    }
    co_await rados.notify_ack(oid, loc, notify_id, watchcookie, {},
			      asio::use_awaitable);
  } else {
    ec.clear();
    co_await rados.unwatch(watchcookie, loc,
			   asio::redirect_error(asio::use_awaitable, ec));
    if (ec) {
      ldpp_dout(&dp, -1) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << ": failed to set unwatch oid=" << oid
			 << ", " << ec.message() << dendl;
    }

    try {
      co_await watch();
    } catch (const std::exception& e) {
      ldpp_dout(&dp, -1)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": failed to re-establish watch, unsafe to continue: oid="
	<< oid << ": " << e.what() << dendl;
      std::terminate();
    }
  }
}
