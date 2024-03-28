// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <exception>
#include <ranges>
#include <utility>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/system/detail/errc.hpp>

#include "include/fs_types.h"
#include "include/neorados/RADOS.hpp"

#include "common/async/async_yield.h"
#include "common/async/blocked_completion.h"
#include "common/async/coro_aiocomplete.h"

#include "common/async/yield_context.h"
#include "common/debug.h"
#include "common/containers.h"
#include "common/errno.h"
#include "common/error_code.h"

#include "neorados/cls/fifo.h"
#include "neorados/cls/log.h"

#include "rgw_bucket_layout.h"
#include "rgw_datalog.h"
#include "rgw_log_backing.h"
#include "rgw_tools.h"
#include "rgw_sal_rados.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

using namespace std::literals;

namespace sys = boost::system;

namespace nlog = ::neorados::cls::log;
namespace fifo = ::neorados::cls::fifo;

namespace async = ceph::async;
namespace buffer = ceph::buffer;

using ceph::containers::tiny_vector;

inline int eptr_to_int(const DoutPrefixProvider* dpp,
		       std::exception_ptr eptr,
		       std::string_view caller)
{
  if (!eptr) {
    return 0;
  } else try {
      std::rethrow_exception(eptr);
    } catch (const sys::system_error& e) {
      if (dpp) {
	ldpp_dout(dpp, 1) << caller << ": Got system_error: " << e.what()
			  << ", translating to "
			  << ceph::from_error_code(e.code()) << dendl;
      }
      return ceph::from_error_code(e.code());
    } catch (const std::exception& e) {
      if (dpp) {
	ldpp_dout(dpp, 1) << caller << ": Got exception: " << e.what()
			  << ", translating to " << -EFAULT
			  << dendl;
      }
      return -EFAULT;
    }
}

void rgw_data_change::dump(ceph::Formatter *f) const
{
  std::string type;
  switch (entity_type) {
    case ENTITY_TYPE_BUCKET:
      type = "bucket";
      break;
    default:
      type = "unknown";
  }
  encode_json("entity_type", type, f);
  encode_json("key", key, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
  encode_json("gen", gen, f);
}

void rgw_data_change::decode_json(JSONObj *obj) {
  std::string s;
  JSONDecoder::decode_json("entity_type", s, obj);
  if (s == "bucket") {
    entity_type = ENTITY_TYPE_BUCKET;
  } else {
    entity_type = ENTITY_TYPE_UNKNOWN;
  }
  JSONDecoder::decode_json("key", key, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  JSONDecoder::decode_json("gen", gen, obj);
}

void rgw_data_change::generate_test_instances(std::list<rgw_data_change *>& l) {
  l.push_back(new rgw_data_change{});
  l.push_back(new rgw_data_change);
  l.back()->entity_type = ENTITY_TYPE_BUCKET;
  l.back()->key = "bucket_name";
  l.back()->timestamp = ceph::real_clock::zero();
  l.back()->gen = 0;
}

void rgw_data_change_log_entry::dump(Formatter *f) const
{
  encode_json("log_id", log_id, f);
  utime_t ut(log_timestamp);
  encode_json("log_timestamp", ut, f);
  encode_json("entry", entry, f);
}

void rgw_data_change_log_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("log_id", log_id, obj);
  utime_t ut;
  JSONDecoder::decode_json("log_timestamp", ut, obj);
  log_timestamp = ut.to_real_time();
  JSONDecoder::decode_json("entry", entry, obj);
}

void rgw_data_notify_entry::dump(Formatter *f) const
{
  encode_json("key", key, f);
  encode_json("gen", gen, f);
}

void rgw_data_notify_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("gen", gen, obj);
}

class RGWDataChangesOmap final : public RGWDataChangesBE {
  using centries = std::vector<cls::log::entry>;
  std::vector<std::string> oids;

public:
  RGWDataChangesOmap(neorados::RADOS& r,
		     neorados::IOContext loc,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     int num_shards)
    : RGWDataChangesBE(r, std::move(loc), datalog, gen_id) {
    oids.reserve(num_shards);
    for (auto i = 0; i < num_shards; ++i) {
      oids.push_back(get_oid(i));
    }
  }
  ~RGWDataChangesOmap() override = default;

  void prepare(ceph::real_time ut, const std::string& key,
	       buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](const auto& v) { return std::empty(v); }, out));
      out = centries();
    }

    cls::log::entry e{ut, {}, key, std::move(entry)};
    std::get<centries>(out).push_back(std::move(e));
  }
  asio::awaitable<void> push(const DoutPrefixProvider *dpp, int index,
			     entries&& items) override {
    co_await r.execute(
      oids[index], loc,
      neorados::WriteOp{}.exec(nlog::add(std::get<centries>(items))),
      asio::use_awaitable);
    co_return;
  }
  asio::awaitable<void> push(const DoutPrefixProvider *dpp, int index,
			     ceph::real_time now, const std::string& key,
			     buffer::list&& bl) override {
    co_await r.execute(
      oids[index], loc,
      neorados::WriteOp{}.exec(nlog::add(now, {}, key, std::move(bl))),
      asio::use_awaitable);
    co_return;
  }

  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::optional<std::string>>>
  list(const DoutPrefixProvider* dpp, int shard,
       std::span<rgw_data_change_log_entry> entries,
       std::optional<std::string> marker) override {
    std::vector<cls::log::entry> entrystore{entries.size()};

    try {
      auto [lentries, lmark] =
	co_await nlog::list(r, oids[shard], loc, {}, {}, marker, entrystore,
			    asio::use_awaitable);

      entries = entries.first(lentries.size());
      std::ranges::transform(lentries, std::begin(entries),
			     [](const auto& e) {
			       rgw_data_change_log_entry entry;
			       entry.log_id = e.id;
			       entry.log_timestamp = e.timestamp;
			       auto liter = e.data.cbegin();
			       decode(entry.entry, liter);
			       return entry;
			     });
      co_return std::make_tuple(std::move(entries), lmark);
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to decode data changes log entry: "
			 << err.what() << dendl;
      throw;
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	co_return std::make_tuple(entries.first(0), std::nullopt);
      } else {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			   << ": failed to list " << oids[shard]
			   << ": " << e.what() << dendl;
	throw;
      }
    }
  }
  asio::awaitable<RGWDataChangesLogInfo> get_info(const DoutPrefixProvider *dpp,
						  int index) override {
    try {
      auto header = co_await nlog::info(r, oids[index], loc,
					asio::use_awaitable);
      co_return RGWDataChangesLogInfo{.marker = header.max_marker,
				      .last_update = header.max_time};
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	co_return RGWDataChangesLogInfo{};
      }
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to get info from " << oids[index]
			 << ": " << e.what() << dendl;
      throw;
    }
  }
  asio::awaitable<void> trim(const DoutPrefixProvider *dpp, int index,
			     std::string_view marker) override {
    try {
      co_await nlog::trim(r, oids[index], loc, {}, std::string{marker},
			  asio::use_awaitable);
      co_return;
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	co_return;
      } else {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			   << ": failed to get trim " << oids[index]
			   << ": " << e.what() << dendl;
	throw;
      }
    }
  }
  std::string_view max_marker() const override {
    return "99999999";
  }
  asio::awaitable<bool> is_empty(const DoutPrefixProvider* dpp) override {
    std::vector<cls::log::entry> entrystore{1};
    for (auto oid = 0; oid < std::ssize(oids); ++oid) {
      try {
	auto [entries, marker] =
	  co_await nlog::list(r, oids[oid], loc, {}, {}, {}, entrystore,
			      asio::use_awaitable);
	if (!entries.empty()) {
	  co_return false;
	}
      } catch (const sys::system_error& e) {
	if (e.code() == sys::errc::no_such_file_or_directory) {
	  continue;
	}
      }
    }
    co_return true;
  }
};

class RGWDataChangesFIFO final : public RGWDataChangesBE {
  using centries = std::vector<buffer::list>;
  tiny_vector<LazyFIFO> fifos;

public:
  RGWDataChangesFIFO(neorados::RADOS& r,
		     neorados::IOContext loc,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     int num_shards)
    : RGWDataChangesBE(r, std::move(loc), datalog, gen_id),
      fifos(num_shards, [&r, &loc, this](std::size_t i, auto emplacer) {
	emplacer.emplace(r, get_oid(i), loc);
      }) {}
  ~RGWDataChangesFIFO() override = default;
  void prepare(ceph::real_time, const std::string&,
	       buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](auto& v) { return std::empty(v); }, out));
      out = centries();
    }
    std::get<centries>(out).push_back(std::move(entry));
  }
  asio::awaitable<void> push(const DoutPrefixProvider* dpp, int index,
			     entries&& items) override {
    co_return co_await fifos[index].push(dpp, std::get<centries>(items));
  }
  asio::awaitable<void> push(const DoutPrefixProvider* dpp, int index,
			     ceph::real_time, const std::string&,
			     buffer::list&& bl) override {
    co_return co_await fifos[index].push(dpp, std::move(bl));
  }
  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::optional<std::string>>>
  list(const DoutPrefixProvider* dpp, int shard,
       std::span<rgw_data_change_log_entry> entries,
       std::optional<std::string> marker) override {
    try {
      std::vector<fifo::entry> log_entries{entries.size()};
      auto [lentries, outmark] =
	co_await fifos[shard].list(dpp, marker, log_entries);
      entries = entries.first(lentries.size());
      std::ranges::transform(lentries, entries.begin(),
			     [](const auto& e) {
			       rgw_data_change_log_entry entry ;
			       entry.log_id = e.marker;
			       entry.log_timestamp = e.mtime;
			       auto liter = e.data.cbegin();
			       decode(entry.entry, liter);
			       return entry;
			     });
      co_return  std::make_tuple(std::move(entries), std::move(outmark));
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to decode data changes log entry: "
			 << err.what() << dendl;
      throw;
    }
  }
  asio::awaitable<RGWDataChangesLogInfo>
  get_info(const DoutPrefixProvider *dpp, int index) override {
    auto& fifo = fifos[index];
    auto [marker, last_update] = co_await fifo.last_entry_info(dpp);
    co_return RGWDataChangesLogInfo{ .marker = marker,
				     .last_update = last_update };
  }
  asio::awaitable<void> trim(const DoutPrefixProvider *dpp, int index,
	   std::string_view marker) override {
    co_await fifos[index].trim(dpp, std::string{marker}, false);
  }
  std::string_view max_marker() const override {
    static const auto max_mark = fifo::FIFO::max_marker();
    return std::string_view(max_mark);
  }
  asio::awaitable<bool> is_empty(const DoutPrefixProvider *dpp) override {
    std::vector<fifo::entry> entrystore;
    for (auto shard = 0u; shard < fifos.size(); ++shard) {
      auto [lentries, outmark] =
	co_await fifos[shard].list(dpp, {}, entrystore);
      if (!lentries.empty()) {
	co_return false;
      }
    }
    co_return true;
  }
};

RGWDataChangesLog::RGWDataChangesLog(CephContext* cct)
  : cct(cct),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    prefix(get_prefix()),
    changes(cct->_conf->rgw_data_log_changes_size) {}

void DataLogBackends::handle_init(entries_t e) {
  std::unique_lock l(m);
  for (const auto& [gen_id, gen] : e) {
    if (gen.pruned) {
      lderr(datalog.cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": ERROR: given empty generation: gen_id=" << gen_id << dendl;
    }
    if (count(gen_id) != 0) {
      lderr(datalog.cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": ERROR: generation already exists: gen_id=" << gen_id << dendl;
    }
    try {
      switch (gen.type) {
      case log_type::omap:
	emplace(gen_id,
		boost::intrusive_ptr<RGWDataChangesBE>(
		  new RGWDataChangesOmap(rados, loc, datalog, gen_id, shards)));
	break;
      case log_type::fifo:
	emplace(gen_id,
		boost::intrusive_ptr<RGWDataChangesBE>(
		  new RGWDataChangesFIFO(rados, loc, datalog, gen_id, shards)));
	break;
      default:
	lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": IMPOSSIBLE: invalid log type: gen_id=" << gen_id
	  << ", type" << gen.type << dendl;
	throw sys::system_error{EFAULT, sys::generic_category()};
      }
    } catch (const sys::system_error& err) {
      lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": error setting up backend: gen_id=" << gen_id
	  << ", err=" << err.what() << dendl;
      throw;
    }
  }
}

void DataLogBackends::handle_new_gens(entries_t e) {
  handle_init(std::move(e));
}

void DataLogBackends::handle_empty_to(uint64_t new_tail) {
  std::unique_lock l(m);
  auto i = cbegin();
  if (i->first < new_tail) {
    return;
  }
  if (new_tail >= (cend() - 1)->first) {
    lderr(datalog.cct)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << ": ERROR: attempt to trim head: new_tail=" << new_tail << dendl;
    throw sys::system_error(EFAULT, sys::system_category());
  }
  erase(i, upper_bound(new_tail));
}


int RGWDataChangesLog::start(const DoutPrefixProvider *dpp, const RGWZone* _zone,
			     const RGWZoneParams& zoneparams,
			     rgw::sal::RadosStore* _store)
{
  zone = _zone;
  store = _store;
  ceph_assert(zone);
  auto defbacking = to_log_type(
    cct->_conf.get_val<std::string>("rgw_default_data_log_backing"));
  // Should be guaranteed by `set_enum_allowed`
  ceph_assert(defbacking);
  auto log_pool = zoneparams.log_pool;

  try {
    std::exception_ptr eptr;
    std::tie(eptr, loc) =
      asio::co_spawn(store->get_io_context(),
		     rgw::neorados::init_iocontext(dpp, store->get_neorados(),
						   log_pool,
						   rgw::neorados::create),
		     async::use_blocked);
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  } catch (const sys::system_error& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to initialized ioctx: " << e.what()
		       << ", pool=" << log_pool << dendl;
    return ceph::from_error_code(e.code());
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to initialized ioctx: " << e.what()
		       << ", pool=" << log_pool << dendl;
    return -EIO;
  }

  // Blocking in startup code, not ideal, but won't hurt anything.
  try {
    std::exception_ptr eptr;
    std::tie(eptr, bes) =
      asio::co_spawn(
	store->get_io_context().get_executor(),
	logback_generations::init<DataLogBackends>(
	  dpp, store->get_neorados(), metadata_log_oid(), loc,
	  [this](uint64_t gen_id, int shard) {
	    return get_oid(gen_id, shard);
	  }, num_shards, *defbacking, *this),
	async::use_blocked);
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  } catch (const sys::system_error& e) {
    lderr(cct) << __PRETTY_FUNCTION__
	       << ": Error initializing backends: "
	       << e.what() << dendl;
    return ceph::from_error_code(e.code());
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Error initializing backends: " << e.what()
		       << dendl;
    return -EIO;
  }

  asio::co_spawn(store->get_io_context().get_executor(),
		 renew_run(), asio::detached);
  return 0;
}

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
  const auto& name = bs.bucket.name;
  auto shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
  auto r = (ceph_str_hash_linux(name.data(), name.size()) +
	    shard_shift) % num_shards;
  return static_cast<int>(r);
}

asio::awaitable<void>
RGWDataChangesLog::renew_entries(const DoutPrefixProvider* dpp)
{
  if (!zone->log_data)
    co_return;

  /* we can't keep the bucket name as part of the cls::log::entry, and we need
   * it later, so we keep two lists under the map */
  bc::flat_map<int, std::pair<std::vector<BucketGen>,
			      RGWDataChangesBE::entries>> m;

  std::unique_lock l(lock);
  decltype(cur_cycle) entries;
  entries.swap(cur_cycle);
  l.unlock();

  auto ut = real_clock::now();
  auto be = bes->head();
  for (const auto& [bs, gen] : entries) {
    auto index = choose_oid(bs);

    rgw_data_change change;
    buffer::list bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = ut;
    change.gen = gen;
    encode(change, bl);

    m[index].first.push_back({bs, gen});
    be->prepare(ut, change.key, std::move(bl), m[index].second);
  }

  for (auto& [index, p] : m) {
    auto& [buckets, entries] = p;

    auto now = real_clock::now();
    co_await be->push(dpp, index, std::move(entries));
    auto expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);
    for (auto& [bs, gen] : buckets) {
      update_renewed(bs, gen, expiration);
    }
  }
  co_return;
}

auto RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs,
				    uint64_t gen)
  -> ChangeStatusPtr
{
  ChangeStatusPtr status;
  if (!changes.find({bs, gen}, status)) {
    status = std::make_shared<ChangeStatus>(store->get_io_context()
					    .get_executor());
    changes.add({bs, gen}, status);
  }
  return status;
}

void RGWDataChangesLog::register_renew(const rgw_bucket_shard& bs,
				       const rgw::bucket_log_layout_generation& gen)
{
  std::scoped_lock l{lock};
  cur_cycle.insert({bs, gen.gen});
}

void RGWDataChangesLog::update_renewed(const rgw_bucket_shard& bs,
				       uint64_t gen,
				       real_time expiration)
{
  std::unique_lock l{lock};
  auto status = _get_change(bs, gen);
  l.unlock();

  ldout(cct, 20) << "RGWDataChangesLog::update_renewed() bucket_name="
		 << bs.bucket.name << " shard_id=" << bs.shard_id
		 << " expiration=" << expiration << dendl;

  std::unique_lock sl(status->lock);
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);
  return choose_oid(bs);
}

asio::awaitable<bool>
RGWDataChangesLog::filter_bucket(const DoutPrefixProvider *dpp,
				 const rgw_bucket& bucket) const
{
  if (!bucket_filter) {
    co_return true;
  }

  co_return co_await
    async::async_yield<spawn::yield_context>(
      store->get_io_context().get_executor(),
      [this, dpp, &bucket](spawn::yield_context yc) {
	optional_yield y(store->get_io_context(), yc);
	return bucket_filter(bucket, y, dpp);
      }, asio::use_awaitable);
  co_return true;
}

std::string RGWDataChangesLog::get_oid(uint64_t gen_id, int i) const {
  return (gen_id > 0 ?
	  fmt::format("{}@G{}.{}", prefix, gen_id, i) :
	  fmt::format("{}.{}", prefix, i));
}

asio::awaitable<void>
RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
			     const RGWBucketInfo& bucket_info,
			     const rgw::bucket_log_layout_generation& gen,
			     int shard_id)
{
  if (!zone->log_data) {
    co_return;
  }

  auto& bucket = bucket_info.bucket;

  if (!co_await filter_bucket(dpp, bucket)) {
    co_return;
  }

  if (observer) {
    observer->on_bucket_changed(bucket.get_key());
  }

  rgw_bucket_shard bs(bucket, shard_id);

  int index = choose_oid(bs);

  mark_modified(index, bs, gen.gen);

  std::unique_lock l(lock);

  auto status = _get_change(bs, gen.gen);
  l.unlock();

  auto now = real_clock::now();

  std::unique_lock sl(status->lock);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name
		     << " shard_id=" << shard_id << " now=" << now
		     << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    sl.unlock();
    register_renew(bs, gen);
    co_return;
  }

  if (status->pending) {
    co_await status->cond.wait(sl, asio::use_awaitable);
    sl.unlock();
    register_renew(bs, gen);
    co_return;
  }

  status->cond.reset();
  status->pending = true;

  ceph::real_time expiration;
  status->cur_sent = now;

  expiration = now;
  expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);

  sl.unlock();

  buffer::list bl;
  rgw_data_change change;
  change.entity_type = ENTITY_TYPE_BUCKET;
  change.key = bs.get_key();
  change.timestamp = now;
  change.gen = gen.gen;
  encode(change, bl);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

  auto be = bes->head();
  co_await be->push(dpp, index, now, change.key, std::move(bl));

  now = real_clock::now();

  sl.lock();

  status->pending = false;
  /* time of when operation started, not completed */
  status->cur_expiration = status->cur_sent;
  status->cur_expiration += make_timespan(cct->_conf->rgw_data_log_window);
  sl.unlock();

  status->cond.notify();

  co_return;
}

int RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
				 const RGWBucketInfo& bucket_info,
				 const rgw::bucket_log_layout_generation& gen,
				 int shard_id, optional_yield y)
{
  std::exception_ptr eptr;
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    eptr = asio::co_spawn(context.get_executor(),
			  add_entry(dpp, bucket_info, gen, shard_id),
			  yield);

  } else {
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << "WARNING: blocking call in asio thread!" << dendl;
#ifdef _BACKTRACE_LOGGING
      ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": "
			 << ClibBackTrace(0) << dendl;
#endif
    }
    eptr = asio::co_spawn(store->get_io_context().get_executor(),
			  add_entry(dpp, bucket_info, gen, shard_id),
			  async::use_blocked);
  }
  return eptr_to_int(dpp, eptr, __PRETTY_FUNCTION__);
}

asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			   std::optional<std::string>>>
DataLogBackends::list(const DoutPrefixProvider *dpp, int shard,
		      std::span<rgw_data_change_log_entry> entries,
		      std::optional<std::string> marker)
{
  const auto [start_id, // Starting generation
	      start_cursor // Cursor to be used when listing the
			   // starting generation
    ] = cursorgen(marker);
  auto gen_id = start_id; // Current generation being listed
  // Cursor with prepended generation, returned to caller
  std::optional<std::string> out_cursor;
  // Span to return to caller
  auto entries_out = entries;
  // Iterator (for inserting stuff into span)
  auto out = entries_out.begin();
  // Allocated storage for raw listings from backend
  std::vector<rgw_data_change_log_entry> gentries{entries.size()};
  while (out < entries_out.end()) {
    std::unique_lock l(m);
    auto i = lower_bound(gen_id);
    if (i == end())
      break;
    auto be = i->second;
    l.unlock();
    gen_id = be->gen_id;
    auto inspan = std::span{gentries}.first(entries_out.end() - out);
    // Since later generations continue listings from the
    // first, start them at the beginning.
    auto incursor = (gen_id == start_id ?
		     std::optional{start_cursor} :
		     std::nullopt);
    auto [raw_entries, raw_cursor]
      = co_await be->list(dpp, shard, inspan, incursor);
    out = std::transform(std::make_move_iterator(raw_entries.begin()),
			 std::make_move_iterator(raw_entries.end()),
			 out, [gen_id](rgw_data_change_log_entry e) {
			   e.log_id = gencursor(gen_id, e.log_id);
			   return e;
			 });
    if (raw_cursor && !raw_cursor->empty()) {
      out_cursor = gencursor(gen_id, *raw_cursor);
    } else {
      out_cursor = std::nullopt;
      break;
    }
    ++gen_id;
  }
  entries_out = entries_out.first(out - entries_out.begin());
  co_return std::make_tuple(entries_out, std::move(out_cursor));
}

int RGWDataChangesLog::list_entries(
  const DoutPrefixProvider *dpp, int shard,
  int max_entries, std::vector<rgw_data_change_log_entry>& entries,
  std::string_view marker_, std::string* out_marker, bool* truncated,
  optional_yield y)
{
  assert(shard < num_shards);
  std::exception_ptr eptr;
  std::tuple<std::span<rgw_data_change_log_entry>,
	     std::optional<std::string>> out;
  if (std::ssize(entries) < max_entries) {
    entries.resize(max_entries);
  }
  std::optional<std::string> marker = {marker_.empty() ?
				       std::optional{std::string{marker_}} :
				       std::nullopt};
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    std::tie(eptr, out) = asio::co_spawn(context.get_executor(),
					 bes->list(dpp, shard, entries, marker),
					 yield);

  } else {
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << "WARNING: blocking call in asio thread!" << dendl;
#ifdef _BACKTRACE_LOGGING
      ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": "
			 << ClibBackTrace(0) << dendl;
#endif
    }
    std::tie(eptr, out) = asio::co_spawn(store->get_io_context().get_executor(),
					 bes->list(dpp, shard, entries, marker),
					 async::use_blocked);
  }
  if (eptr) {
    return eptr_to_int(dpp, eptr, __PRETTY_FUNCTION__);
  }
  auto& [outries, outmark] = out;
  if (auto size = std::ssize(outries); size < std::ssize(entries)) {
    entries.resize(size);
  }
  if (truncated) {
    *truncated = !outmark;
  }
  if (out_marker) {
    if (outmark) {
      *out_marker = std::move(*outmark);
    } else {
      out_marker->clear();
    }
  }
  return 0;
}

int RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp, int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    LogMarker& marker, bool *ptruncated,
				    optional_yield y)
{
  bool truncated;
  entries.clear();
  for (; marker.shard < num_shards && int(entries.size()) < max_entries;
       marker.shard++, marker.marker.clear()) {
    int ret = list_entries(dpp, marker.shard, max_entries - entries.size(),
			   entries, marker.marker, NULL, &truncated, y);
    if (ret == -ENOENT) {
      continue;
    }
    if (ret < 0) {
      return ret;
    }
    if (!truncated) {
      *ptruncated = false;
      return 0;
    }
  }
  *ptruncated = (marker.shard < num_shards);
  return 0;
}

int RGWDataChangesLog::get_info(const DoutPrefixProvider* dpp, int shard_id,
				RGWDataChangesLogInfo* info, optional_yield y)
{
  assert(shard_id < num_shards);
  auto be = bes->head();
  std::exception_ptr eptr;
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    std::tie(eptr, *info) = asio::co_spawn(context.get_executor(),
					   be->get_info(dpp, shard_id),
					   yield);

  } else {
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << "WARNING: blocking call in asio thread!" << dendl;
#ifdef _BACKTRACE_LOGGING
      ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": "
			 << ClibBackTrace(0) << dendl;
#endif
    }
    std::tie(eptr, *info) = asio::co_spawn(store->get_io_context().get_executor(),
					   be->get_info(dpp, shard_id),
					   async::use_blocked);
  }
  if (!info->marker.empty()) {
    info->marker = gencursor(be->gen_id, info->marker);
  }
  return eptr_to_int(dpp, eptr, __PRETTY_FUNCTION__);
}

asio::awaitable<void> DataLogBackends::trim_entries(
  const DoutPrefixProvider *dpp, int shard_id,
  std::string_view marker)
{
  auto [target_gen, cursor] = cursorgen(std::string{marker});
  std::unique_lock l(m);
  const auto head_gen = (end() - 1)->second->gen_id;
  const auto tail_gen = begin()->first;
  if (target_gen < tail_gen)
    co_return;
  auto r = 0;
  for (auto be = lower_bound(0)->second;
       be->gen_id <= target_gen && be->gen_id <= head_gen && r >= 0;
       be = upper_bound(be->gen_id)->second) {
    l.unlock();
    auto c = be->gen_id == target_gen ? cursor : be->max_marker();
    co_await be->trim(dpp, shard_id, c);
    if (r == -ENOENT)
      r = -ENODATA;
    if (r == -ENODATA && be->gen_id < target_gen)
      r = 0;
    if (be->gen_id == target_gen)
      break;
    l.lock();
  };
  co_return;
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id,
				    std::string_view marker, optional_yield y)
{
  assert(shard_id < num_shards);
  std::exception_ptr eptr;
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    eptr = asio::co_spawn(context.get_executor(),
			  bes->trim_entries(dpp, shard_id, marker),
			  yield);

  } else {
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << "WARNING: blocking call in asio thread!" << dendl;
#ifdef _BACKTRACE_LOGGING
      ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": "
			 << ClibBackTrace(0) << dendl;
#endif
    }
    eptr = asio::co_spawn(store->get_io_context().get_executor(),
			  bes->trim_entries(dpp, shard_id, marker),
			  async::use_blocked);
  }
  return eptr_to_int(dpp, eptr, __PRETTY_FUNCTION__);
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider* dpp, int shard_id,
				    std::string_view marker,
				    librados::AioCompletion* c) {
  async::coro_aiocomplete(dpp, store->get_io_context().get_executor(),
				 bes->trim_entries(dpp, shard_id, marker),
				 c);
  return 0;
}


asio::awaitable<void> DataLogBackends::trim_generations(
  const DoutPrefixProvider *dpp,
  std::optional<uint64_t>& through)
{
  if (size() != 1) {
    std::vector<mapped_type> candidates;
    {
      std::scoped_lock l(m);
      auto e = cend() - 1;
      for (auto i = cbegin(); i < e; ++i) {
	candidates.push_back(i->second);
      }
    }

    std::optional<uint64_t> highest;
    for (auto& be : candidates) {
      if (co_await be->is_empty(dpp)) {
	highest = be->gen_id;
      } else {
	break;
      }
    }

    through = highest;
    if (!highest) {
      co_return;
    }
    co_await empty_to(dpp, *highest);
  }

  co_await remove_empty(dpp);
  co_return;
}


bool RGWDataChangesLog::going_down() const
{
  return down_flag;
}

RGWDataChangesLog::~RGWDataChangesLog() {
  down_flag = true;
  renew_stop();
}

asio::awaitable<void> RGWDataChangesLog::renew_run() {
  static constexpr auto runs_per_prune = 150;
  auto run = 0;
  renew_timer.emplace(co_await asio::this_coro::executor);
  co_await asio::this_coro::throw_if_cancelled(true);
  co_await asio::this_coro::reset_cancellation_state(
    asio::enable_terminal_cancellation());
  std::string_view operation;
  const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
  for (;;) try {
      ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: start"
			<< dendl;
      operation = "RGWDataChangesLog::renew_entries"sv;
      co_await renew_entries(&dp);
      operation = {};
      if (going_down())
	break;

      if (run == runs_per_prune) {
	std::optional<uint64_t> through;
	ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: pruning old generations" << dendl;
	// This null_yield can stay, for now, as it's in its own thread.
	operation = "trim_generations"sv;
	co_await bes->trim_generations(&dp, through);
	operation = {};
	if (through) {
	  ldpp_dout(&dp, 2)
	    << "RGWDataChangesLog::ChangesRenewThread: pruned generations "
	    << "through " << *through << "." << dendl;
	} else {
	  ldpp_dout(&dp, 2)
	    << "RGWDataChangesLog::ChangesRenewThread: nothing to prune."
	    << dendl;
	}
        run = 0;
      } else {
	++run;
      }

      int interval = cct->_conf->rgw_data_log_window * 3 / 4;
      renew_timer->expires_after(std::chrono::seconds(interval));
      co_await renew_timer->async_wait(asio::use_awaitable);
    } catch (sys::system_error& e) {
      if (e.code() == asio::error::operation_aborted) {
	ldpp_dout(&dp, 10)
	  << "RGWDataChangesLog::renew_entries canceled, going down" << dendl;
	break;
      } else {
	ldpp_dout(&dp, 0)
	  << "renew_thread: ERROR: "
	  << (operation.empty() ? operation : "<unknown"sv)
	  << "threw exception: " << e.what() << dendl;
	continue;
      }
    }
}

void RGWDataChangesLog::renew_stop()
{
  std::lock_guard l{lock};
  if (renew_timer) {
    renew_timer->cancel();
  }
}

void RGWDataChangesLog::mark_modified(int shard_id, const rgw_bucket_shard& bs, uint64_t gen)
{
  if (!cct->_conf->rgw_data_notify_interval_msec) {
    return;
  }

  auto key = bs.get_key();
  {
    std::shared_lock rl{modified_lock}; // read lock to check for existence
    auto shard = modified_shards.find(shard_id);
    if (shard != modified_shards.end() && shard->second.count({key, gen})) {
      return;
    }
  }

  std::unique_lock wl{modified_lock}; // write lock for insertion
  modified_shards[shard_id].insert(rgw_data_notify_entry{key, gen});
}

std::string RGWDataChangesLog::max_marker() const {
  return gencursor(std::numeric_limits<uint64_t>::max(),
		   "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
}

int RGWDataChangesLog::change_format(const DoutPrefixProvider *dpp, log_type type, optional_yield y) {
  std::exception_ptr eptr;
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    eptr = asio::co_spawn(context.get_executor(),
			  bes->new_backing(dpp, type),
			  yield);

  } else {
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << "WARNING: blocking call in asio thread!" << dendl;
#ifdef _BACKTRACE_LOGGING
      ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": "
			 << ClibBackTrace(0) << dendl;
#endif
    }
    eptr = asio::co_spawn(store->get_io_context().get_executor(),
			  bes->new_backing(dpp, type),
			  async::use_blocked);
  }
  return eptr_to_int(dpp, eptr, __PRETTY_FUNCTION__);
}

int RGWDataChangesLog::trim_generations(const DoutPrefixProvider *dpp,
					std::optional<uint64_t>& through,
					optional_yield y) {
  std::exception_ptr eptr;
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    eptr = asio::co_spawn(context.get_executor(),
			  bes->trim_generations(dpp, through),
			  yield);

  } else {
    // work on asio threads should be asynchronous, so warn when they block
    if (is_asio_thread) {
      ldpp_dout(dpp, 20) << __PRETTY_FUNCTION__ << ":" << __LINE__
			 << "WARNING: blocking call in asio thread!" << dendl;
#ifdef _BACKTRACE_LOGGING
      ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": "
			 << ClibBackTrace(0) << dendl;
#endif
    }
    eptr = asio::co_spawn(store->get_io_context().get_executor(),
			  bes->trim_generations(dpp, through),
			  async::use_blocked);
  }
  return eptr_to_int(dpp, eptr, __PRETTY_FUNCTION__);
}

void RGWDataChangesLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWDataChangesLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("last_update", last_update, obj);
}
