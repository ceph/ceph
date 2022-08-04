// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <vector>

#include "common/debug.h"
#include "common/containers.h"
#include "common/errno.h"
#include "common/error_code.h"

#include "common/async/blocked_completion.h"
#include "common/async/librados_completion.h"

#include "cls/fifo/cls_fifo_types.h"
#include "cls/log/cls_log_client.h"

#include "cls_fifo_legacy.h"
#include "rgw_datalog.h"
#include "rgw_log_backing.h"
#include "rgw_tools.h"

#define dout_context g_ceph_context
static constexpr auto dout_subsys = ceph_subsys_rgw;

namespace bs = boost::system;
namespace lr = librados;

using ceph::containers::tiny_vector;

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

class RGWDataChangesOmap final : public RGWDataChangesBE {
  using centries = std::list<cls_log_entry>;
  std::vector<std::string> oids;

public:
  RGWDataChangesOmap(lr::IoCtx& ioctx,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     int num_shards)
    : RGWDataChangesBE(ioctx, datalog, gen_id) {
    oids.reserve(num_shards);
    for (auto i = 0; i < num_shards; ++i) {
      oids.push_back(get_oid(i));
    }
  }
  ~RGWDataChangesOmap() override = default;

  void prepare(ceph::real_time ut, const std::string& key,
	       ceph::buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](const auto& v) { return std::empty(v); }, out));
      out = centries();
    }

    cls_log_entry e;
    cls_log_add_prepare_entry(e, utime_t(ut), {}, key, entry);
    std::get<centries>(out).push_back(std::move(e));
  }
  int push(const DoutPrefixProvider *dpp, int index, entries&& items) override {
    lr::ObjectWriteOperation op;
    cls_log_add(op, std::get<centries>(items), true);
    auto r = rgw_rados_operate(dpp, ioctx, oids[index], &op, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": failed to push to " << oids[index] << cpp_strerror(-r)
		 << dendl;
    }
    return r;
  }
  int push(const DoutPrefixProvider *dpp, int index, ceph::real_time now,
	   const std::string& key,
	   ceph::buffer::list&& bl) override {
    lr::ObjectWriteOperation op;
    cls_log_add(op, utime_t(now), {}, key, bl);
    auto r = rgw_rados_operate(dpp, ioctx, oids[index], &op, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": failed to push to " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int list(const DoutPrefixProvider *dpp, int index, int max_entries,
	   std::vector<rgw_data_change_log_entry>& entries,
	   std::optional<std::string_view> marker,
	   std::string* out_marker, bool* truncated) override {
    std::list<cls_log_entry> log_entries;
    lr::ObjectReadOperation op;
    cls_log_list(op, {}, {}, std::string(marker.value_or("")),
		 max_entries, log_entries, out_marker, truncated);
    auto r = rgw_rados_operate(dpp, ioctx, oids[index], &op, nullptr, null_yield);
    if (r == -ENOENT) {
      *truncated = false;
      return 0;
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": failed to list " << oids[index]
		 << cpp_strerror(-r) << dendl;
      return r;
    }
    for (auto iter = log_entries.begin(); iter != log_entries.end(); ++iter) {
      rgw_data_change_log_entry log_entry;
      log_entry.log_id = iter->id;
      auto rt = iter->timestamp.to_real_time();
      log_entry.log_timestamp = rt;
      auto liter = iter->data.cbegin();
      try {
	decode(log_entry.entry, liter);
      } catch (ceph::buffer::error& err) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": failed to decode data changes log entry: "
		   << err.what() << dendl;
	return -EIO;
      }
      entries.push_back(log_entry);
    }
    return 0;
  }
  int get_info(const DoutPrefixProvider *dpp, int index, RGWDataChangesLogInfo *info) override {
    cls_log_header header;
    lr::ObjectReadOperation op;
    cls_log_info(op, &header);
    auto r = rgw_rados_operate(dpp, ioctx, oids[index], &op, nullptr, null_yield);
    if (r == -ENOENT) r = 0;
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    } else {
      info->marker = header.max_marker;
      info->last_update = header.max_time.to_real_time();
    }
    return r;
  }
  int trim(const DoutPrefixProvider *dpp, int index, std::string_view marker) override {
    lr::ObjectWriteOperation op;
    cls_log_trim(op, {}, {}, {}, std::string(marker));
    auto r = rgw_rados_operate(dpp, ioctx, oids[index], &op, null_yield);
    if (r == -ENOENT) r = -ENODATA;
    if (r < 0 && r != -ENODATA) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(const DoutPrefixProvider *dpp, int index, std::string_view marker,
	   lr::AioCompletion* c) override {
    lr::ObjectWriteOperation op;
    cls_log_trim(op, {}, {}, {}, std::string(marker));
    auto r = ioctx.aio_operate(oids[index], c, &op, 0);
    if (r == -ENOENT) r = -ENODATA;
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  std::string_view max_marker() const override {
    return "99999999"sv;
  }
  int is_empty(const DoutPrefixProvider *dpp) override {
    for (auto shard = 0u; shard < oids.size(); ++shard) {
      std::list<cls_log_entry> log_entries;
      lr::ObjectReadOperation op;
      std::string out_marker;
      bool truncated;
      cls_log_list(op, {}, {}, {}, 1, log_entries, &out_marker, &truncated);
      auto r = rgw_rados_operate(dpp, ioctx, oids[shard], &op, nullptr, null_yield);
      if (r == -ENOENT) {
	continue;
      }
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": failed to list " << oids[shard]
		   << cpp_strerror(-r) << dendl;
	return r;
      }
      if (!log_entries.empty()) {
	return 0;
      }
    }
    return 1;
  }
};

class RGWDataChangesFIFO final : public RGWDataChangesBE {
  using centries = std::vector<ceph::buffer::list>;
  tiny_vector<LazyFIFO> fifos;

public:
  RGWDataChangesFIFO(lr::IoCtx& ioctx,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id, int shards)
    : RGWDataChangesBE(ioctx, datalog, gen_id),
      fifos(shards, [&ioctx, this](std::size_t i, auto emplacer) {
	emplacer.emplace(ioctx, get_oid(i));
      }) {}
  ~RGWDataChangesFIFO() override = default;
  void prepare(ceph::real_time, const std::string&,
	       ceph::buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](auto& v) { return std::empty(v); }, out));
      out = centries();
    }
    std::get<centries>(out).push_back(std::move(entry));
  }
  int push(const DoutPrefixProvider *dpp, int index, entries&& items) override {
    auto r = fifos[index].push(dpp, std::get<centries>(items), null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to push to FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int push(const DoutPrefixProvider *dpp, int index, ceph::real_time,
	   const std::string&,
	   ceph::buffer::list&& bl) override {
    auto r = fifos[index].push(dpp, std::move(bl), null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to push to FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int list(const DoutPrefixProvider *dpp, int index, int max_entries,
	   std::vector<rgw_data_change_log_entry>& entries,
	   std::optional<std::string_view> marker,
	   std::string* out_marker, bool* truncated) override {
    std::vector<rgw::cls::fifo::list_entry> log_entries;
    bool more = false;
    auto r = fifos[index].list(dpp, max_entries, marker, &log_entries, &more,
			       null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to list FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& entry : log_entries) {
      rgw_data_change_log_entry log_entry;
      log_entry.log_id = entry.marker;
      log_entry.log_timestamp = entry.mtime;
      auto liter = entry.data.cbegin();
      try {
	decode(log_entry.entry, liter);
      } catch (const buffer::error& err) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": failed to decode data changes log entry: "
		   << err.what() << dendl;
	return -EIO;
      }
      entries.push_back(std::move(log_entry));
    }
    if (truncated)
      *truncated = more;
    if (out_marker && !log_entries.empty()) {
      *out_marker = log_entries.back().marker;
    }
    return 0;
  }
  int get_info(const DoutPrefixProvider *dpp, int index, RGWDataChangesLogInfo *info) override {
    auto& fifo = fifos[index];
    auto r = fifo.read_meta(dpp, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to get FIFO metadata: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    rados::cls::fifo::info m;
    fifo.meta(dpp, m, null_yield);
    auto p = m.head_part_num;
    if (p < 0) {
      info->marker = ""s;
      info->last_update = ceph::real_clock::zero();
      return 0;
    }
    rgw::cls::fifo::part_info h;
    r = fifo.get_part_info(dpp, p, &h, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to get part info: " << get_oid(index) << "/" << p
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    info->marker = rgw::cls::fifo::marker{p, h.last_ofs}.to_string();
    info->last_update = h.max_time;
    return 0;
  }
  int trim(const DoutPrefixProvider *dpp, int index, std::string_view marker) override {
    auto r = fifos[index].trim(dpp, marker, false, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to trim FIFO: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(const DoutPrefixProvider *dpp, int index, std::string_view marker,
	   librados::AioCompletion* c) override {
    int r = 0;
    if (marker == rgw::cls::fifo::marker(0, 0).to_string()) {
      rgw_complete_aio_completion(c, -ENODATA);
    } else {
      fifos[index].trim(dpp, marker, false, c, null_yield);
    }
    return r;
  }
  std::string_view max_marker() const override {
    static const std::string mm =
      rgw::cls::fifo::marker::max().to_string();
    return std::string_view(mm);
  }
  int is_empty(const DoutPrefixProvider *dpp) override {
    std::vector<rgw::cls::fifo::list_entry> log_entries;
    bool more = false;
    for (auto shard = 0u; shard < fifos.size(); ++shard) {
      auto r = fifos[shard].list(dpp, 1, {}, &log_entries, &more,
				 null_yield);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": unable to list FIFO: " << get_oid(shard)
		   << ": " << cpp_strerror(-r) << dendl;
	return r;
      }
      if (!log_entries.empty()) {
	return 0;
      }
    }
    return 1;
  }
};

RGWDataChangesLog::RGWDataChangesLog(CephContext* cct)
  : cct(cct),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    prefix(get_prefix()),
    changes(cct->_conf->rgw_data_log_changes_size) {}

bs::error_code DataLogBackends::handle_init(entries_t e) noexcept {
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
	emplace(gen_id, new RGWDataChangesOmap(ioctx, datalog, gen_id, shards));
	break;
      case log_type::fifo:
	emplace(gen_id, new RGWDataChangesFIFO(ioctx, datalog, gen_id, shards));
	break;
      default:
	lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": IMPOSSIBLE: invalid log type: gen_id=" << gen_id
	  << ", type" << gen.type << dendl;
	return bs::error_code(EFAULT, bs::system_category());
      }
    } catch (const bs::system_error& err) {
      lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": error setting up backend: gen_id=" << gen_id
	  << ", err=" << err.what() << dendl;
      return err.code();
    }
  }
  return {};
}
bs::error_code DataLogBackends::handle_new_gens(entries_t e) noexcept {
  return handle_init(std::move(e));
}
bs::error_code DataLogBackends::handle_empty_to(uint64_t new_tail) noexcept {
  std::unique_lock l(m);
  auto i = cbegin();
  if (i->first < new_tail) {
    return {};
  }
  if (new_tail >= (cend() - 1)->first) {
    lderr(datalog.cct)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << ": ERROR: attempt to trim head: new_tail=" << new_tail << dendl;
    return bs::error_code(EFAULT, bs::system_category());
  }
  erase(i, upper_bound(new_tail));
  return {};
}


int RGWDataChangesLog::start(const DoutPrefixProvider *dpp,
                             const RGWZone* _zone,
			     const RGWZoneParams& zoneparams,
			     librados::Rados* lr)
{
  zone = _zone;
  ceph_assert(zone);
  auto defbacking = to_log_type(
    cct->_conf.get_val<std::string>("rgw_default_data_log_backing"));
  // Should be guaranteed by `set_enum_allowed`
  ceph_assert(defbacking);
  auto log_pool = zoneparams.log_pool;
  auto r = rgw_init_ioctx(dpp, lr, log_pool, ioctx, true, false);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
	       << ": Failed to initialized ioctx, r=" << r
	       << ", pool=" << log_pool << dendl;
    return -r;
  }

  auto besr = logback_generations::init<DataLogBackends>(
    dpp, ioctx, metadata_log_oid(), [this](uint64_t gen_id, int shard) {
      return get_oid(gen_id, shard);
    },
    num_shards, *defbacking, null_yield, *this);


  if (!besr) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
	       << ": Error initializing backends: "
	       << besr.error().message() << dendl;
    return ceph::from_error_code(besr.error());
  }

  bes = std::move(*besr);
  renew_thread = make_named_thread("rgw_dt_lg_renew",
				   &RGWDataChangesLog::renew_run, this);
  return 0;
}

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
  const auto& name = bs.bucket.name;
  auto shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
  auto r = (ceph_str_hash_linux(name.data(), name.size()) +
	    shard_shift) % num_shards;
  return static_cast<int>(r);
}

int RGWDataChangesLog::renew_entries(const DoutPrefixProvider *dpp)
{
  if (!zone->log_data)
    return 0;

  /* we can't keep the bucket name as part of the cls_log_entry, and we need
   * it later, so we keep two lists under the map */
  bc::flat_map<int, std::pair<std::vector<rgw_bucket_shard>,
			      RGWDataChangesBE::entries>> m;

  std::unique_lock l(lock);
  decltype(cur_cycle) entries;
  entries.swap(cur_cycle);
  l.unlock();

  auto ut = real_clock::now();
  auto be = bes->head();
  for (const auto& bs : entries) {
    auto index = choose_oid(bs);

    rgw_data_change change;
    bufferlist bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = ut;
    encode(change, bl);

    m[index].first.push_back(bs);
    be->prepare(ut, change.key, std::move(bl), m[index].second);
  }

  for (auto& [index, p] : m) {
    auto& [buckets, entries] = p;

    auto now = real_clock::now();

    auto ret = be->push(dpp, index, std::move(entries));
    if (ret < 0) {
      /* we don't really need to have a special handling for failed cases here,
       * as this is just an optimization. */
      ldpp_dout(dpp, -1) << "ERROR: svc.cls->timelog.add() returned " << ret << dendl;
      return ret;
    }

    auto expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);
    for (auto& bs : buckets) {
      update_renewed(bs, expiration);
    }
  }

  return 0;
}

void RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs,
				    ChangeStatusPtr& status)
{
  ceph_assert(ceph_mutex_is_locked(lock));
  if (!changes.find(bs, status)) {
    status = ChangeStatusPtr(new ChangeStatus);
    changes.add(bs, status);
  }
}

void RGWDataChangesLog::register_renew(const rgw_bucket_shard& bs)
{
  std::scoped_lock l{lock};
  cur_cycle.insert(bs);
}

void RGWDataChangesLog::update_renewed(const rgw_bucket_shard& bs,
				       real_time expiration)
{
  std::unique_lock l{lock};
  ChangeStatusPtr status;
  _get_change(bs, status);
  l.unlock();


  ldout(cct, 20) << "RGWDataChangesLog::update_renewd() bucket_name="
		 << bs.bucket.name << " shard_id=" << bs.shard_id
		 << " expiration=" << expiration << dendl;

  std::unique_lock sl(status->lock);
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);
  return choose_oid(bs);
}

bool RGWDataChangesLog::filter_bucket(const DoutPrefixProvider *dpp, 
                                      const rgw_bucket& bucket,
				      optional_yield y) const
{
  if (!bucket_filter) {
    return true;
  }

  return bucket_filter(bucket, y, dpp);
}

std::string RGWDataChangesLog::get_oid(uint64_t gen_id, int i) const {
  return (gen_id > 0 ?
	  fmt::format("{}@G{}.{}", prefix, gen_id, i) :
	  fmt::format("{}.{}", prefix, i));
}

int RGWDataChangesLog::add_entry(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, int shard_id) {
  auto& bucket = bucket_info.bucket;

  if (!filter_bucket(dpp, bucket, null_yield)) {
    return 0;
  }

  if (observer) {
    observer->on_bucket_changed(bucket.get_key());
  }

  rgw_bucket_shard bs(bucket, shard_id);

  int index = choose_oid(bs);
  mark_modified(index, bs);

  std::unique_lock l(lock);

  ChangeStatusPtr status;
  _get_change(bs, status);
  l.unlock();

  auto now = real_clock::now();

  std::unique_lock sl(status->lock);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name
		 << " shard_id=" << shard_id << " now=" << now
		 << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    sl.unlock();
    register_renew(bs);
    return 0;
  }

  RefCountedCond* cond;

  if (status->pending) {
    cond = status->cond;

    ceph_assert(cond);

    status->cond->get();
    sl.unlock();

    int ret = cond->wait();
    cond->put();
    if (!ret) {
      register_renew(bs);
    }
    return ret;
  }

  status->cond = new RefCountedCond;
  status->pending = true;

  ceph::real_time expiration;

  int ret;

  do {
    status->cur_sent = now;

    expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);

    sl.unlock();

    ceph::buffer::list bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    encode(change, bl);

    ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

    auto be = bes->head();
    ret = be->push(dpp, index, now, change.key, std::move(bl));

    now = real_clock::now();

    sl.lock();

  } while (!ret && real_clock::now() > expiration);

  cond = status->cond;

  status->pending = false;
  /* time of when operation started, not completed */
  status->cur_expiration = status->cur_sent;
  status->cur_expiration += make_timespan(cct->_conf->rgw_data_log_window);
  status->cond = nullptr;
  sl.unlock();

  cond->done(ret);
  cond->put();

  return ret;
}

int DataLogBackends::list(const DoutPrefixProvider *dpp, int shard, int max_entries,
			  std::vector<rgw_data_change_log_entry>& entries,
			  std::string_view marker,
			  std::string* out_marker,
			  bool* truncated)
{
  const auto [start_id, start_cursor] = cursorgen(marker);
  auto gen_id = start_id;
  std::string out_cursor;
  while (max_entries > 0) {
    std::vector<rgw_data_change_log_entry> gentries;
    std::unique_lock l(m);
    auto i = lower_bound(gen_id);
    if (i == end()) return 0;
    auto be = i->second;
    l.unlock();
    gen_id = be->gen_id;
    auto r = be->list(dpp, shard, max_entries, gentries,
		      gen_id == start_id ? start_cursor : std::string{},
		      &out_cursor, truncated);
    if (r < 0)
      return r;

    if (out_marker && !out_cursor.empty()) {
      *out_marker = gencursor(gen_id, out_cursor);
    }
    for (auto& g : gentries) {
      g.log_id = gencursor(gen_id, g.log_id);
    }
    if (int s = gentries.size(); s < 0 || s > max_entries)
      max_entries = 0;
    else
      max_entries -= gentries.size();

    std::move(gentries.begin(), gentries.end(),
	      std::back_inserter(entries));
    ++gen_id;
  }
  return 0;
}

int RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp, int shard, int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    std::string_view marker,
				    std::string* out_marker, bool* truncated)
{
  assert(shard < num_shards);
  return bes->list(dpp, shard, max_entries, entries, marker, out_marker, truncated);
}

int RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp, int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    LogMarker& marker, bool *ptruncated)
{
  bool truncated;
  entries.clear();
  for (; marker.shard < num_shards && int(entries.size()) < max_entries;
       marker.shard++, marker.marker.clear()) {
    int ret = list_entries(dpp, marker.shard, max_entries - entries.size(),
			   entries, marker.marker, NULL, &truncated);
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

int RGWDataChangesLog::get_info(const DoutPrefixProvider *dpp, int shard_id, RGWDataChangesLogInfo *info)
{
  assert(shard_id < num_shards);
  auto be = bes->head();
  auto r = be->get_info(dpp, shard_id, info);
  if (!info->marker.empty()) {
    info->marker = gencursor(be->gen_id, info->marker);
  }
  return r;
}

int DataLogBackends::trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker)
{
  auto [target_gen, cursor] = cursorgen(marker);
  std::unique_lock l(m);
  const auto head_gen = (end() - 1)->second->gen_id;
  const auto tail_gen = begin()->first;
  if (target_gen < tail_gen) return 0;
  auto r = 0;
  for (auto be = lower_bound(0)->second;
       be->gen_id <= target_gen && be->gen_id <= head_gen && r >= 0;
       be = upper_bound(be->gen_id)->second) {
    l.unlock();
    auto c = be->gen_id == target_gen ? cursor : be->max_marker();
    r = be->trim(dpp, shard_id, c);
    if (r == -ENOENT)
      r = -ENODATA;
    if (r == -ENODATA && be->gen_id < target_gen)
      r = 0;
    if (be->gen_id == target_gen)
      break;
    l.lock();
  };
  return r;
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker)
{
  assert(shard_id < num_shards);
  return bes->trim_entries(dpp, shard_id, marker);
}

class GenTrim : public rgw::cls::fifo::Completion<GenTrim> {
public:
  DataLogBackends* const bes;
  const int shard_id;
  const uint64_t target_gen;
  const std::string cursor;
  const uint64_t head_gen;
  const uint64_t tail_gen;
  boost::intrusive_ptr<RGWDataChangesBE> be;

  GenTrim(const DoutPrefixProvider *dpp, DataLogBackends* bes, int shard_id, uint64_t target_gen,
	  std::string cursor, uint64_t head_gen, uint64_t tail_gen,
	  boost::intrusive_ptr<RGWDataChangesBE> be,
	  lr::AioCompletion* super)
    : Completion(dpp, super), bes(bes), shard_id(shard_id), target_gen(target_gen),
      cursor(std::move(cursor)), head_gen(head_gen), tail_gen(tail_gen),
      be(std::move(be)) {}

  void handle(const DoutPrefixProvider *dpp, Ptr&& p, int r) {
    auto gen_id = be->gen_id;
    be.reset();
    if (r == -ENOENT)
      r = -ENODATA;
    if (r == -ENODATA && gen_id < target_gen)
      r = 0;
    if (r < 0) {
      complete(std::move(p), r);
      return;
    }

    {
      std::unique_lock l(bes->m);
      auto i = bes->upper_bound(gen_id);
      if (i == bes->end() || i->first > target_gen || i->first > head_gen) {
	l.unlock();
	complete(std::move(p), -ENODATA);
	return;
      }
      be = i->second;
    }
    auto c = be->gen_id == target_gen ? cursor : be->max_marker();
    be->trim(dpp, shard_id, c, call(std::move(p)));
  }
};

void DataLogBackends::trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker,
				   librados::AioCompletion* c)
{
  auto [target_gen, cursor] = cursorgen(marker);
  std::unique_lock l(m);
  const auto head_gen = (end() - 1)->second->gen_id;
  const auto tail_gen = begin()->first;
  if (target_gen < tail_gen) {
    l.unlock();
    rgw_complete_aio_completion(c, -ENODATA);
    return;
  }
  auto be = begin()->second;
  l.unlock();
  auto gt = std::make_unique<GenTrim>(dpp, this, shard_id, target_gen,
				      std::string(cursor), head_gen, tail_gen,
				      be, c);

  auto cc = be->gen_id == target_gen ? cursor : be->max_marker();
  be->trim(dpp, shard_id, cc,  GenTrim::call(std::move(gt)));
}

int DataLogBackends::trim_generations(const DoutPrefixProvider *dpp, std::optional<uint64_t>& through) {
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
      auto r = be->is_empty(dpp);
      if (r < 0) {
	return r;
      } else if (r == 1) {
	highest = be->gen_id;
      } else {
	break;
      }
    }

    through = highest;
    if (!highest) {
      return 0;
    }
    auto ec = empty_to(dpp, *highest, null_yield);
    if (ec) {
      return ceph::from_error_code(ec);
    }
  }

  return ceph::from_error_code(remove_empty(dpp, null_yield));
}


int RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker,
				    librados::AioCompletion* c)
{
  assert(shard_id < num_shards);
  bes->trim_entries(dpp, shard_id, marker, c);
  return 0;
}

bool RGWDataChangesLog::going_down() const
{
  return down_flag;
}

RGWDataChangesLog::~RGWDataChangesLog() {
  down_flag = true;
  if (renew_thread.joinable()) {
    renew_stop();
    renew_thread.join();
  }
}

void RGWDataChangesLog::renew_run() {
  static constexpr auto runs_per_prune = 150;
  auto run = 0;
  for (;;) {
    const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
    ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: start" << dendl;
    int r = renew_entries(&dp);
    if (r < 0) {
      ldpp_dout(&dp, 0) << "ERROR: RGWDataChangesLog::renew_entries returned error r=" << r << dendl;
    }

    if (going_down())
      break;

    if (run == runs_per_prune) {
      std::optional<uint64_t> through;
      ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: pruning old generations" << dendl;
      trim_generations(&dp, through);
      if (r < 0) {
	derr << "RGWDataChangesLog::ChangesRenewThread: failed pruning r="
	     << r << dendl;
      } else if (through) {
	ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: pruned generations "
		<< "through " << *through << "." << dendl;
      } else {
	ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: nothing to prune."
		<< dendl;
      }
      run = 0;
    } else {
      ++run;
    }

    int interval = cct->_conf->rgw_data_log_window * 3 / 4;
    std::unique_lock locker{renew_lock};
    renew_cond.wait_for(locker, std::chrono::seconds(interval));
  }
}

void RGWDataChangesLog::renew_stop()
{
  std::lock_guard l{renew_lock};
  renew_cond.notify_all();
}

void RGWDataChangesLog::mark_modified(int shard_id, const rgw_bucket_shard& bs)
{
  auto key = bs.get_key();
  {
    std::shared_lock rl{modified_lock}; // read lock to check for existence
    auto shard = modified_shards.find(shard_id);
    if (shard != modified_shards.end() && shard->second.count(key)) {
      return;
    }
  }

  std::unique_lock wl{modified_lock}; // write lock for insertion
  modified_shards[shard_id].insert(key);
}

std::string RGWDataChangesLog::max_marker() const {
  return gencursor(std::numeric_limits<uint64_t>::max(),
		   "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
}

int RGWDataChangesLog::change_format(const DoutPrefixProvider *dpp, log_type type, optional_yield y) {
  return ceph::from_error_code(bes->new_backing(dpp, type, y));
}

int RGWDataChangesLog::trim_generations(const DoutPrefixProvider *dpp, std::optional<uint64_t>& through) {
  return bes->trim_generations(dpp, through);
}
