// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <vector>

#include "common/debug.h"
#include "common/errno.h"
#include "common/error_code.h"

#include "common/async/blocked_completion.h"
#include "common/async/librados_completion.h"

#include "cls/fifo/cls_fifo_types.h"

#include "cls_fifo_legacy.h"
#include "rgw_datalog.h"
#include "rgw_tools.h"

#define dout_context g_ceph_context
static constexpr auto dout_subsys = ceph_subsys_rgw;

namespace bs = boost::system;

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

int RGWDataChangesBE::remove(const DoutPrefixProvider *dpp, 
                             CephContext* cct, librados::Rados* rados,
			     const rgw_pool& log_pool)
{
  auto num_shards = cct->_conf->rgw_data_log_num_shards;
  librados::IoCtx ioctx;
  auto r = rgw_init_ioctx(rados, log_pool.name, ioctx,
			  false, false);
  if (r < 0) {
    if (r == -ENOENT) {
      return 0;
    } else {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": rgw_init_ioctx failed: " << log_pool.name
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
  }
  for (auto i = 0; i < num_shards; ++i) {
    auto oid = get_oid(cct, i);
    librados::ObjectWriteOperation op;
    op.remove();
    auto r = rgw_rados_operate(dpp, ioctx, oid, &op, null_yield);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": remove failed: " << log_pool.name << "/" << oid
		 << ": " << cpp_strerror(-r) << dendl;
    }
  }
  return 0;
}


class RGWDataChangesOmap final : public RGWDataChangesBE {
  using centries = std::list<cls_log_entry>;
  RGWSI_Cls& cls;
  std::vector<std::string> oids;
public:
  RGWDataChangesOmap(CephContext* cct, RGWSI_Cls& cls)
    : RGWDataChangesBE(cct), cls(cls) {
    auto num_shards = cct->_conf->rgw_data_log_num_shards;
    oids.reserve(num_shards);
    for (auto i = 0; i < num_shards; ++i) {
      oids.push_back(get_oid(i));
    }
  }
  ~RGWDataChangesOmap() override = default;
  static int exists(const DoutPrefixProvider *dpp, CephContext* cct, RGWSI_Cls& cls, bool* exists,
		    bool* has_entries) {
    auto num_shards = cct->_conf->rgw_data_log_num_shards;
    std::string out_marker;
    bool truncated = false;
    std::list<cls_log_entry> log_entries;
    const cls_log_header empty_info;
    *exists = false;
    *has_entries = false;
    for (auto i = 0; i < num_shards; ++i) {
      cls_log_header info;
      auto oid = get_oid(cct, i);
      auto r = cls.timelog.info(dpp, oid, &info, null_yield);
      if (r < 0 && r != -ENOENT) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": failed to get info " << oid << ": " << cpp_strerror(-r)
		   << dendl;
	return r;
      } else if ((r == -ENOENT) || (info == empty_info)) {
	continue;
      }
      *exists = true;
      r = cls.timelog.list(dpp, oid, {}, {}, 100, log_entries, "", &out_marker,
			   &truncated, null_yield);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": failed to list " << oid << ": " << cpp_strerror(-r)
		   << dendl;
	return r;
      } else if (!log_entries.empty()) {
	*has_entries = true;
	break; // No reason to continue, once we have both existence
	       // AND non-emptiness
      }
    }
    return 0;
  }

  void prepare(ceph::real_time ut, const std::string& key,
	       ceph::buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](const auto& v) { return std::empty(v); }, out));
      out = centries();
    }

    cls_log_entry e;
    cls.timelog.prepare_entry(e, ut, {}, key, entry);
    std::get<centries>(out).push_back(std::move(e));
  }
  int push(const DoutPrefixProvider *dpp, int index, entries&& items) override {
    auto r = cls.timelog.add(dpp, oids[index], std::get<centries>(items),
			     nullptr, true, null_yield);
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
    auto r = cls.timelog.add(dpp, oids[index], now, {}, key, bl, null_yield);
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
    auto r = cls.timelog.list(dpp, oids[index], {}, {},
			      max_entries, log_entries,
			      std::string(marker.value_or("")),
			      out_marker, truncated, null_yield);
    if (r == -ENOENT) {
      *truncated = false;
      return 0;
    }
    if (r < 0) {
      lderr(cct) << __PRETTY_FUNCTION__
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
	lderr(cct) << __PRETTY_FUNCTION__
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
    auto r = cls.timelog.info(dpp, oids[index], &header, null_yield);
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
    auto r = cls.timelog.trim(dpp, oids[index], {}, {},
			      {}, std::string(marker), nullptr,
			      null_yield);

    if (r == -ENOENT) r = 0;
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": failed to get info from " << oids[index]
		 << cpp_strerror(-r) << dendl;
    }
    return r;
  }
  int trim(const DoutPrefixProvider *dpp, int index, std::string_view marker,
	   librados::AioCompletion* c) override {
    auto r = cls.timelog.trim(dpp, oids[index], {}, {},
			    {}, std::string(marker), c, null_yield);

    if (r == -ENOENT) r = 0;
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
};

class RGWDataChangesFIFO final : public RGWDataChangesBE {
  using centries = std::vector<ceph::buffer::list>;
  std::vector<std::unique_ptr<rgw::cls::fifo::FIFO>> fifos;
public:
  RGWDataChangesFIFO(const DoutPrefixProvider *dpp, CephContext* cct, librados::Rados* rados,
		     const rgw_pool& log_pool)
    : RGWDataChangesBE(cct) {
    librados::IoCtx ioctx;
    auto shards = cct->_conf->rgw_data_log_num_shards;
    auto r = rgw_init_ioctx(rados, log_pool.name, ioctx,
			    true, false);
    if (r < 0) {
      throw bs::system_error(ceph::to_error_code(r));
    }
    fifos.resize(shards);
    for (auto i = 0; i < shards; ++i) {
      r = rgw::cls::fifo::FIFO::create(dpp, ioctx, get_oid(i),
				       &fifos[i], null_yield);
      if (r < 0) {
	throw bs::system_error(ceph::to_error_code(r));
      }
    }
    ceph_assert(fifos.size() == unsigned(shards));
    ceph_assert(std::none_of(fifos.cbegin(), fifos.cend(),
			     [](const auto& p) {
			       return p == nullptr;
			     }));
  }
  ~RGWDataChangesFIFO() override = default;
  static int exists(const DoutPrefixProvider *dpp, CephContext* cct, librados::Rados* rados,
		    const rgw_pool& log_pool, bool* exists, bool* has_entries) {
    auto num_shards = cct->_conf->rgw_data_log_num_shards;
    librados::IoCtx ioctx;
    auto r = rgw_init_ioctx(rados, log_pool.name, ioctx,
			    false, false);
    if (r < 0) {
      if (r == -ENOENT) {
	return 0;
      } else {
	lderr(cct) << __PRETTY_FUNCTION__
		   << ": rgw_init_ioctx failed: " << log_pool.name
		   << ": " << cpp_strerror(-r) << dendl;
	return r;
      }
    }
    *exists = false;
    *has_entries = false;
    for (auto i = 0; i < num_shards; ++i) {
      std::unique_ptr<rgw::cls::fifo::FIFO> fifo;
      auto oid = get_oid(cct, i);
      std::vector<rgw::cls::fifo::list_entry> log_entries;
      bool more = false;
      auto r = rgw::cls::fifo::FIFO::open(dpp, ioctx, oid,
					  &fifo, null_yield,
					  std::nullopt, true);
      if (r == -ENOENT || r == -ENODATA) {
	continue;
      } else if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": unable to open FIFO: " << log_pool << "/" << oid
		   << ": " << cpp_strerror(-r) << dendl;
	return r;
      }
      *exists = true;
      r = fifo->list(dpp, 1, nullopt, &log_entries, &more,
		     null_yield);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": unable to list entries: " << log_pool << "/" << oid
		   << ": " << cpp_strerror(-r) << dendl;
      } else if (!log_entries.empty()) {
	*has_entries = true;
	break;
      }
    }
    return 0;
  }
  void prepare(ceph::real_time, const std::string&,
	       ceph::buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](auto& v) { return std::empty(v); }, out));
      out = centries();
    }
    std::get<centries>(out).push_back(std::move(entry));
  }
  int push(const DoutPrefixProvider *dpp, int index, entries&& items) override {
    auto r = fifos[index]->push(dpp, std::get<centries>(items), null_yield);
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
    auto r = fifos[index]->push(dpp, std::move(bl), null_yield);
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
    auto r = fifos[index]->list(dpp, max_entries, marker, &log_entries, &more,
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
    auto r = fifo->read_meta(dpp, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": unable to get FIFO metadata: " << get_oid(index)
		 << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    auto m = fifo->meta();
    auto p = m.head_part_num;
    if (p < 0) {
      info->marker = rgw::cls::fifo::marker{}.to_string();
      info->last_update = ceph::real_clock::zero();
      return 0;
    }
    rgw::cls::fifo::part_info h;
    r = fifo->get_part_info(dpp, p, &h, null_yield);
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
    auto r = fifos[index]->trim(dpp, marker, false, null_yield);
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
      auto pc = c->pc;
      pc->get();
      pc->lock.lock();
      pc->rval = 0;
      pc->complete = true;
      pc->lock.unlock();
      auto cb_complete = pc->callback_complete;
      auto cb_complete_arg = pc->callback_complete_arg;
      if (cb_complete)
	cb_complete(pc, cb_complete_arg);

      auto cb_safe = pc->callback_safe;
      auto cb_safe_arg = pc->callback_safe_arg;
      if (cb_safe)
	cb_safe(pc, cb_safe_arg);

      pc->lock.lock();
      pc->callback_complete = NULL;
      pc->callback_safe = NULL;
      pc->cond.notify_all();
      pc->put_unlock();
    } else {
      r = fifos[index]->trim(marker, false, c);
      if (r < 0) {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		   << ": unable to trim FIFO: " << get_oid(index)
		   << ": " << cpp_strerror(-r) << dendl;
      }
    }
    return r;
  }
  std::string_view max_marker() const override {
    static const std::string mm =
      rgw::cls::fifo::marker::max().to_string();
    return std::string_view(mm);
  }
};

RGWDataChangesLog::RGWDataChangesLog(CephContext* cct)
  : cct(cct),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    changes(cct->_conf->rgw_data_log_changes_size) {}

int RGWDataChangesLog::start(const DoutPrefixProvider *dpp, 
                             const RGWZone* _zone,
			     const RGWZoneParams& zoneparams,
			     RGWSI_Cls *cls, librados::Rados* lr)
{
  zone = _zone;
  assert(zone);
  auto backing = cct->_conf.get_val<std::string>("rgw_data_log_backing");
  // Should be guaranteed by `set_enum_allowed`
  ceph_assert(backing == "auto" || backing == "fifo" || backing == "omap");
  auto log_pool = zoneparams.log_pool;
  bool omapexists = false, omaphasentries = false;
  auto r = RGWDataChangesOmap::exists(dpp, cct, *cls, &omapexists, &omaphasentries);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
	       << ": Error when checking for existing Omap datalog backend: "
	       << cpp_strerror(-r) << dendl;
  }
  bool fifoexists = false, fifohasentries = false;
  r = RGWDataChangesFIFO::exists(dpp, cct, lr, log_pool, &fifoexists, &fifohasentries);
  if (r < 0) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
	       << ": Error when checking for existing FIFO datalog backend: "
	       << cpp_strerror(-r) << dendl;
  }
  bool has_entries = omaphasentries || fifohasentries;
  bool remove = false;

  if (omapexists && fifoexists) {
    if (has_entries) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": Both Omap and FIFO backends exist, cannot continue."
		 << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 0)
      << __PRETTY_FUNCTION__
      << ": Both Omap and FIFO backends exist, but are empty. Will remove."
      << dendl;
    remove = true;
  }
  if (backing == "omap" && fifoexists) {
    if (has_entries) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": Omap requested, but FIFO backend exists, cannot continue."
		 << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 0) << __PRETTY_FUNCTION__
		  << ": Omap requested, FIFO exists, but is empty. Deleting."
		  << dendl;
    remove = true;
  }
  if (backing == "fifo" && omapexists) {
    if (has_entries) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": FIFO requested, but Omap backend exists, cannot continue."
		 << dendl;
      return -EINVAL;
    }
    ldpp_dout(dpp, 0) << __PRETTY_FUNCTION__
		  << ": FIFO requested, Omap exists, but is empty. Deleting."
		  << dendl;
    remove = true;
  }

  if (remove) {
    r = RGWDataChangesBE::remove(dpp, cct, lr, log_pool);
    if (r < 0) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		 << ": remove failed, cannot continue."
		 << dendl;
      return r;
    }
    omapexists = false;
    fifoexists = false;
  }

  try {
    if (backing == "omap" || (backing == "auto" && omapexists)) {
      be = std::make_unique<RGWDataChangesOmap>(cct, *cls);
    } else if (backing != "omap") {
      be = std::make_unique<RGWDataChangesFIFO>(dpp, cct, lr, log_pool);
    }
  } catch (bs::system_error& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
	       << ": Error when starting backend: "
	       << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }

  ceph_assert(be);
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
  std::scoped_lock l{lock};
  ChangeStatusPtr status;
  _get_change(bs, status);

  ldout(cct, 20) << "RGWDataChangesLog::update_renewd() bucket_name="
		 << bs.bucket.name << " shard_id=" << bs.shard_id
		 << " expiration=" << expiration << dendl;
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

std::string RGWDataChangesLog::get_oid(int i) const {
  return be->get_oid(i);
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

int RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp, int shard, int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    std::optional<std::string_view> marker,
				    std::string* out_marker, bool* truncated)
{
  assert(shard < num_shards);
  return be->list(dpp, shard, max_entries, entries, std::string(marker.value_or("")),
		  out_marker, truncated);
}

int RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp, int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    LogMarker& marker, bool *ptruncated)
{
  bool truncated;
  entries.clear();
  for (; marker.shard < num_shards && int(entries.size()) < max_entries;
       marker.shard++, marker.marker.reset()) {
    int ret = list_entries(dpp, marker.shard, max_entries - entries.size(),
			   entries, marker.marker, NULL, &truncated);
    if (ret == -ENOENT) {
      continue;
    }
    if (ret < 0) {
      return ret;
    }
    if (truncated) {
      *ptruncated = true;
      return 0;
    }
  }
  *ptruncated = (marker.shard < num_shards);
  return 0;
}

int RGWDataChangesLog::get_info(const DoutPrefixProvider *dpp, int shard_id, RGWDataChangesLogInfo *info)
{
  assert(shard_id < num_shards);
  return be->get_info(dpp, shard_id, info);
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker)
{
  assert(shard_id < num_shards);
  return be->trim(dpp, shard_id, marker);
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id, std::string_view marker,
				    librados::AioCompletion* c)
{
  assert(shard_id < num_shards);
  return be->trim(dpp, shard_id, marker, c);
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

void RGWDataChangesLog::renew_run() noexcept {
  for (;;) {
    // TODO FIX THIS 
    const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
    ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: start" << dendl;
    int r = renew_entries(&dp);
    if (r < 0) {
      ldpp_dout(&dp, 0) << "ERROR: RGWDataChangesLog::renew_entries returned error r=" << r << dendl;
    }

    if (going_down())
      break;

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

std::string_view RGWDataChangesLog::max_marker() const {
  return be->max_marker();
}
