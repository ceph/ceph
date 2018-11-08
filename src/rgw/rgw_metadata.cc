// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/intrusive_ptr.hpp>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "rgw_metadata.h"
#include "rgw_coroutine.h"
#include "cls/version/cls_version_types.h"

#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_tools.h"

#include "rgw_cr_rados.h"

#include "services/svc_zone.h"

#include "include/ceph_assert.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

void LogStatusDump::dump(Formatter *f) const {
  string s;
  switch (status) {
    case MDLOG_STATUS_WRITE:
      s = "write";
      break;
    case MDLOG_STATUS_SETATTRS:
      s = "set_attrs";
      break;
    case MDLOG_STATUS_REMOVE:
      s = "remove";
      break;
    case MDLOG_STATUS_COMPLETE:
      s = "complete";
      break;
    case MDLOG_STATUS_ABORT:
      s = "abort";
      break;
    default:
      s = "unknown";
      break;
  }
  encode_json("status", s, f);
}

void RGWMetadataLogData::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(read_version, bl);
  encode(write_version, bl);
  uint32_t s = (uint32_t)status;
  encode(s, bl);
  ENCODE_FINISH(bl);
}

void RGWMetadataLogData::decode(bufferlist::const_iterator& bl) {
   DECODE_START(1, bl);
   decode(read_version, bl);
   decode(write_version, bl);
   uint32_t s;
   decode(s, bl);
   status = (RGWMDLogStatus)s;
   DECODE_FINISH(bl);
}

void RGWMetadataLogData::dump(Formatter *f) const {
  encode_json("read_version", read_version, f);
  encode_json("write_version", write_version, f);
  encode_json("status", LogStatusDump(status), f);
}

void decode_json_obj(RGWMDLogStatus& status, JSONObj *obj) {
  string s;
  JSONDecoder::decode_json("status", s, obj);
  if (s == "complete") {
    status = MDLOG_STATUS_COMPLETE;
  } else if (s == "write") {
    status = MDLOG_STATUS_WRITE;
  } else if (s == "remove") {
    status = MDLOG_STATUS_REMOVE;
  } else if (s == "set_attrs") {
    status = MDLOG_STATUS_SETATTRS;
  } else if (s == "abort") {
    status = MDLOG_STATUS_ABORT;
  } else {
    status = MDLOG_STATUS_UNKNOWN;
  }
}

void RGWMetadataLogData::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("read_version", read_version, obj);
  JSONDecoder::decode_json("write_version", write_version, obj);
  JSONDecoder::decode_json("status", status, obj);
}


int RGWMetadataLog::add_entry(RGWMetadataHandler *handler, const string& section, const string& key, bufferlist& bl) {
  if (!store->svc.zone->need_to_log_metadata())
    return 0;

  string oid;

  string hash_key;
  handler->get_hash_key(section, key, hash_key);

  int shard_id;
  store->shard_name(prefix, cct->_conf->rgw_md_log_max_shards, hash_key, oid, &shard_id);
  mark_modified(shard_id);
  real_time now = real_clock::now();
  return store->time_log_add(oid, now, section, key, bl);
}

int RGWMetadataLog::store_entries_in_shard(list<cls_log_entry>& entries, int shard_id, librados::AioCompletion *completion)
{
  string oid;

  mark_modified(shard_id);
  store->shard_name(prefix, shard_id, oid);
  return store->time_log_add(oid, entries, completion, false);
}

void RGWMetadataLog::init_list_entries(int shard_id, const real_time& from_time, const real_time& end_time, 
                                       string& marker, void **handle)
{
  LogListCtx *ctx = new LogListCtx();

  ctx->cur_shard = shard_id;
  ctx->from_time = from_time;
  ctx->end_time  = end_time;
  ctx->marker    = marker;

  get_shard_oid(ctx->cur_shard, ctx->cur_oid);

  *handle = (void *)ctx;
}

void RGWMetadataLog::complete_list_entries(void *handle) {
  LogListCtx *ctx = static_cast<LogListCtx *>(handle);
  delete ctx;
}

int RGWMetadataLog::list_entries(void *handle,
				 int max_entries,
				 list<cls_log_entry>& entries,
				 string *last_marker,
				 bool *truncated) {
  LogListCtx *ctx = static_cast<LogListCtx *>(handle);

  if (!max_entries) {
    *truncated = false;
    return 0;
  }

  std::string next_marker;
  int ret = store->time_log_list(ctx->cur_oid, ctx->from_time, ctx->end_time,
				 max_entries, entries, ctx->marker,
				 &next_marker, truncated);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  ctx->marker = std::move(next_marker);
  if (last_marker) {
    *last_marker = ctx->marker;
  }

  if (ret == -ENOENT)
    *truncated = false;

  return 0;
}

int RGWMetadataLog::get_info(int shard_id, RGWMetadataLogInfo *info)
{
  string oid;
  get_shard_oid(shard_id, oid);

  cls_log_header header;

  int ret = store->time_log_info(oid, &header);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  info->marker = header.max_marker;
  info->last_update = header.max_time.to_real_time();

  return 0;
}

static void _mdlog_info_completion(librados::completion_t cb, void *arg)
{
  auto infoc = static_cast<RGWMetadataLogInfoCompletion *>(arg);
  infoc->finish(cb);
  infoc->put(); // drop the ref from get_info_async()
}

RGWMetadataLogInfoCompletion::RGWMetadataLogInfoCompletion(info_callback_t cb)
  : completion(librados::Rados::aio_create_completion((void *)this, nullptr,
                                                      _mdlog_info_completion)),
    callback(cb)
{
}

RGWMetadataLogInfoCompletion::~RGWMetadataLogInfoCompletion()
{
  completion->release();
}

int RGWMetadataLog::get_info_async(int shard_id, RGWMetadataLogInfoCompletion *completion)
{
  string oid;
  get_shard_oid(shard_id, oid);

  completion->get(); // hold a ref until the completion fires

  return store->time_log_info_async(completion->get_io_ctx(), oid,
                                    &completion->get_header(),
                                    completion->get_completion());
}

int RGWMetadataLog::trim(int shard_id, const real_time& from_time, const real_time& end_time,
                         const string& start_marker, const string& end_marker)
{
  string oid;
  get_shard_oid(shard_id, oid);

  int ret;

  ret = store->time_log_trim(oid, from_time, end_time, start_marker, end_marker);

  if (ret == -ENOENT || ret == -ENODATA)
    ret = 0;

  return ret;
}
  
int RGWMetadataLog::lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return store->lock_exclusive(store->svc.zone->get_zone_params().log_pool, oid, duration, zone_id, owner_id);
}

int RGWMetadataLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return store->unlock(store->svc.zone->get_zone_params().log_pool, oid, zone_id, owner_id);
}

void RGWMetadataLog::mark_modified(int shard_id)
{
  lock.get_read();
  if (modified_shards.find(shard_id) != modified_shards.end()) {
    lock.unlock();
    return;
  }
  lock.unlock();

  RWLock::WLocker wl(lock);
  modified_shards.insert(shard_id);
}

void RGWMetadataLog::read_clear_modified(set<int> &modified)
{
  RWLock::WLocker wl(lock);
  modified.swap(modified_shards);
  modified_shards.clear();
}

obj_version& RGWMetadataObject::get_version()
{
  return objv;
}

class RGWMetadataTopHandler : public RGWMetadataHandler {
  struct iter_data {
    set<string> sections;
    set<string>::iterator iter;
  };

public:
  RGWMetadataTopHandler() {}

  string get_type() override { return string(); }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) override { return -ENOTSUP; }
  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
                  real_time mtime, JSONObj *obj, sync_type_t sync_type) override { return -ENOTSUP; }

  virtual void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {}

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override { return -ENOTSUP; }

  int list_keys_init(RGWRados *store, const string& marker, void **phandle) override {
    iter_data *data = new iter_data;
    list<string> sections;
    store->meta_mgr->get_sections(sections);
    for (auto& s : sections) {
      data->sections.insert(s);
    }
    data->iter = data->sections.lower_bound(marker);

    *phandle = data;

    return 0;
  }
  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override  {
    iter_data *data = static_cast<iter_data *>(handle);
    for (int i = 0; i < max && data->iter != data->sections.end(); ++i, ++(data->iter)) {
      keys.push_back(*data->iter);
    }

    *truncated = (data->iter != data->sections.end());

    return 0;
  }
  void list_keys_complete(void *handle) override {
    iter_data *data = static_cast<iter_data *>(handle);

    delete data;
  }

  virtual string get_marker(void *handle) override {
    iter_data *data = static_cast<iter_data *>(handle);

    if (data->iter != data->sections.end()) {
      return *(data->iter);
    }

    return string();
  }
};

static RGWMetadataTopHandler md_top_handler;


RGWMetadataManager::RGWMetadataManager(CephContext *_cct, RGWRados *_store)
  : cct(_cct), store(_store)
{
}

RGWMetadataManager::~RGWMetadataManager()
{
  map<string, RGWMetadataHandler *>::iterator iter;

  for (iter = handlers.begin(); iter != handlers.end(); ++iter) {
    delete iter->second;
  }

  handlers.clear();
}

const std::string RGWMetadataLogHistory::oid = "meta.history";

namespace {

int read_history(RGWRados *store, RGWMetadataLogHistory *state,
                 RGWObjVersionTracker *objv_tracker)
{
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto& pool = store->svc.zone->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  bufferlist bl;
  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid, bl, objv_tracker, nullptr);
  if (ret < 0) {
    return ret;
  }
  if (bl.length() == 0) {
    /* bad history object, remove it */
    rgw_raw_obj obj(pool, oid);
    auto sysobj = obj_ctx.get_obj(obj);
    ret = sysobj.wop().remove();
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: meta history is empty, but cannot remove it (" << cpp_strerror(-ret) << ")" << dendl;
      return ret;
    }
    return -ENOENT;
  }
  try {
    auto p = bl.cbegin();
    state->decode(p);
  } catch (buffer::error& e) {
    ldout(store->ctx(), 1) << "failed to decode the mdlog history: "
        << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int write_history(RGWRados *store, const RGWMetadataLogHistory& state,
                  RGWObjVersionTracker *objv_tracker, bool exclusive = false)
{
  bufferlist bl;
  state.encode(bl);

  auto& pool = store->svc.zone->get_zone_params().log_pool;
  const auto& oid = RGWMetadataLogHistory::oid;
  return rgw_put_system_obj(store, pool, oid, bl,
                            exclusive, objv_tracker, real_time{});
}

using Cursor = RGWPeriodHistory::Cursor;

/// read the mdlog history and use it to initialize the given cursor
class ReadHistoryCR : public RGWCoroutine {
  RGWRados *store;
  Cursor *cursor;
  RGWObjVersionTracker *objv_tracker;
  RGWMetadataLogHistory state;
 public:
  ReadHistoryCR(RGWRados *store, Cursor *cursor,
                RGWObjVersionTracker *objv_tracker)
    : RGWCoroutine(store->ctx()), store(store), cursor(cursor),
      objv_tracker(objv_tracker)
  {}

  int operate() {
    reenter(this) {
      yield {
        rgw_raw_obj obj{store->svc.zone->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};
        constexpr bool empty_on_enoent = false;

        using ReadCR = RGWSimpleRadosReadCR<RGWMetadataLogHistory>;
        call(new ReadCR(store->get_async_rados(), store->svc.sysobj, obj,
                        &state, empty_on_enoent, objv_tracker));
      }
      if (retcode < 0) {
        ldout(cct, 1) << "failed to read mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      *cursor = store->period_history->lookup(state.oldest_realm_epoch);
      if (!*cursor) {
        return set_cr_error(cursor->get_error());
      }

      ldout(cct, 10) << "read mdlog history with oldest period id="
          << state.oldest_period_id << " realm_epoch="
          << state.oldest_realm_epoch << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

/// write the given cursor to the mdlog history
class WriteHistoryCR : public RGWCoroutine {
  RGWRados *store;
  Cursor cursor;
  RGWObjVersionTracker *objv;
  RGWMetadataLogHistory state;
 public:
  WriteHistoryCR(RGWRados *store, const Cursor& cursor,
                 RGWObjVersionTracker *objv)
    : RGWCoroutine(store->ctx()), store(store), cursor(cursor), objv(objv)
  {}

  int operate() {
    reenter(this) {
      state.oldest_period_id = cursor.get_period().get_id();
      state.oldest_realm_epoch = cursor.get_epoch();

      yield {
        rgw_raw_obj obj{store->svc.zone->get_zone_params().log_pool,
                        RGWMetadataLogHistory::oid};

        using WriteCR = RGWSimpleRadosWriteCR<RGWMetadataLogHistory>;
        call(new WriteCR(store->get_async_rados(), store->svc.sysobj, obj, state, objv));
      }
      if (retcode < 0) {
        ldout(cct, 1) << "failed to write mdlog history: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }

      ldout(cct, 10) << "wrote mdlog history with oldest period id="
          << state.oldest_period_id << " realm_epoch="
          << state.oldest_realm_epoch << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

/// update the mdlog history to reflect trimmed logs
class TrimHistoryCR : public RGWCoroutine {
  RGWRados *store;
  const Cursor cursor; //< cursor to trimmed period
  RGWObjVersionTracker *objv; //< to prevent racing updates
  Cursor next; //< target cursor for oldest log period
  Cursor existing; //< existing cursor read from disk

 public:
  TrimHistoryCR(RGWRados *store, Cursor cursor, RGWObjVersionTracker *objv)
    : RGWCoroutine(store->ctx()),
      store(store), cursor(cursor), objv(objv), next(cursor)
  {
    next.next(); // advance past cursor
  }

  int operate() {
    reenter(this) {
      // read an existing history, and write the new history if it's newer
      yield call(new ReadHistoryCR(store, &existing, objv));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      // reject older trims with ECANCELED
      if (cursor.get_epoch() < existing.get_epoch()) {
        ldout(cct, 4) << "found oldest log epoch=" << existing.get_epoch()
            << ", rejecting trim at epoch=" << cursor.get_epoch() << dendl;
        return set_cr_error(-ECANCELED);
      }
      // overwrite with updated history
      yield call(new WriteHistoryCR(store, next, objv));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

// traverse all the way back to the beginning of the period history, and
// return a cursor to the first period in a fully attached history
Cursor find_oldest_period(RGWRados *store)
{
  auto cct = store->ctx();
  auto cursor = store->period_history->get_current();

  while (cursor) {
    // advance to the period's predecessor
    if (!cursor.has_prev()) {
      auto& predecessor = cursor.get_period().get_predecessor();
      if (predecessor.empty()) {
        // this is the first period, so our logs must start here
        ldout(cct, 10) << "find_oldest_period returning first "
            "period " << cursor.get_period().get_id() << dendl;
        return cursor;
      }
      // pull the predecessor and add it to our history
      RGWPeriod period;
      int r = store->period_puller->pull(predecessor, period);
      if (r < 0) {
        return Cursor{r};
      }
      auto prev = store->period_history->insert(std::move(period));
      if (!prev) {
        return prev;
      }
      ldout(cct, 20) << "find_oldest_period advancing to "
          "predecessor period " << predecessor << dendl;
      ceph_assert(cursor.has_prev());
    }
    cursor.prev();
  }
  ldout(cct, 10) << "find_oldest_period returning empty cursor" << dendl;
  return cursor;
}

} // anonymous namespace

Cursor RGWMetadataManager::init_oldest_log_period()
{
  // read the mdlog history
  RGWMetadataLogHistory state;
  RGWObjVersionTracker objv;
  int ret = read_history(store, &state, &objv);

  if (ret == -ENOENT) {
    // initialize the mdlog history and write it
    ldout(cct, 10) << "initializing mdlog history" << dendl;
    auto cursor = find_oldest_period(store);
    if (!cursor) {
      return cursor;
    }

    // write the initial history
    state.oldest_realm_epoch = cursor.get_epoch();
    state.oldest_period_id = cursor.get_period().get_id();

    constexpr bool exclusive = true; // don't overwrite
    int ret = write_history(store, state, &objv, exclusive);
    if (ret < 0 && ret != -EEXIST) {
      ldout(cct, 1) << "failed to write mdlog history: "
          << cpp_strerror(ret) << dendl;
      return Cursor{ret};
    }
    return cursor;
  } else if (ret < 0) {
    ldout(cct, 1) << "failed to read mdlog history: "
        << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }

  // if it's already in the history, return it
  auto cursor = store->period_history->lookup(state.oldest_realm_epoch);
  if (cursor) {
    return cursor;
  }
  // pull the oldest period by id
  RGWPeriod period;
  ret = store->period_puller->pull(state.oldest_period_id, period);
  if (ret < 0) {
    ldout(cct, 1) << "failed to read period id=" << state.oldest_period_id
        << " for mdlog history: " << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }
  // verify its realm_epoch
  if (period.get_realm_epoch() != state.oldest_realm_epoch) {
    ldout(cct, 1) << "inconsistent mdlog history: read period id="
        << period.get_id() << " with realm_epoch=" << period.get_realm_epoch()
        << ", expected realm_epoch=" << state.oldest_realm_epoch << dendl;
    return Cursor{-EINVAL};
  }
  // attach the period to our history
  return store->period_history->attach(std::move(period));
}

Cursor RGWMetadataManager::read_oldest_log_period() const
{
  RGWMetadataLogHistory state;
  int ret = read_history(store, &state, nullptr);
  if (ret < 0) {
    ldout(store->ctx(), 1) << "failed to read mdlog history: "
        << cpp_strerror(ret) << dendl;
    return Cursor{ret};
  }

  ldout(store->ctx(), 10) << "read mdlog history with oldest period id="
      << state.oldest_period_id << " realm_epoch="
      << state.oldest_realm_epoch << dendl;

  return store->period_history->lookup(state.oldest_realm_epoch);
}

RGWCoroutine* RGWMetadataManager::read_oldest_log_period_cr(Cursor *period,
        RGWObjVersionTracker *objv) const
{
  return new ReadHistoryCR(store, period, objv);
}

RGWCoroutine* RGWMetadataManager::trim_log_period_cr(Cursor period,
        RGWObjVersionTracker *objv) const
{
  return new TrimHistoryCR(store, period, objv);
}

int RGWMetadataManager::init(const std::string& current_period)
{
  // open a log for the current period
  current_log = get_log(current_period);
  return 0;
}

RGWMetadataLog* RGWMetadataManager::get_log(const std::string& period)
{
  // construct the period's log in place if it doesn't exist
  auto insert = md_logs.emplace(std::piecewise_construct,
                                std::forward_as_tuple(period),
                                std::forward_as_tuple(cct, store, period));
  return &insert.first->second;
}

int RGWMetadataManager::register_handler(RGWMetadataHandler *handler)
{
  string type = handler->get_type();

  if (handlers.find(type) != handlers.end())
    return -EINVAL;

  handlers[type] = handler;

  return 0;
}

RGWMetadataHandler *RGWMetadataManager::get_handler(const string& type)
{
  map<string, RGWMetadataHandler *>::iterator iter = handlers.find(type);
  if (iter == handlers.end())
    return NULL;

  return iter->second;
}

void RGWMetadataManager::parse_metadata_key(const string& metadata_key, string& type, string& entry)
{
  auto pos = metadata_key.find(':');
  if (pos == string::npos) {
    type = metadata_key;
  } else {
    type = metadata_key.substr(0, pos);
    entry = metadata_key.substr(pos + 1);
  }
}

int RGWMetadataManager::find_handler(const string& metadata_key, RGWMetadataHandler **handler, string& entry)
{
  string type;

  parse_metadata_key(metadata_key, type, entry);

  if (type.empty()) {
    *handler = &md_top_handler;
    return 0;
  }

  map<string, RGWMetadataHandler *>::iterator iter = handlers.find(type);
  if (iter == handlers.end())
    return -ENOENT;

  *handler = iter->second;

  return 0;

}

int RGWMetadataManager::get(string& metadata_key, Formatter *f)
{
  RGWMetadataHandler *handler;
  string entry;
  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  RGWMetadataObject *obj;

  ret = handler->get(store, entry, &obj);
  if (ret < 0) {
    return ret;
  }

  f->open_object_section("metadata_info");
  encode_json("key", metadata_key, f);
  encode_json("ver", obj->get_version(), f);
  real_time mtime = obj->get_mtime();
  if (!real_clock::is_zero(mtime)) {
    utime_t ut(mtime);
    encode_json("mtime", ut, f);
  }
  encode_json("data", *obj, f);
  f->close_section();

  delete obj;

  return 0;
}

int RGWMetadataManager::put(string& metadata_key, bufferlist& bl,
                            RGWMetadataHandler::sync_type_t sync_type,
                            obj_version *existing_version)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0)
    return ret;

  JSONParser parser;
  if (!parser.parse(bl.c_str(), bl.length())) {
    return -EINVAL;
  }

  RGWObjVersionTracker objv_tracker;

  obj_version *objv = &objv_tracker.write_version;

  utime_t mtime;

  try {
    JSONDecoder::decode_json("key", metadata_key, &parser);
    JSONDecoder::decode_json("ver", *objv, &parser);
    JSONDecoder::decode_json("mtime", mtime, &parser);
  } catch (JSONDecoder::err& e) {
    return -EINVAL;
  }

  JSONObj *jo = parser.find_obj("data");
  if (!jo) {
    return -EINVAL;
  }

  ret = handler->put(store, entry, objv_tracker, mtime.to_real_time(), jo, sync_type);
  if (existing_version) {
    *existing_version = objv_tracker.read_version;
  }
  return ret;
}

int RGWMetadataManager::prepare_mutate(RGWRados *store,
                                       rgw_pool& pool, const string& oid,
                                       const real_time& mtime,
                                       RGWObjVersionTracker *objv_tracker,
                                       RGWMetadataHandler::sync_type_t sync_mode)
{
  bufferlist bl;
  real_time orig_mtime;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  int ret = rgw_get_system_obj(store, obj_ctx, pool, oid,
                               bl, objv_tracker, &orig_mtime,
                               nullptr, nullptr);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  if (ret != -ENOENT &&
      !RGWMetadataHandler::check_versions(objv_tracker->read_version, orig_mtime,
                                          objv_tracker->write_version, mtime, sync_mode)) {
    return STATUS_NO_APPLY;
  }

  if (objv_tracker->write_version.tag.empty()) {
    if (objv_tracker->read_version.tag.empty()) {
      objv_tracker->generate_new_write_ver(store->ctx());
    } else {
      objv_tracker->write_version = objv_tracker->read_version;
      objv_tracker->write_version.ver++;
    }
  }
  return 0;
}

int RGWMetadataManager::remove(string& metadata_key)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0)
    return ret;

  RGWMetadataObject *obj;

  ret = handler->get(store, entry, &obj);
  if (ret < 0) {
    return ret;
  }

  RGWObjVersionTracker objv_tracker;

  objv_tracker.read_version = obj->get_version();

  delete obj;

  return handler->remove(store, entry, objv_tracker);
}

int RGWMetadataManager::lock_exclusive(string& metadata_key, timespan duration, string& owner_id) {
  RGWMetadataHandler *handler;
  string entry;
  string zone_id;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) 
    return ret;

  rgw_pool pool;
  string oid;

  handler->get_pool_and_oid(store, entry, pool, oid);

  return store->lock_exclusive(pool, oid, duration, zone_id, owner_id);  
}

int RGWMetadataManager::unlock(string& metadata_key, string& owner_id) {
  librados::IoCtx io_ctx;
  RGWMetadataHandler *handler;
  string entry;
  string zone_id;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) 
    return ret;

  rgw_pool pool;
  string oid;

  handler->get_pool_and_oid(store, entry, pool, oid);

  return store->unlock(pool, oid, zone_id, owner_id);  
}

struct list_keys_handle {
  void *handle;
  RGWMetadataHandler *handler;
};

int RGWMetadataManager::list_keys_init(const string& section, void **handle)
{
  return list_keys_init(section, string(), handle);
}

int RGWMetadataManager::list_keys_init(const string& section,
                                       const string& marker, void **handle)
{
  string entry;
  RGWMetadataHandler *handler;

  int ret;

  ret = find_handler(section, &handler, entry);
  if (ret < 0) {
    return -ENOENT;
  }

  list_keys_handle *h = new list_keys_handle;
  h->handler = handler;
  ret = handler->list_keys_init(store, marker, &h->handle);
  if (ret < 0) {
    delete h;
    return ret;
  }

  *handle = (void *)h;

  return 0;
}

int RGWMetadataManager::list_keys_next(void *handle, int max, list<string>& keys, bool *truncated)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  RGWMetadataHandler *handler = h->handler;

  return handler->list_keys_next(h->handle, max, keys, truncated);
}

void RGWMetadataManager::list_keys_complete(void *handle)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  RGWMetadataHandler *handler = h->handler;

  handler->list_keys_complete(h->handle);
  delete h;
}

string RGWMetadataManager::get_marker(void *handle)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  return h->handler->get_marker(h->handle);
}

void RGWMetadataManager::dump_log_entry(cls_log_entry& entry, Formatter *f)
{
  f->open_object_section("entry");
  f->dump_string("id", entry.id);
  f->dump_string("section", entry.section);
  f->dump_string("name", entry.name);
  entry.timestamp.gmtime_nsec(f->dump_stream("timestamp"));

  try {
    RGWMetadataLogData log_data;
    auto iter = entry.data.cbegin();
    decode(log_data, iter);

    encode_json("data", log_data, f);
  } catch (buffer::error& err) {
    lderr(cct) << "failed to decode log entry: " << entry.section << ":" << entry.name<< " ts=" << entry.timestamp << dendl;
  }
  f->close_section();
}

void RGWMetadataManager::get_sections(list<string>& sections)
{
  for (map<string, RGWMetadataHandler *>::iterator iter = handlers.begin(); iter != handlers.end(); ++iter) {
    sections.push_back(iter->first);
  }
}

int RGWMetadataManager::pre_modify(RGWMetadataHandler *handler, string& section, const string& key,
                                   RGWMetadataLogData& log_data, RGWObjVersionTracker *objv_tracker,
                                   RGWMDLogStatus op_type)
{
  section = handler->get_type();

  /* if write version has not been set, and there's a read version, set it so that we can
   * log it
   */
  if (objv_tracker) {
    if (objv_tracker->read_version.ver && !objv_tracker->write_version.ver) {
      objv_tracker->write_version = objv_tracker->read_version;
      objv_tracker->write_version.ver++;
    }
    log_data.read_version = objv_tracker->read_version;
    log_data.write_version = objv_tracker->write_version;
  }

  log_data.status = op_type;

  bufferlist logbl;
  encode(log_data, logbl);

  ceph_assert(current_log); // must have called init()
  int ret = current_log->add_entry(handler, section, key, logbl);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWMetadataManager::post_modify(RGWMetadataHandler *handler, const string& section, const string& key, RGWMetadataLogData& log_data,
                                    RGWObjVersionTracker *objv_tracker, int ret)
{
  if (ret >= 0)
    log_data.status = MDLOG_STATUS_COMPLETE;
  else 
    log_data.status = MDLOG_STATUS_ABORT;

  bufferlist logbl;
  encode(log_data, logbl);

  ceph_assert(current_log); // must have called init()
  int r = current_log->add_entry(handler, section, key, logbl);
  if (ret < 0)
    return ret;

  if (r < 0)
    return r;

  return 0;
}

string RGWMetadataManager::heap_oid(RGWMetadataHandler *handler, const string& key, const obj_version& objv)
{
  char buf[objv.tag.size() + 32];
  snprintf(buf, sizeof(buf), "%s:%lld", objv.tag.c_str(), (long long)objv.ver);
  return string(".meta:") + handler->get_type() + ":" + key + ":" + buf;
}

int RGWMetadataManager::store_in_heap(RGWMetadataHandler *handler, const string& key, bufferlist& bl,
                                      RGWObjVersionTracker *objv_tracker, real_time mtime,
				      map<string, bufferlist> *pattrs)
{
  if (!objv_tracker) {
    return -EINVAL;
  }

  rgw_pool heap_pool(store->svc.zone->get_zone_params().metadata_heap);

  if (heap_pool.empty()) {
    return 0;
  }

  RGWObjVersionTracker otracker;
  otracker.write_version = objv_tracker->write_version;
  string oid = heap_oid(handler, key, objv_tracker->write_version);
  int ret = rgw_put_system_obj(store, heap_pool, oid,
                               bl, false, &otracker, mtime, pattrs);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: rgw_put_system_obj() oid=" << oid << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWMetadataManager::remove_from_heap(RGWMetadataHandler *handler, const string& key, RGWObjVersionTracker *objv_tracker)
{
  if (!objv_tracker) {
    return -EINVAL;
  }

  rgw_pool heap_pool(store->svc.zone->get_zone_params().metadata_heap);

  if (heap_pool.empty()) {
    return 0;
  }

  string oid = heap_oid(handler, key, objv_tracker->write_version);
  rgw_raw_obj obj(heap_pool, oid);
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  int ret = sysobj.wop().remove();
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: sysobj.wop().remove() oid=" << oid << " returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWMetadataManager::put_entry(RGWMetadataHandler *handler, const string& key, bufferlist& bl, bool exclusive,
                                  RGWObjVersionTracker *objv_tracker, real_time mtime, map<string, bufferlist> *pattrs)
{
  string section;
  RGWMetadataLogData log_data;
  int ret = pre_modify(handler, section, key, log_data, objv_tracker, MDLOG_STATUS_WRITE);
  if (ret < 0)
    return ret;

  string oid;
  rgw_pool pool;

  handler->get_pool_and_oid(store, key, pool, oid);

  ret = store_in_heap(handler, key, bl, objv_tracker, mtime, pattrs);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": store_in_heap() key=" << key << " returned ret=" << ret << dendl;
    goto done;
  }

  ret = rgw_put_system_obj(store, pool, oid, bl, exclusive,
                           objv_tracker, mtime, pattrs);

  if (ret < 0) {
    int r = remove_from_heap(handler, key, objv_tracker);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": remove_from_heap() key=" << key << " returned ret=" << r << dendl;
    }
  }
done:
  /* cascading ret into post_modify() */

  ret = post_modify(handler, section, key, log_data, objv_tracker, ret);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWMetadataManager::remove_entry(RGWMetadataHandler *handler, string& key, RGWObjVersionTracker *objv_tracker)
{
  string section;
  RGWMetadataLogData log_data;
  int ret = pre_modify(handler, section, key, log_data, objv_tracker, MDLOG_STATUS_REMOVE);
  if (ret < 0)
    return ret;

  string oid;
  rgw_pool pool;

  handler->get_pool_and_oid(store, key, pool, oid);

  rgw_raw_obj obj(pool, oid);

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  ret = sysobj.wop()
              .set_objv_tracker(objv_tracker)
              .remove();
  /* cascading ret into post_modify() */

  ret = post_modify(handler, section, key, log_data, objv_tracker, ret);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWMetadataManager::get_log_shard_id(const string& section,
                                         const string& key, int *shard_id)
{
  RGWMetadataHandler *handler = get_handler(section);
  if (!handler) {
    return -EINVAL;
  }
  string hash_key;
  handler->get_hash_key(section, key, hash_key);
  *shard_id = store->key_to_shard_id(hash_key, cct->_conf->rgw_md_log_max_shards);
  return 0;
}
