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
#include "rgw_mdlog.h"

#include "rgw_cr_rados.h"

#include "services/svc_zone.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"

#include "include/ceph_assert.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

const std::string RGWMetadataLogHistory::oid = "meta.history";

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


int RGWMetadataLog::add_entry(RGWSI_MetaBackend::Module *module, const string& section, const string& key, bufferlist& bl) {
  if (!store->svc.zone->need_to_log_metadata())
    return 0;

  string oid;

  string hash_key;
  module->get_hash_key(section, key, hash_key);

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

  class HandlerModule : public RGWSI_MetaBackend::Module {
  public:
    void get_pool_and_oid(const string& key, rgw_pool& pool, string& oid) {}
    void key_to_oid(string& key) {}
    void oid_to_key(string& oid) {}
  };

  struct Svc {
    RGWSI_Meta *meta{nullptr};
  } svc;

public:
  RGWMetadataTopHandler(RGWSI_Meta *meta_svc) {
    svc.meta = meta_svc;
  }

  string get_type() override { return string(); }

  int init_module() override {
    be_module.reset(new HandlerModule());
    return 0;
  }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) {
    return new RGWMetadataObject;
  }

  int do_get(RGWSI_MetaBackend::Context *ctx, string& entry, RGWMetadataObject **obj, optional_yield y) override { return -ENOTSUP; }
  int do_put(RGWSI_MetaBackend::Context *ctx, string& entry, RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker, optional_yield y, RGWMDLogSyncType type) override { return -ENOTSUP; }
  int do_remove(RGWSI_MetaBackend::Context *ctx, string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y) override { return -ENOTSUP; }

  int list_keys_init(const string& marker, void **phandle) override {
    iter_data *data = new iter_data;
    list<string> sections;
    svc.meta->get_mgr()->get_sections(sections);
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

RGWMetadataManager::RGWMetadataManager(RGWSI_Meta *_meta_svc)
  : cct(_meta_svc->ctx()), meta_svc(_meta_svc)
{
  md_top_handler.reset(new RGWMetadataTopHandler(meta_svc));
}

RGWMetadataManager::~RGWMetadataManager()
{
  map<string, RGWMetadataHandler *>::iterator iter;

  for (iter = handlers.begin(); iter != handlers.end(); ++iter) {
    delete iter->second;
  }

  handlers.clear();
}

int RGWMetadataHandler::init(RGWMetadataManager *manager)
{
  int r = init_module();
  if (r < 0) {
    return r;
  }

  return manager->register_handler(this, &meta_be, &be_handle);
}

RGWMetadataHandler::Put::Put(RGWMetadataHandler *handler, RGWSI_MetaBackend::Context *_ctx,
                             string& _entry, RGWMetadataObject *_obj,
                             RGWObjVersionTracker& _objv_tracker,
                             optional_yield _y,
                             RGWMDLogSyncType _type) :
  ctx(_ctx), entry(_entry),
  obj(_obj), objv_tracker(_objv_tracker),
  apply_type(_type), y(_y)
{
  meta_be = handler->get_meta_be();
}

int RGWMetadataHandlerPut_SObj::put()
{
  RGWMetadataObject *old_obj{nullptr};
#warning what about attrs?
  map<string, bufferlist> attrs;
  int ret = get(&old_obj);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }

  std::unique_ptr<RGWMetadataObject> oo(old_obj);

  auto old_ver = (!old_obj ? obj_version() : old_obj->get_version());
  auto old_mtime = (!old_obj ? ceph::real_time() : old_obj->get_mtime());

  // are we actually going to perform this put, or is it too old?
  bool exists = (ret != -ENOENT);
  if (!handler->check_versions(exists, old_ver, old_mtime,
                               objv_tracker.write_version, obj->get_mtime(),
                               apply_type)) {
    return STATUS_NO_APPLY;
  }

  objv_tracker.read_version = old_ver; /* maintain the obj version we just read */

  return put_checked(old_obj);
}

int RGWMetadataHandlerPut_SObj::put_checked(RGWMetadataObject *_old_obj)
{
  RGWSI_MBSObj_PutParams params(obj->get_pattrs(), obj->get_mtime());

  encode_obj(&params.bl);
  int ret = meta_be->put_entry(ctx, params,
                               &objv_tracker,
                               y);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWMetadataHandler::do_put_operate(Put *put_op)
{
  int r = put_op->put_pre();
  if (r != 0) { /* r can also be STATUS_NO_APPLY */
    return r;
  }

  r = put_op->put();
  if (r != 0) {
    return r;
  }

  r = put_op->put_post();
  if (r != 0) {  /* e.g., -error or STATUS_APPLIED */
    return r;
  }

  return 0;
}

int RGWMetadataHandler::call_with_ctx(std::function<int(RGWSI_MetaBackend::Context *ctx)> f)
{
  RGWSI_Meta_Ctx ctx;
  meta_be->init_ctx(be_handle, &ctx);
  return f(ctx.get());
}

int RGWMetadataHandler::call_with_ctx(const string& entry, std::function<int(RGWSI_MetaBackend::Context *ctx)> f)
{
  RGWSI_Meta_Ctx ctx;
  meta_be->init_ctx(be_handle, &ctx);
  ctx->set_key(entry);
  return f(ctx.get());
}

int RGWMetadataHandler::get(string& entry, RGWMetadataObject **obj, optional_yield y)
{
  return call_with_ctx(entry, [&](RGWSI_MetaBackend::Context *ctx) {
    return do_get(ctx, entry, obj, y);
  });
}

int RGWMetadataHandler::put(string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
                            optional_yield y, RGWMDLogSyncType type)
{
  return call_with_ctx(entry, [&](RGWSI_MetaBackend::Context *ctx) {
    return do_put(ctx, entry, obj, objv_tracker, y, type);
  });
}

int RGWMetadataHandler::remove(string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y)
{
  return call_with_ctx(entry, [&](RGWSI_MetaBackend::Context *ctx) {
    return do_remove(ctx, entry, objv_tracker, y);
  });
}

int RGWMetadataManager::register_handler(RGWMetadataHandler *handler, RGWSI_MetaBackend **pmeta_be, RGWSI_MetaBackend_Handle *phandle)
{
  string type = handler->get_type();

  if (handlers.find(type) != handlers.end())
    return -EINVAL;

  int ret = meta_svc->init_handler(handler, pmeta_be, phandle);
  if (ret < 0) {
    return ret;
  }

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
    *handler = md_top_handler.get();
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

  ret = handler->get(entry, &obj);
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
                            RGWMDLogSyncType sync_type,
                            obj_version *existing_version)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

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

  RGWMetadataObject *obj = handler->get_meta_obj(jo, *objv, mtime.to_real_time());
  if (!obj) {
    return -EINVAL;
  }

  ret = handler->put(entry, obj, objv_tracker, sync_type);
  if (existing_version) {
    *existing_version = objv_tracker.read_version;
  }

  delete obj;

  return ret;
}

int RGWMetadataManager::remove(string& metadata_key)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  RGWMetadataObject *obj;
  ret = handler->get(entry, &obj);
  if (ret < 0) {
    return ret;
  }
  RGWObjVersionTracker objv_tracker;
  objv_tracker.read_version = obj->get_version();
  delete obj;

  return handler->remove(entry, objv_tracker);
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
  ret = handler->list_keys_init(marker, &h->handle);
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

int RGWMetadataManager::get_log_shard_id(const string& section,
                                         const string& key, int *shard_id)
{
  RGWMetadataHandler *handler = get_handler(section);
  if (!handler) {
    return -EINVAL;
  }
  string hash_key;
  auto& module = handler->get_be_module();
  module->get_hash_key(section, key, hash_key);
  *shard_id = rgw_shard_id(hash_key, cct->_conf->rgw_md_log_max_shards);
  return 0;
}
