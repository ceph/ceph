// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_json.h"
#include "rgw_metadata.h"
#include "cls/version/cls_version_types.h"

#include "rgw_rados.h"
#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

struct LogStatusDump {
  RGWMDLogStatus status;

  LogStatusDump(RGWMDLogStatus _status) : status(_status) {}
  void dump(Formatter *f) const {
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
};

struct RGWMetadataLogData {
  obj_version read_version;
  obj_version write_version;
  RGWMDLogStatus status;
  
  RGWMetadataLogData() : status(MDLOG_STATUS_UNKNOWN) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(read_version, bl);
    ::encode(write_version, bl);
    uint32_t s = (uint32_t)status;
    ::encode(s, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
     DECODE_START(1, bl);
     ::decode(read_version, bl);
     ::decode(write_version, bl);
     uint32_t s;
     ::decode(s, bl);
     status = (RGWMDLogStatus)s;
     DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const {
    encode_json("read_version", read_version, f);
    encode_json("write_version", write_version, f);
    encode_json("status", LogStatusDump(status), f);
  }
};
WRITE_CLASS_ENCODER(RGWMetadataLogData)


int RGWMetadataLog::add_entry(RGWRados *store, RGWMetadataHandler *handler, const string& section, const string& key, bufferlist& bl) {
  if (!store->need_to_log_metadata())
    return 0;

  string oid;

  string hash_key;
  handler->get_hash_key(section, key, hash_key);

  store->shard_name(prefix, cct->_conf->rgw_md_log_max_shards, hash_key, oid);
  utime_t now = ceph_clock_now(cct);
  return store->time_log_add(oid, now, section, key, bl);
}

void RGWMetadataLog::init_list_entries(int shard_id, utime_t& from_time, utime_t& end_time, 
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

  int ret = store->time_log_list(ctx->cur_oid, ctx->from_time, ctx->end_time,
				 max_entries, entries, ctx->marker,
				 last_marker, truncated);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

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
  info->last_update = header.max_time;

  return 0;
}

int RGWMetadataLog::trim(int shard_id, const utime_t& from_time, const utime_t& end_time,
                         const string& start_marker, const string& end_marker)
{
  string oid;
  get_shard_oid(shard_id, oid);

  int ret;

  ret = store->time_log_trim(oid, from_time, end_time, start_marker, end_marker);

  if (ret == -ENOENT)
    ret = 0;

  return ret;
}
  
int RGWMetadataLog::lock_exclusive(int shard_id, utime_t& duration, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return store->lock_exclusive(store->zone.log_pool, oid, duration, zone_id, owner_id);
}

int RGWMetadataLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return store->unlock(store->zone.log_pool, oid, zone_id, owner_id);
}

obj_version& RGWMetadataObject::get_version()
{
  return objv;
}

class RGWMetadataTopHandler : public RGWMetadataHandler {
  struct iter_data {
    list<string> sections;
    list<string>::iterator iter;
  };

public:
  RGWMetadataTopHandler() {}

  virtual string get_type() { return string(); }

  virtual int get(RGWRados *store, string& entry, RGWMetadataObject **obj) { return -ENOTSUP; }
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
                  time_t mtime, JSONObj *obj, sync_type_t sync_type) { return -ENOTSUP; }

  virtual void get_pool_and_oid(RGWRados *store, const string& key, rgw_bucket& bucket, string& oid) {}

  virtual int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) { return -ENOTSUP; }

  virtual int list_keys_init(RGWRados *store, void **phandle) {
    iter_data *data = new iter_data;
    store->meta_mgr->get_sections(data->sections);
    data->iter = data->sections.begin();

    *phandle = data;

    return 0;
  }
  virtual int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated)  {
    iter_data *data = static_cast<iter_data *>(handle);
    for (int i = 0; i < max && data->iter != data->sections.end(); ++i, ++(data->iter)) {
      keys.push_back(*data->iter);
    }

    *truncated = (data->iter != data->sections.end());

    return 0;
  }
  virtual void list_keys_complete(void *handle) {
    iter_data *data = static_cast<iter_data *>(handle);

    delete data;
  }
};

static RGWMetadataTopHandler md_top_handler;

RGWMetadataManager::RGWMetadataManager(CephContext *_cct, RGWRados *_store) : cct(_cct), store(_store)
{
  md_log = new RGWMetadataLog(_cct, _store);
}

RGWMetadataManager::~RGWMetadataManager()
{
  map<string, RGWMetadataHandler *>::iterator iter;

  for (iter = handlers.begin(); iter != handlers.end(); ++iter) {
    delete iter->second;
  }

  handlers.clear();
  delete md_log;
}

int RGWMetadataManager::register_handler(RGWMetadataHandler *handler)
{
  string type = handler->get_type();

  if (handlers.find(type) != handlers.end())
    return -EINVAL;

  handlers[type] = handler;

  return 0;
}

RGWMetadataHandler *RGWMetadataManager::get_handler(const char *type)
{
  map<string, RGWMetadataHandler *>::iterator iter = handlers.find(type);
  if (iter == handlers.end())
    return NULL;

  return iter->second;
}

void RGWMetadataManager::parse_metadata_key(const string& metadata_key, string& type, string& entry)
{
  int pos = metadata_key.find(':');
  if (pos < 0) {
    type = metadata_key;
  }

  type = metadata_key.substr(0, pos);
  entry = metadata_key.substr(pos + 1);
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
  time_t mtime = obj->get_mtime();
  if (mtime > 0) {
    encode_json("mtime", mtime, f);
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

  time_t mtime = 0;

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

  ret = handler->put(store, entry, objv_tracker, mtime, jo, sync_type);
  if (existing_version) {
    *existing_version = objv_tracker.read_version;
  }
  return ret;
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

int RGWMetadataManager::lock_exclusive(string& metadata_key, utime_t duration, string& owner_id) {
  RGWMetadataHandler *handler;
  string entry;
  string zone_id;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) 
    return ret;

  rgw_bucket pool;
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

  rgw_bucket pool;
  string oid;

  handler->get_pool_and_oid(store, entry, pool, oid);

  return store->unlock(pool, oid, zone_id, owner_id);  
}

struct list_keys_handle {
  void *handle;
  RGWMetadataHandler *handler;
};


int RGWMetadataManager::list_keys_init(string& section, void **handle)
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
  ret = handler->list_keys_init(store, &h->handle);
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

void RGWMetadataManager::dump_log_entry(cls_log_entry& entry, Formatter *f)
{
  f->open_object_section("entry");
  f->dump_string("id", entry.id);
  f->dump_string("section", entry.section);
  f->dump_string("name", entry.name);
  entry.timestamp.gmtime(f->dump_stream("timestamp"));

  try {
    RGWMetadataLogData log_data;
    bufferlist::iterator iter = entry.data.begin();
    ::decode(log_data, iter);

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
  ::encode(log_data, logbl);

  int ret = md_log->add_entry(store, handler, section, key, logbl);
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
  ::encode(log_data, logbl);

  int r = md_log->add_entry(store, handler, section, key, logbl);
  if (ret < 0)
    return ret;

  if (r < 0)
    return r;

  return 0;
}

int RGWMetadataManager::put_entry(RGWMetadataHandler *handler, const string& key, bufferlist& bl, bool exclusive,
                                  RGWObjVersionTracker *objv_tracker, time_t mtime, map<string, bufferlist> *pattrs)
{
  string section;
  RGWMetadataLogData log_data;
  int ret = pre_modify(handler, section, key, log_data, objv_tracker, MDLOG_STATUS_WRITE);
  if (ret < 0)
    return ret;

  string oid;
  rgw_bucket bucket;

  handler->get_pool_and_oid(store, key, bucket, oid);

  ret = rgw_put_system_obj(store, bucket, oid,
                           bl.c_str(), bl.length(), exclusive,
                           objv_tracker, mtime, pattrs);
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
  rgw_bucket bucket;

  handler->get_pool_and_oid(store, key, bucket, oid);

  rgw_obj obj(bucket, oid);

  ret = store->delete_system_obj(obj, objv_tracker);
  /* cascading ret into post_modify() */

  ret = post_modify(handler, section, key, log_data, objv_tracker, ret);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWMetadataManager::set_attrs(RGWMetadataHandler *handler, string& key,
                                  rgw_obj& obj, map<string, bufferlist>& attrs,
                                  map<string, bufferlist>* rmattrs,
                                  RGWObjVersionTracker *objv_tracker)
{
  string section;
  RGWMetadataLogData log_data;
  int ret = pre_modify(handler, section, key, log_data, objv_tracker, MDLOG_STATUS_SETATTRS);
  if (ret < 0)
    return ret;

  ret = store->set_attrs(NULL, obj, attrs, rmattrs, objv_tracker);
  /* cascading ret into post_modify() */

  ret = post_modify(handler, section, key, log_data, objv_tracker, ret);
  if (ret < 0)
    return ret;

  return 0;
}
