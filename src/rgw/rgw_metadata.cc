

#include "rgw_metadata.h"
#include "common/ceph_json.h"
#include "cls/version/cls_version_types.h"

#include "rgw_rados.h"

int RGWMetadataLog::add_entry(RGWRados *store, string& section, string& key, bufferlist& bl) {
  string oid;

  store->shard_name(prefix, cct->_conf->rgw_md_log_max_shards, section, key, oid);
  utime_t now = ceph_clock_now(cct);
  return store->time_log_add(oid, now, section, key, bl);
}

void RGWMetadataLog::init_list_entries(RGWRados *store, utime_t& from_time, utime_t& end_time, void **handle)
{
  LogListCtx *ctx = new LogListCtx(store);

  ctx->from_time = from_time;
  ctx->end_time = end_time;

  get_shard_oid(0, ctx->cur_oid);

  *handle = (void *)ctx;
}

void RGWMetadataLog::complete_list_entries(void *handle) {
  LogListCtx *ctx = (LogListCtx *)handle;
  delete ctx;
}

int RGWMetadataLog::list_entries(void *handle,
                 int max_entries,
                 list<cls_log_entry>& entries,
                 bool *truncated) {
  LogListCtx *ctx = (LogListCtx *)handle;

  if (ctx->done || !max_entries) {
    *truncated = false;
    return 0;
  }

  entries.clear();

  do {
    list<cls_log_entry> ents;
    bool is_truncated;
    int ret = store->time_log_list(ctx->cur_oid, ctx->from_time, ctx->end_time,
                                 max_entries - entries.size(), ents, ctx->marker, &is_truncated);
    if (ret == -ENOENT) {
      is_truncated = false;
      ret = 0;
    }
    if (ret < 0)
      return ret;

    if (ents.size()) {
      entries.splice(entries.end(), ents);
    }

    if (!is_truncated) {
      ++ctx->cur_shard;
      if (ctx->cur_shard <cct->_conf->rgw_md_log_max_shards) {
        get_shard_oid(ctx->cur_shard, ctx->cur_oid);
        ctx->marker.clear();
      } else {
        ctx->done = true;
        break;
      }
    }
  } while (entries.size() < (size_t)max_entries);

  *truncated = !ctx->done;

  return 0;
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
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker, JSONObj *obj) { return -ENOTSUP; }
  virtual int put_obj(RGWRados *store, string& key, bufferlist& bl, bool exclusive,
                      RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs) { return -ENOTSUP; }

  virtual int list_keys_init(RGWRados *store, void **phandle) {
    iter_data *data = new iter_data;
    store->meta_mgr->get_sections(data->sections);
    data->iter = data->sections.begin();

    *phandle = data;

    return 0;
  }
  virtual int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated)  {
    iter_data *data = (iter_data *)handle;
    for (int i = 0; i < max && data->iter != data->sections.end(); ++i, ++(data->iter)) {
      keys.push_back(*data->iter);
    }

    *truncated = (data->iter != data->sections.end());

    return 0;
  }
  virtual void list_keys_complete(void *handle) {
    iter_data *data = (iter_data *)handle;

    delete data;
  }
};

static RGWMetadataTopHandler md_top_handler;

RGWMetadataManager::RGWMetadataManager(CephContext *_cct, RGWRados *_store) : store(_store)
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
  encode_json("data", *obj, f);
  f->close_section();

  delete obj;

  return 0;
}

int RGWMetadataManager::put(string& metadata_key, bufferlist& bl)
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

  string meadata_key;
  RGWObjVersionTracker objv_tracker;

  obj_version *objv = &objv_tracker.write_version;


  JSONDecoder::decode_json("key", metadata_key, &parser);
  JSONDecoder::decode_json("ver", *objv, &parser);

  JSONObj *jo = parser.find_obj("data");
  if (!jo) {
    return -EINVAL;
  }

  return handler->put(store, entry, objv_tracker, jo);
}


struct list_keys_handle {
  void *handle;
  RGWMetadataHandler *handler;
};


int RGWMetadataManager::list_keys_init(string& section, void **handle)
{
  string entry;
  RGWMetadataHandler *handler;
  int ret = find_handler(section, &handler, entry);
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
  list_keys_handle *h = (list_keys_handle *)handle;

  RGWMetadataHandler *handler = h->handler;

  return handler->list_keys_next(h->handle, max, keys, truncated);
}


void RGWMetadataManager::list_keys_complete(void *handle)
{
  list_keys_handle *h = (list_keys_handle *)handle;

  RGWMetadataHandler *handler = h->handler;

  handler->list_keys_complete(h->handle);
  delete h;
}

void RGWMetadataManager::get_sections(list<string>& sections)
{
  for (map<string, RGWMetadataHandler *>::iterator iter = handlers.begin(); iter != handlers.end(); ++iter) {
    sections.push_back(iter->first);
  }
}


int RGWMetadataManager::put_obj(RGWMetadataHandler *handler, string& key, bufferlist& bl, bool exclusive,
                                RGWObjVersionTracker *objv_tracker, map<string, bufferlist> *pattrs)
{
  bufferlist logbl;
  string section = handler->get_type();
  int ret = md_log->add_entry(store, section, key, logbl);
  if (ret < 0)
    return ret;

  ret = handler->put_obj(store, key, bl, exclusive, objv_tracker, pattrs);
  if (ret < 0)
    return ret;

  ret = md_log->add_entry(store, section, key, logbl);
  if (ret < 0)
    return ret;

  return 0;
}

