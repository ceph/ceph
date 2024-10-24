// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_metadata.h"

#include "rgw_mdlog.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

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

void encode_json(const char *name, const obj_version& v, Formatter *f)
{
  f->open_object_section(name);
  f->dump_string("tag", v.tag);
  f->dump_unsigned("ver", v.ver);
  f->close_section();
}

void decode_json_obj(obj_version& v, JSONObj *obj)
{
  JSONDecoder::decode_json("tag", v.tag, obj);
  JSONDecoder::decode_json("ver", v.ver, obj);
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

void RGWMetadataLogData::generate_test_instances(std::list<RGWMetadataLogData *>& l) {
  l.push_back(new RGWMetadataLogData{});
  l.push_back(new RGWMetadataLogData);
  l.back()->read_version = obj_version();
  l.back()->read_version.tag = "read_tag";
  l.back()->write_version = obj_version();
  l.back()->write_version.tag = "write_tag";
  l.back()->status = MDLOG_STATUS_WRITE;
}

class RGWMetadataTopHandler : public RGWMetadataHandler {
  struct iter_data {
    set<string> sections;
    set<string>::iterator iter;
  };

  RGWMetadataManager *mgr;

public:
  explicit RGWMetadataTopHandler(RGWMetadataManager *_mgr) : mgr(_mgr) {}

  string get_type() override { return string(); }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) {
    return new RGWMetadataObject;
  }

  int get(string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) override {
    return -ENOTSUP;
  }

  int put(string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider *dpp, RGWMDLogSyncType type, bool from_remote_zone) override {
    return -ENOTSUP;
  }

  int remove(string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp) override {
    return -ENOTSUP;
  }

  int mutate(const string& entry,
             const ceph::real_time& mtime,
             RGWObjVersionTracker *objv_tracker,
             optional_yield y,
             const DoutPrefixProvider *dpp,
             RGWMDLogStatus op_type,
             std::function<int()> f) {
    return -ENOTSUP;
  }

  int list_keys_init(const DoutPrefixProvider *dpp, const string& marker, void **phandle) override {
    iter_data *data = new iter_data;
    list<string> sections;
    mgr->get_sections(sections);
    for (auto& s : sections) {
      data->sections.insert(s);
    }
    data->iter = data->sections.lower_bound(marker);

    *phandle = data;

    return 0;
  }
  int list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, list<string>& keys, bool *truncated) override  {
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

int RGWMetadataHandler::attach(RGWMetadataManager *manager)
{
  return manager->register_handler(this);
}

RGWMetadataHandler::~RGWMetadataHandler() {}

obj_version& RGWMetadataObject::get_version()
{
  return objv;
}

RGWMetadataManager::RGWMetadataManager()
{
  md_top_handler.reset(new RGWMetadataTopHandler(this));
}

RGWMetadataManager::~RGWMetadataManager()
{
}

int RGWMetadataManager::register_handler(RGWMetadataHandler *handler)
{
  string type = handler->get_type();

  if (handlers.find(type) != handlers.end())
    return -EEXIST;

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

int RGWMetadataManager::get(string& metadata_key, Formatter *f, optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWMetadataHandler *handler;
  string entry;
  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  RGWMetadataObject *obj;

  ret = handler->get(entry, &obj, y, dpp);
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
                            optional_yield y,
                            const DoutPrefixProvider *dpp,
                            RGWMDLogSyncType sync_type,
                            bool from_remote_zone,
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

  ret = handler->put(entry, obj, objv_tracker, y, dpp, sync_type, from_remote_zone);
  if (existing_version) {
    *existing_version = objv_tracker.read_version;
  }

  delete obj;

  return ret;
}

int RGWMetadataManager::remove(string& metadata_key, optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  RGWMetadataObject *obj;
  ret = handler->get(entry, &obj, y, dpp);
  if (ret < 0) {
    return ret;
  }
  RGWObjVersionTracker objv_tracker;
  objv_tracker.read_version = obj->get_version();
  delete obj;

  return handler->remove(entry, objv_tracker, y, dpp);
}

int RGWMetadataManager::mutate(const string& metadata_key,
                               const ceph::real_time& mtime,
                               RGWObjVersionTracker *objv_tracker,
                               optional_yield y,
                               const DoutPrefixProvider *dpp,
                               RGWMDLogStatus op_type,
                               std::function<int()> f)
{
  RGWMetadataHandler *handler;
  string entry;

  int ret = find_handler(metadata_key, &handler, entry);
  if (ret < 0) {
    return ret;
  }

  return handler->mutate(entry, mtime, objv_tracker, y, dpp, op_type, f);
}

int RGWMetadataManager::get_shard_id(const string& section, const string& entry, int *shard_id)
{
  RGWMetadataHandler *handler = get_handler(section);
  if (!handler) {
    return -EINVAL;
  }

  return handler->get_shard_id(entry, shard_id);
}

struct list_keys_handle {
  void *handle;
  RGWMetadataHandler *handler;
};

int RGWMetadataManager::list_keys_init(const DoutPrefixProvider *dpp, const string& section, void **handle)
{
  return list_keys_init(dpp, section, string(), handle);
}

int RGWMetadataManager::list_keys_init(const DoutPrefixProvider *dpp, const string& section,
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
  ret = handler->list_keys_init(dpp, marker, &h->handle);
  if (ret < 0) {
    delete h;
    return ret;
  }

  *handle = (void *)h;

  return 0;
}

int RGWMetadataManager::list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, list<string>& keys, bool *truncated)
{
  list_keys_handle *h = static_cast<list_keys_handle *>(handle);

  RGWMetadataHandler *handler = h->handler;

  return handler->list_keys_next(dpp, h->handle, max, keys, truncated);
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

void RGWMetadataManager::dump_log_entry(cls::log::entry& entry, Formatter *f)
{
  f->open_object_section("entry");
  f->dump_string("id", entry.id);
  f->dump_string("section", entry.section);
  f->dump_string("name", entry.name);
  utime_t ts(entry.timestamp);
  ts.gmtime_nsec(f->dump_stream("timestamp"));

  try {
    RGWMetadataLogData log_data;
    auto iter = entry.data.cbegin();
    decode(log_data, iter);

    encode_json("data", log_data, f);
  } catch (buffer::error& err) {
  }
  f->close_section();
}

void RGWMetadataManager::get_sections(list<string>& sections)
{
  for (map<string, RGWMetadataHandler *>::iterator iter = handlers.begin(); iter != handlers.end(); ++iter) {
    sections.push_back(iter->first);
  }
}

