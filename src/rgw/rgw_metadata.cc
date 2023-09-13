// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_metadata.h"

#include "rgw_mdlog.h"


#include "services/svc_meta.h"
#include "services/svc_meta_be_sobj.h"

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

RGWMetadataHandler_GenericMetaBE::Put::Put(RGWMetadataHandler_GenericMetaBE *_handler,
					   RGWSI_MetaBackend_Handler::Op *_op,
					   string& _entry, RGWMetadataObject *_obj,
					   RGWObjVersionTracker& _objv_tracker,
					   optional_yield _y,
					   RGWMDLogSyncType _type, bool _from_remote_zone):
  handler(_handler), op(_op),
  entry(_entry), obj(_obj),
  objv_tracker(_objv_tracker),
  apply_type(_type),
  y(_y),
  from_remote_zone(_from_remote_zone)
{
}

int RGWMetadataHandler_GenericMetaBE::do_put_operate(Put *put_op, const DoutPrefixProvider *dpp)
{
  int r = put_op->put_pre(dpp);
  if (r != 0) { /* r can also be STATUS_NO_APPLY */
    return r;
  }

  r = put_op->put(dpp);
  if (r != 0) {
    return r;
  }

  r = put_op->put_post(dpp);
  if (r != 0) {  /* e.g., -error or STATUS_APPLIED */
    return r;
  }

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::get(string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_get(op, entry, obj, y, dpp);
  });
}

int RGWMetadataHandler_GenericMetaBE::put(string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
                                          optional_yield y, const DoutPrefixProvider *dpp, RGWMDLogSyncType type, bool from_remote_zone)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_put(op, entry, obj, objv_tracker, y, dpp, type, from_remote_zone);
  });
}

int RGWMetadataHandler_GenericMetaBE::remove(string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_remove(op, entry, objv_tracker, y, dpp);
  });
}

int RGWMetadataHandler_GenericMetaBE::mutate(const string& entry,
                                             const ceph::real_time& mtime,
                                             RGWObjVersionTracker *objv_tracker,
                                             optional_yield y,
                                             const DoutPrefixProvider *dpp,
                                             RGWMDLogStatus op_type,
                                             std::function<int()> f)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    RGWSI_MetaBackend::MutateParams params(mtime, op_type);
    return op->mutate(entry,
                      params,
                      objv_tracker,
		      y,
                      f,
                      dpp);
  });
}

int RGWMetadataHandler_GenericMetaBE::get_shard_id(const string& entry, int *shard_id)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return op->get_shard_id(entry, shard_id);
  });
}

int RGWMetadataHandler_GenericMetaBE::list_keys_init(const DoutPrefixProvider *dpp, const string& marker, void **phandle)
{
  auto op = std::make_unique<RGWSI_MetaBackend_Handler::Op_ManagedCtx>(be_handler);

  int ret = op->list_init(dpp, marker);
  if (ret < 0) {
    return ret;
  }

  *phandle = (void *)op.release();

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, list<string>& keys, bool *truncated)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);

  int ret = op->list_next(dpp, max, &keys, truncated);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  if (ret == -ENOENT) {
    if (truncated) {
      *truncated = false;
    }
    return 0;
  }

  return 0;
}

void RGWMetadataHandler_GenericMetaBE::list_keys_complete(void *handle)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);
  delete op;
}

string RGWMetadataHandler_GenericMetaBE::get_marker(void *handle)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);
  string marker;
  int r = op->list_get_marker(&marker);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): list_get_marker() returned: r=" << r << dendl;
    /* not much else to do */
  }

  return marker;
}

RGWMetadataHandlerPut_SObj::RGWMetadataHandlerPut_SObj(RGWMetadataHandler_GenericMetaBE *handler,
                                                       RGWSI_MetaBackend_Handler::Op *op,
                                                       string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
						       optional_yield y,
                                                       RGWMDLogSyncType type, bool from_remote_zone) : Put(handler, op, entry, obj, objv_tracker, y, type, from_remote_zone) {
}

int RGWMetadataHandlerPut_SObj::put_pre(const DoutPrefixProvider *dpp)
{
  int ret = get(&old_obj, dpp);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  exists = (ret != -ENOENT);

  oo.reset(old_obj);

  auto old_ver = (!old_obj ? obj_version() : old_obj->get_version());
  auto old_mtime = (!old_obj ? ceph::real_time() : old_obj->get_mtime());

  // are we actually going to perform this put, or is it too old?
  if (!handler->check_versions(exists, old_ver, old_mtime,
                               objv_tracker.write_version, obj->get_mtime(),
                               apply_type)) {
    return STATUS_NO_APPLY;
  }

  objv_tracker.read_version = old_ver; /* maintain the obj version we just read */

  return 0;
}

int RGWMetadataHandlerPut_SObj::put(const DoutPrefixProvider *dpp)
{
  int ret = put_check(dpp);
  if (ret != 0) {
    return ret;
  }

  return put_checked(dpp);
}

int RGWMetadataHandlerPut_SObj::put_checked(const DoutPrefixProvider *dpp)
{
  RGWSI_MBSObj_PutParams params(obj->get_pattrs(), obj->get_mtime());

  encode_obj(&params.bl);

  int ret = op->put(entry, params, &objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

class RGWMetadataTopHandler : public RGWMetadataHandler {
  struct iter_data {
    set<string> sections;
    set<string>::iterator iter;
  };

  struct Svc {
    RGWSI_Meta *meta{nullptr};
  } svc;

  RGWMetadataManager *mgr;

public:
  RGWMetadataTopHandler(RGWSI_Meta *meta_svc,
                        RGWMetadataManager *_mgr) : mgr(_mgr) {
    base_init(meta_svc->ctx());
    svc.meta = meta_svc;
  }

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

RGWMetadataHandlerPut_SObj::~RGWMetadataHandlerPut_SObj() {}

int RGWMetadataHandler::attach(RGWMetadataManager *manager)
{
  return manager->register_handler(this);
}

RGWMetadataHandler::~RGWMetadataHandler() {}

obj_version& RGWMetadataObject::get_version()
{
  return objv;
}

RGWMetadataManager::RGWMetadataManager(RGWSI_Meta *_meta_svc)
  : cct(_meta_svc->ctx()), meta_svc(_meta_svc)
{
  md_top_handler.reset(new RGWMetadataTopHandler(meta_svc, this));
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

