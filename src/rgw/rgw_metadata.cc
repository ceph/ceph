// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/intrusive_ptr.hpp>
#include "common/ceph_json.h"
#include "rgw_metadata.h"
#include "rgw_coroutine.h"
#include "cls/version/cls_version_types.h"

#include "rgw_rados.h"
#include "rgw_tools.h"

#include "rgw_cr_rados.h"
#include "rgw_boost_asio_yield.h"

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
  ::encode(read_version, bl);
  ::encode(write_version, bl);
  uint32_t s = (uint32_t)status;
  ::encode(s, bl);
  ENCODE_FINISH(bl);
}

void RGWMetadataLogData::decode(bufferlist::iterator& bl) {
   DECODE_START(1, bl);
   ::decode(read_version, bl);
   ::decode(write_version, bl);
   uint32_t s;
   ::decode(s, bl);
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
  if (!store->need_to_log_metadata())
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
  info->last_update = header.max_time.to_real_time();

  return 0;
}

static void _mdlog_info_completion(librados::completion_t cb, void *arg);

class RGWMetadataLogInfoCompletion : public RefCountedObject {
  RGWMetadataLogInfo *pinfo;
  RGWCompletionManager *completion_manager;
  void *user_info;
  int *pret;
  cls_log_header header;
  librados::IoCtx io_ctx;
  librados::AioCompletion *completion;

public:
  RGWMetadataLogInfoCompletion(RGWMetadataLogInfo *_pinfo, RGWCompletionManager *_cm, void *_uinfo, int *_pret) :
                                               pinfo(_pinfo), completion_manager(_cm), user_info(_uinfo), pret(_pret) {
    completion = librados::Rados::aio_create_completion((void *)this, _mdlog_info_completion, NULL);
  }

  ~RGWMetadataLogInfoCompletion() {
    completion->release();
  }

  void finish(librados::completion_t cb) {
    *pret = completion->get_return_value();
    if (*pret >= 0) {
      pinfo->marker = header.max_marker;
      pinfo->last_update = header.max_time.to_real_time();
    }
    completion_manager->complete(NULL, user_info);
    put();
  }

  librados::IoCtx& get_io_ctx() { return io_ctx; }

  cls_log_header *get_header() {
    return &header;
  }

  librados::AioCompletion *get_completion() {
    return completion;
  }
};

static void _mdlog_info_completion(librados::completion_t cb, void *arg)
{
  RGWMetadataLogInfoCompletion *infoc = (RGWMetadataLogInfoCompletion *)arg;
  infoc->finish(cb);
}

int RGWMetadataLog::get_info_async(int shard_id, RGWMetadataLogInfo *info, RGWCompletionManager *completion_manager, void *user_info, int *pret)
{
  string oid;
  get_shard_oid(shard_id, oid);

  RGWMetadataLogInfoCompletion *req_completion = new RGWMetadataLogInfoCompletion(info, completion_manager, user_info, pret);

  req_completion->get();

  int ret = store->time_log_info_async(req_completion->get_io_ctx(), oid, req_completion->get_header(), req_completion->get_completion());
  if (ret < 0) {
    return ret;
  }

  req_completion->put();

  return 0;
}

int RGWMetadataLog::trim(int shard_id, const real_time& from_time, const real_time& end_time,
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
  
int RGWMetadataLog::lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return store->lock_exclusive(store->get_zone_params().log_pool, oid, duration, zone_id, owner_id);
}

int RGWMetadataLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return store->unlock(store->get_zone_params().log_pool, oid, zone_id, owner_id);
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
    list<string> sections;
    list<string>::iterator iter;
  };

public:
  RGWMetadataTopHandler() {}

  virtual string get_type() { return string(); }

  virtual int get(RGWRados *store, string& entry, RGWMetadataObject **obj) { return -ENOTSUP; }
  virtual int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
                  real_time mtime, JSONObj *obj, sync_type_t sync_type) { return -ENOTSUP; }

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

namespace {

class FindAnyShardCR : public RGWCoroutine {
  RGWRados *const store;
  const RGWMetadataLog& mdlog;
  const int num_shards;
  int ret = 0;
 public:
  FindAnyShardCR(RGWRados *store, const RGWMetadataLog& mdlog, int num_shards)
    : RGWCoroutine(store->ctx()), store(store), mdlog(mdlog),
      num_shards(num_shards) {}

  int operate() {
    reenter(this) {
      // send stat requests for each shard in parallel
      yield {
        auto async_rados = store->get_async_rados();
        auto& pool = store->get_zone_params().log_pool;
        auto oid = std::string{};

        for (int i = 0; i < num_shards; i++) {
          mdlog.get_shard_oid(i, oid);
          auto obj = rgw_obj{pool, oid};
          spawn(new RGWStatObjCR(async_rados, store, obj), true);
        }
      }
      drain_all();
      // if any shards were found, return success
      while (collect_next(&ret)) {
        if (ret == 0) {
          // TODO: cancel instead of waiting for the rest
          return set_cr_done();
        }
        ret = 0; // collect_next() won't modify &ret unless it's a failure
      }
      // no shards found
      set_retcode(-ENOENT);
      return set_cr_error(-ENOENT);
    }
    return 0;
  }
};

// return true if any log shards exist for the given period
int find_shards_for_period(RGWRados *store, const std::string& period_id)
{
  auto cct = store->ctx();
  RGWMetadataLog mdlog(cct, store, period_id);
  auto num_shards = cct->_conf->rgw_md_log_max_shards;

  using FindAnyShardCRRef = boost::intrusive_ptr<FindAnyShardCR>;
  auto cr = FindAnyShardCRRef{new FindAnyShardCR(store, mdlog, num_shards)};

  RGWCoroutinesManager mgr(cct, nullptr);
  int r = mgr.run(cr.get());
  if (r < 0) {
    return r;
  }
  return cr->get_ret_status();
}

RGWPeriodHistory::Cursor find_oldest_log_period(RGWRados *store)
{
  // search backwards through the period history for the first period with no
  // log shard objects, and return its successor (some shards may be missing
  // if they contain no metadata yet, so we need to check all shards)
  auto cursor = store->period_history->get_current();
  auto oldest_log = cursor;

  while (cursor) {
    // search for an existing log shard object for this period
    int r = find_shards_for_period(store, cursor.get_period().get_id());
    if (r == -ENOENT) {
      ldout(store->ctx(), 10) << "find_oldest_log_period found no log shards "
          "for period " << cursor.get_period().get_id() << "; returning "
          "period " << oldest_log.get_period().get_id() << dendl;
      return oldest_log;
    }
    if (r < 0) {
      return RGWPeriodHistory::Cursor{r};
    }
    oldest_log = cursor;

    // advance to the period's predecessor
    if (!cursor.has_prev()) {
      auto& predecessor = cursor.get_period().get_predecessor();
      if (predecessor.empty()) {
        // this is the first period, so our logs must start here
        ldout(store->ctx(), 10) << "find_oldest_log_period returning first "
            "period " << cursor.get_period().get_id() << dendl;
        return cursor;
      }
      // pull the predecessor and add it to our history
      RGWPeriod period;
      int r = store->period_puller->pull(predecessor, period);
      if (r < 0) {
        return RGWPeriodHistory::Cursor{r};
      }
      auto prev = store->period_history->insert(std::move(period));
      if (!prev) {
        return prev;
      }
      ldout(store->ctx(), 10) << "find_oldest_log_period advancing to "
          "predecessor period " << predecessor << dendl;
      assert(cursor.has_prev());
    }
    cursor.prev();
  }
  ldout(store->ctx(), 10) << "find_oldest_log_period returning empty cursor" << dendl;
  return cursor;
}

} // anonymous namespace

int RGWMetadataManager::init(const std::string& current_period)
{
  if (store->is_meta_master()) {
    // find our oldest log so we can tell other zones where to start their sync
    oldest_log_period = find_oldest_log_period(store);
  }
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
  int pos = metadata_key.find(':');
  if (pos < 0) {
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

  assert(current_log); // must have called init()
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
  ::encode(log_data, logbl);

  assert(current_log); // must have called init()
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

  rgw_bucket heap_pool(store->get_zone_params().metadata_heap);

  RGWObjVersionTracker otracker;
  otracker.write_version = objv_tracker->write_version;
  string oid = heap_oid(handler, key, objv_tracker->write_version);
  int ret = rgw_put_system_obj(store, heap_pool, oid,
                               bl.c_str(), bl.length(), false,
                               &otracker, mtime, pattrs);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: rgw_put_system_obj() oid=" << oid << ") returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWMetadataManager::remove_from_heap(RGWMetadataHandler *handler, const string& key, RGWObjVersionTracker *objv_tracker)
{
  if (!objv_tracker) {
    return -EINVAL;
  }

  rgw_bucket heap_pool(store->get_zone_params().metadata_heap);

  string oid = heap_oid(handler, key, objv_tracker->write_version);
  rgw_obj obj(heap_pool, oid);
  int ret = store->delete_system_obj(obj);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: store->delete_system_obj()=" << oid << ") returned ret=" << ret << dendl;
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
  rgw_bucket bucket;

  handler->get_pool_and_oid(store, key, bucket, oid);

  ret = store_in_heap(handler, key, bl, objv_tracker, mtime, pattrs);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": store_in_heap() key=" << key << " returned ret=" << ret << dendl;
    goto done;
  }

  ret = rgw_put_system_obj(store, bucket, oid,
                           bl.c_str(), bl.length(), exclusive,
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
