

#include <string>

using namespace std;

#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_rados.h"
#include "rgw_orphan.h"

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_NUM_SHARDS 64

static string obj_fingerprint(const string& oid, const char *force_ns = NULL)
{
  ssize_t pos = oid.find('_');
  if (pos < 0) {
    cerr << "ERROR: object does not have a bucket marker: " << oid << std::endl;
  }

  string obj_marker = oid.substr(0, pos);

  string obj_name;
  string obj_instance;
  string obj_ns;

  rgw_obj::parse_raw_oid(oid.substr(pos + 1), &obj_name, &obj_instance, &obj_ns);

  if (obj_ns.empty()) {
    return oid;
  }

  string s = oid;

  if (force_ns) {
    rgw_bucket b;
    rgw_obj new_obj(b, obj_name);
    new_obj.set_ns(force_ns);
    new_obj.set_instance(obj_instance);
    s = obj_marker + "_" + new_obj.get_object();
  }

  /* cut out suffix */
  size_t i = s.size() - 1;
  for (; i >= s.size() - 10; --i) {
    char c = s[i];
    if (!isdigit(c) && c != '.' && c != '_') {
      break;
    }
  }

  return s.substr(0, i + 1);
}

int RGWOrphanStore::read_job(const string& job_name, RGWOrphanSearchState & state)
{
  set<string> keys;
  map<string, bufferlist> vals;
  keys.insert(job_name);
  int r = ioctx.omap_get_vals_by_keys(oid, keys, &vals);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist>::iterator iter = vals.find(job_name);
  if (iter == vals.end()) {
    return -ENOENT;
  }

  try {
    bufferlist& bl = iter->second;
    ::decode(state, bl);
  } catch (buffer::error& err) {
    lderr(store->ctx()) << "ERROR: could not decode buffer" << dendl;
    return -EIO;
  }

  return 0;
}

int RGWOrphanStore::write_job(const string& job_name, const RGWOrphanSearchState& state)
{
  map<string, bufferlist> vals;
  bufferlist bl;
  ::encode(state, bl);
  vals[job_name] = bl;
  int r = ioctx.omap_set(oid, vals);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWOrphanStore::remove_job(const string& job_name)
{
  set<string> keys;
  keys.insert(job_name);

  int r = ioctx.omap_rm_keys(oid, keys);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWOrphanStore::init()
{
  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados_handle();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    cerr << "ERROR: failed to open log pool (" << store->get_zone_params().log_pool.name << " ret=" << r << std::endl;
    return r;
  }

  return 0;
}

int RGWOrphanStore::store_entries(const string& oid, const map<string, bufferlist>& entries)
{
  librados::ObjectWriteOperation op;
  op.omap_set(entries);
  cout << "storing " << entries.size() << " entries at " << oid << std::endl;
  ldout(store->ctx(), 20) << "storing " << entries.size() << " entries at " << oid << ": " << dendl;
  for (map<string, bufferlist>::const_iterator iter = entries.begin(); iter != entries.end(); ++iter) {
    ldout(store->ctx(), 20) << " > " << iter->first << dendl;
  }
  int ret = ioctx.operate(oid, &op);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: " << __func__ << "(" << oid << ") returned ret=" << ret << dendl;
  }
  
  return 0;
}

int RGWOrphanStore::read_entries(const string& oid, const string& marker, map<string, bufferlist> *entries, bool *truncated)
{
#define MAX_OMAP_GET 100
  int ret = ioctx.omap_get_vals(oid, marker, MAX_OMAP_GET, entries);
  if (ret < 0) {
    cerr << "ERROR: " << __func__ << "(" << oid << ") returned ret=" << ret << std::endl;
  }

  *truncated = (entries->size() == MAX_OMAP_GET);

  return 0;
}

int RGWOrphanSearch::init(const string& job_name, RGWOrphanSearchInfo *info) {
  int r = orphan_store.init();
  if (r < 0) {
    return r;
  }

  RGWOrphanSearchState state;
  r = orphan_store.read_job(job_name, state);
  if (r < 0 && r != -ENOENT) {
    lderr(store->ctx()) << "ERROR: failed to read state ret=" << r << dendl;
    return r;
  }

  uint64_t num_shards = (info->num_shards ? info->num_shards : DEFAULT_NUM_SHARDS);
  if (r == 0) {
    if (num_shards != state.info.num_shards) {
      return -EINVAL;
    }
    search_info = state.info;
    search_stage = state.stage;
  } else { /* r == -ENOENT */
    search_info = *info;
    search_info.job_name = job_name;
    search_info.num_shards = num_shards;
    search_info.start_time = ceph_clock_now(store->ctx());
    search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_INIT);

    r = save_state();
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed to write state ret=" << r << dendl;
      return r;
    }
  }

  index_objs_prefix = RGW_ORPHAN_INDEX_PREFIX + string(".");
  index_objs_prefix += job_name;

  for (int i = 0; i < search_info.num_shards; i++) {
    char buf[128];

    snprintf(buf, sizeof(buf), "%s.rados.%d", index_objs_prefix.c_str(), i);
    all_objs_index[i] = buf;

    snprintf(buf, sizeof(buf), "%s.buckets.%d", index_objs_prefix.c_str(), i);
    buckets_instance_index[i] = buf;

    snprintf(buf, sizeof(buf), "%s.linked.%d", index_objs_prefix.c_str(), i);
    linked_objs_index[i] = buf;
  }
  return 0;
}

int RGWOrphanSearch::log_oids(map<int, string>& log_shards, map<int, list<string> >& oids)
{
  map<int, list<string> >::iterator miter = oids.begin();

  list<log_iter_info> liters; /* a list of iterator pairs for begin and end */

  for (; miter != oids.end(); ++miter) {
    log_iter_info info;
    info.oid = log_shards[miter->first];
    info.cur = miter->second.begin();
    info.end = miter->second.end();
    liters.push_back(info);
  }

  list<log_iter_info>::iterator list_iter;
  while (!liters.empty()) {
     list_iter = liters.begin();

     while (list_iter != liters.end()) {
       log_iter_info& cur_info = *list_iter;

       list<string>::iterator& cur = cur_info.cur;
       list<string>::iterator& end = cur_info.end;

       map<string, bufferlist> entries;
#define MAX_OMAP_SET_ENTRIES 100
       for (int j = 0; cur != end && j != MAX_OMAP_SET_ENTRIES; ++cur, ++j) {
         ldout(store->ctx(), 20) << "adding obj: " << *cur << dendl;
         entries[*cur] = bufferlist();
       }

       int ret = orphan_store.store_entries(cur_info.oid, entries);
       if (ret < 0) {
         return ret;
       }
       list<log_iter_info>::iterator tmp = list_iter;
       ++list_iter;
       if (cur == end) {
         liters.erase(tmp);
       }
     }
  }
  return 0;
}

int RGWOrphanSearch::build_all_oids_index()
{
  librados::Rados *rados = store->get_rados_handle();

  librados::IoCtx ioctx;

  int ret = rados->ioctx_create(search_info.pool.c_str(), ioctx);
  if (ret < 0) {
    lderr(store->ctx()) << __func__ << ": ioctx_create() returned ret=" << ret << dendl;
    return ret;
  }

  ioctx.set_namespace(librados::all_nspaces);
  librados::NObjectIterator i = ioctx.nobjects_begin();
  librados::NObjectIterator i_end = ioctx.nobjects_end();

  map<int, list<string> > oids;

  int count = 0;
  uint64_t total = 0;

  cout << "logging all objects in the pool" << std::endl;

  for (; i != i_end; ++i) {
    string nspace = i->get_nspace();
    string oid = i->get_oid();
    string locator = i->get_locator();

    ssize_t pos = oid.find('_');
    if (pos < 0) {
      cout << "unidentified oid: " << oid << ", skipping" << std::endl;
      /* what is this object, oids should be in the format of <bucket marker>_<obj>,
       * skip this entry
       */
      continue;
    }
    string stripped_oid = oid.substr(pos + 1);
    string name, instance, ns;
    if (!rgw_obj::parse_raw_oid(stripped_oid, &name, &instance, &ns)) {
      cout << "cannot parse oid: " << oid << ", skipping" << std::endl;
      continue;
    }

    if (ns.empty()) {
      /* skipping head objects, we don't want to remove these as they are mutable and
       * cleaning them up is racy (can race with object removal and a later recreation)
       */
      cout << "skipping head object: oid=" << oid << std::endl;
      continue;
    }

    string oid_fp = obj_fingerprint(oid);

    ldout(store->ctx(), 20) << "oid_fp=" << oid_fp << dendl;

    int shard = orphan_shard(oid_fp);
    oids[shard].push_back(oid);

#define COUNT_BEFORE_FLUSH 1000
    ++total;
    if (++count >= COUNT_BEFORE_FLUSH) {
      ldout(store->ctx(), 1) << "iterated through " << total << " objects" << dendl;
      ret = log_oids(all_objs_index, oids);
      if (ret < 0) {
        cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
        return ret;
      }
      count = 0;
      oids.clear();
    }
  }
  ret = log_oids(all_objs_index, oids);
  if (ret < 0) {
    cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
    return ret;
  }
  
  return 0;
}

int RGWOrphanSearch::build_buckets_instance_index()
{
  void *handle;
  int max = 1000;
  string section = "bucket.instance";
  int ret = store->meta_mgr->list_keys_init(section, &handle);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: can't get key: " << cpp_strerror(-ret) << dendl;
    return -ret;
  }

  map<int, list<string> > instances;

  bool truncated;

  RGWObjectCtx obj_ctx(store);

  int count = 0;
  uint64_t total = 0;

  do {
    list<string> keys;
    ret = store->meta_mgr->list_keys_next(handle, max, keys, &truncated);
    if (ret < 0) {
      lderr(store->ctx()) << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << dendl;
      return -ret;
    }

    for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      ++total;
      ldout(store->ctx(), 10) << "bucket_instance=" << *iter << " total=" << total << dendl;
      int shard = orphan_shard(*iter);
      instances[shard].push_back(*iter);

      if (++count >= COUNT_BEFORE_FLUSH) {
        ret = log_oids(buckets_instance_index, instances);
        if (ret < 0) {
          lderr(store->ctx()) << __func__ << ": ERROR: log_oids() returned ret=" << ret << dendl;
          return ret;
        }
        count = 0;
        instances.clear();
      }
    }

  } while (truncated);

  ret = log_oids(buckets_instance_index, instances);
  if (ret < 0) {
    lderr(store->ctx()) << __func__ << ": ERROR: log_oids() returned ret=" << ret << dendl;
    return ret;
  }
  store->meta_mgr->list_keys_complete(handle);

  return 0;
}

int RGWOrphanSearch::handle_stat_result(map<int, list<string> >& oids, RGWRados::Object::Stat::Result& result)
{
  set<string> obj_oids;
  rgw_bucket& bucket = result.obj.bucket;
  if (!result.has_manifest) { /* a very very old object, or part of a multipart upload during upload */
    const string loc = bucket.bucket_id + "_" + result.obj.get_object();
    obj_oids.insert(obj_fingerprint(loc));

    /*
     * multipart parts don't have manifest on them, it's in the meta object. Instead of reading the
     * meta object, just add a "shadow" object to the mix
     */
    obj_oids.insert(obj_fingerprint(loc, "shadow"));
  } else {
    RGWObjManifest& manifest = result.manifest;

    RGWObjManifest::obj_iterator miter;
    for (miter = manifest.obj_begin(); miter != manifest.obj_end(); ++miter) {
      const rgw_obj& loc = miter.get_location();
      string s = bucket.bucket_id + "_" + loc.get_object();
      obj_oids.insert(obj_fingerprint(s));
    }
  }

  for (set<string>::iterator iter = obj_oids.begin(); iter != obj_oids.end(); ++iter) {
    ldout(store->ctx(), 20) << __func__ << ": oid for obj=" << result.obj << ": " << *iter << dendl;

    int shard = orphan_shard(*iter);
    oids[shard].push_back(*iter);
  }

  return 0;
}

int RGWOrphanSearch::pop_and_handle_stat_op(map<int, list<string> >& oids, std::deque<RGWRados::Object::Stat>& ops)
{
  RGWRados::Object::Stat& front_op = ops.front();

  int ret = front_op.wait();
  if (ret < 0) {
    if (ret != -ENOENT) {
      lderr(store->ctx()) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
    }
    goto done;
  }
  ret = handle_stat_result(oids, front_op.result);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: handle_stat_response() returned error: " << cpp_strerror(-ret) << dendl;
  }
done:
  ops.pop_front();
  return ret;
}

int RGWOrphanSearch::build_linked_oids_for_bucket(const string& bucket_instance_id, map<int, list<string> >& oids)
{
  ldout(store->ctx(), 10) << "building linked oids for bucket instance: " << bucket_instance_id << dendl;
  RGWBucketInfo bucket_info;
  RGWObjectCtx obj_ctx(store);
  int ret = store->get_bucket_instance_info(obj_ctx, bucket_instance_id, bucket_info, NULL, NULL);
  if (ret < 0) {
    if (ret == -ENOENT) {
      /* probably raced with bucket removal */
      return 0;
    }
    lderr(store->ctx()) << __func__ << ": ERROR: RGWRados::get_bucket_instance_info() returned ret=" << ret << dendl;
    return ret;
  }

  RGWRados::Bucket target(store, bucket_info.bucket);
  RGWRados::Bucket::List list_op(&target);

  string marker;
  list_op.params.marker = rgw_obj_key(marker);
  list_op.params.list_versions = true;
  list_op.params.enforce_ns = false;

  bool truncated;

  deque<RGWRados::Object::Stat> stat_ops;

  int count = 0;

  do {
    vector<RGWObjEnt> result;

#define MAX_LIST_OBJS_ENTRIES 100
    ret = list_op.list_objects(MAX_LIST_OBJS_ENTRIES, &result, NULL, &truncated);
    if (ret < 0) {
      cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    for (vector<RGWObjEnt>::iterator iter = result.begin(); iter != result.end(); ++iter) {
      RGWObjEnt& entry = *iter;
      if (entry.key.instance.empty()) {
        ldout(store->ctx(), 20) << "obj entry: " << entry.key.name << dendl;
      } else {
        ldout(store->ctx(), 20) << "obj entry: " << entry.key.name << " [" << entry.key.instance << "]" << dendl;
      }

      ldout(store->ctx(), 20) << __func__ << ": entry.key.name=" << entry.key.name << " entry.key.instance=" << entry.key.instance << " entry.ns=" << entry.ns << dendl;
      rgw_obj obj(bucket_info.bucket, entry.key);
      obj.set_ns(entry.ns);

      RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);

      stat_ops.push_back(RGWRados::Object::Stat(&op_target));
      RGWRados::Object::Stat& op = stat_ops.back();


      ret = op.stat_async();
      if (ret < 0) {
        lderr(store->ctx()) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
        return ret;
      }
      if (stat_ops.size() >= max_concurrent_ios) {
        ret = pop_and_handle_stat_op(oids, stat_ops);
        if (ret < 0) {
          if (ret != -ENOENT) {
            lderr(store->ctx()) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
          }
        }
      }
      if (++count >= COUNT_BEFORE_FLUSH) {
        ret = log_oids(linked_objs_index, oids);
        if (ret < 0) {
          cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
          return ret;
        }
        count = 0;
        oids.clear();
      }
    }
  } while (truncated);

  while (!stat_ops.empty()) {
    ret = pop_and_handle_stat_op(oids, stat_ops);
    if (ret < 0) {
      if (ret != -ENOENT) {
        lderr(store->ctx()) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
      }
    }
  }

  return 0;
}

int RGWOrphanSearch::build_linked_oids_index()
{
  map<int, list<string> > oids;
  map<int, string>::iterator iter = buckets_instance_index.find(search_stage.shard);
  for (; iter != buckets_instance_index.end(); ++iter) {
    ldout(store->ctx(), 0) << "building linked oids index: " << iter->first << "/" << buckets_instance_index.size() << dendl;
    bool truncated;

    string oid = iter->second;

    do {
      map<string, bufferlist> entries;
      int ret = orphan_store.read_entries(oid, search_stage.marker, &entries, &truncated);
      if (ret == -ENOENT) {
        truncated = false;
        ret = 0;
      }

      if (ret < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: read_entries() oid=" << oid << " returned ret=" << ret << dendl;
        return ret;
      }

      if (entries.empty()) {
        break;
      }

      for (map<string, bufferlist>::iterator eiter = entries.begin(); eiter != entries.end(); ++eiter) {
        ldout(store->ctx(), 20) << " indexed entry: " << eiter->first << dendl;
        ret = build_linked_oids_for_bucket(eiter->first, oids);
      }

      search_stage.shard = iter->first;
      search_stage.marker = entries.rbegin()->first; /* last entry */
    } while (truncated);

    search_stage.marker.clear();
  }

  int ret = log_oids(linked_objs_index, oids);
  if (ret < 0) {
    cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
    return ret;
  }

  save_state();

  return 0;
}

class OMAPReader {
  librados::IoCtx ioctx;
  string oid;

  map<string, bufferlist> entries;
  map<string, bufferlist>::iterator iter;
  string marker;
  bool truncated;

public:
  OMAPReader(librados::IoCtx& _ioctx, const string& _oid) : ioctx(_ioctx), oid(_oid), truncated(true) {
    iter = entries.end();
  }

  int get_next(string *key, bufferlist *pbl, bool *done);
};

int OMAPReader::get_next(string *key, bufferlist *pbl, bool *done)
{
  if (iter != entries.end()) {
    *key = iter->first;
    if (pbl) {
      *pbl = iter->second;
    }
    ++iter;
    *done = false;
    marker = *key;
    return 0;
  }

  if (!truncated) {
    *done = true;
    return 0;
  }

#define MAX_OMAP_GET_ENTRIES 100
  int ret = ioctx.omap_get_vals(oid, marker, MAX_OMAP_GET_ENTRIES, &entries);
  if (ret < 0) {
    if (ret == -ENOENT) {
      *done = true;
      return 0;
    }
    return ret;
  }

  truncated = (entries.size() == MAX_OMAP_GET_ENTRIES);
  iter = entries.begin();
  return get_next(key, pbl, done);
}

int RGWOrphanSearch::compare_oid_indexes()
{
  assert(linked_objs_index.size() == all_objs_index.size());

  librados::IoCtx& ioctx = orphan_store.get_ioctx();

  librados::IoCtx data_ioctx;

  librados::Rados *rados = store->get_rados_handle();

  int ret = rados->ioctx_create(search_info.pool.c_str(), data_ioctx);
  if (ret < 0) {
    lderr(store->ctx()) << __func__ << ": ioctx_create() returned ret=" << ret << dendl;
    return ret;
  }

  uint64_t time_threshold = search_info.start_time.sec() - stale_secs;

  map<int, string>::iterator liter = linked_objs_index.begin();
  map<int, string>::iterator aiter = all_objs_index.begin();

  for (; liter != linked_objs_index.end(); ++liter, ++aiter) {
    OMAPReader linked_entries(ioctx, liter->second);
    OMAPReader all_entries(ioctx, aiter->second);

    bool done;

    string cur_linked;
    bool linked_done = false;


    do {
      string key;
      int r = all_entries.get_next(&key, NULL, &done);
      if (r < 0) {
        return r;
      }
      if (done) {
        break;
      }

      string key_fp = obj_fingerprint(key);

      while (cur_linked < key_fp && !linked_done) {
        r = linked_entries.get_next(&cur_linked, NULL, &linked_done);
        if (r < 0) {
          return r;
        }
      }

      if (cur_linked == key_fp) {
        ldout(store->ctx(), 20) << "linked: " << key << dendl;
        continue;
      }

      time_t mtime;
      r = data_ioctx.stat(key, NULL, &mtime);
      if (r < 0) {
        if (r != -ENOENT) {
          lderr(store->ctx()) << "ERROR: ioctx.stat(" << key << ") returned ret=" << r << dendl;
        }
        continue;
      }
      if (stale_secs && (uint64_t)mtime >= time_threshold) {
        ldout(store->ctx(), 20) << "skipping: " << key << " (mtime=" << mtime << " threshold=" << time_threshold << ")" << dendl;
        continue;
      }
      ldout(store->ctx(), 20) << "leaked: " << key << dendl;
      cout << "leaked: " << key << std::endl;
    } while (!done);
  }

  return 0;
}

int RGWOrphanSearch::run()
{
  int r;

  switch (search_stage.stage) {
    
    case ORPHAN_SEARCH_STAGE_INIT:
      ldout(store->ctx(), 0) << __func__ << "(): initializing state" << dendl;
      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_LSPOOL);
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through
    case ORPHAN_SEARCH_STAGE_LSPOOL:
      ldout(store->ctx(), 0) << __func__ << "(): building index of all objects in pool" << dendl;
      r = build_all_oids_index();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: build_all_objs_index returnr ret=" << r << dendl;
        return r;
      }

      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_LSBUCKETS);
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through

    case ORPHAN_SEARCH_STAGE_LSBUCKETS:
      ldout(store->ctx(), 0) << __func__ << "(): building index of all bucket indexes" << dendl;
      r = build_buckets_instance_index();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: build_all_objs_index returnr ret=" << r << dendl;
        return r;
      }

      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_ITERATE_BI);
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through


    case ORPHAN_SEARCH_STAGE_ITERATE_BI:
      ldout(store->ctx(), 0) << __func__ << "(): building index of all linked objects" << dendl;
      r = build_linked_oids_index();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: build_all_objs_index returnr ret=" << r << dendl;
        return r;
      }

      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_COMPARE);
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through

    case ORPHAN_SEARCH_STAGE_COMPARE:
      r = compare_oid_indexes();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: build_all_objs_index returnr ret=" << r << dendl;
        return r;
      }

      break;

    default:
      assert(0);
  };

  return 0;
}


int RGWOrphanSearch::remove_index(map<int, string>& index)
{
  librados::IoCtx& ioctx = orphan_store.get_ioctx();

  for (map<int, string>::iterator iter = index.begin(); iter != index.end(); ++iter) {
    int r = ioctx.remove(iter->second);
    if (r < 0) {
      if (r != -ENOENT) {
        ldout(store->ctx(), 0) << "ERROR: couldn't remove " << iter->second << ": ret=" << r << dendl;
      }
    }
  }
  return 0;
}

int RGWOrphanSearch::finish()
{
  int r = remove_index(all_objs_index);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: remove_index(" << all_objs_index << ") returned ret=" << r << dendl;
  }
  r = remove_index(buckets_instance_index);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: remove_index(" << buckets_instance_index << ") returned ret=" << r << dendl;
  }
  r = remove_index(linked_objs_index);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: remove_index(" << linked_objs_index << ") returned ret=" << r << dendl;
  }

  r = orphan_store.remove_job(search_info.job_name);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: could not remove job name (" << search_info.job_name << ") ret=" << r << dendl;
  }

  return r;
}
