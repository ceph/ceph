// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <string>


#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_op.h"
#include "rgw_multi.h"
#include "rgw_orphan.h"
#include "rgw_zone.h"
#include "rgw_bucket.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_NUM_SHARDS 64

using namespace std;

static string obj_fingerprint(const string& oid, const char *force_ns = NULL)
{
  ssize_t pos = oid.find('_');
  if (pos < 0) {
    cerr << "ERROR: object does not have a bucket marker: " << oid << std::endl;
  }

  string obj_marker = oid.substr(0, pos);

  rgw_obj_key key;

  rgw_obj_key::parse_raw_oid(oid.substr(pos + 1), &key);

  if (key.ns.empty()) {
    return oid;
  }

  string s = oid;

  if (force_ns) {
    rgw_bucket b;
    rgw_obj new_obj(b, key);
    s = obj_marker + "_" + new_obj.get_oid();
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
    decode(state, bl);
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
  encode(state, bl);
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

int RGWOrphanStore::list_jobs(map <string,RGWOrphanSearchState>& job_list)
{
  map <string,bufferlist> vals;
  int MAX_READ=1024;
  string marker="";
  int r = 0;

  // loop through all the omap vals from index object, storing them to job_list,
  // read in batches of 1024, we update the marker every iteration and exit the
  // loop when we find that total size read out is less than batch size
  do {
    r = ioctx.omap_get_vals(oid, marker, MAX_READ, &vals);
    if (r < 0) {
      return r;
    }
    r = vals.size();

    for (const auto &it : vals) {
      marker=it.first;
      RGWOrphanSearchState state;
      try {
        bufferlist bl = it.second;
        decode(state, bl);
      } catch (buffer::error& err) {
        lderr(store->ctx()) << "ERROR: could not decode buffer" << dendl;
        return -EIO;
      }
      job_list[it.first] = state;
    }
  } while (r == MAX_READ);

  return 0;
}

int RGWOrphanStore::init(const DoutPrefixProvider *dpp)
{
  const rgw_pool& log_pool = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone_params().log_pool;
  int r = rgw_init_ioctx(dpp, static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_rados_handle(), log_pool, ioctx);
  if (r < 0) {
    cerr << "ERROR: failed to open log pool (" << log_pool << " ret=" << r << std::endl;
    return r;
  }

  return 0;
}

int RGWOrphanStore::store_entries(const DoutPrefixProvider *dpp, const string& oid, const map<string, bufferlist>& entries)
{
  librados::ObjectWriteOperation op;
  op.omap_set(entries);
  cout << "storing " << entries.size() << " entries at " << oid << std::endl;
  ldpp_dout(dpp, 20) << "storing " << entries.size() << " entries at " << oid << ": " << dendl;
  for (map<string, bufferlist>::const_iterator iter = entries.begin(); iter != entries.end(); ++iter) {
    ldpp_dout(dpp, 20) << " > " << iter->first << dendl;
  }
  int ret = rgw_rados_operate(dpp, ioctx, oid, &op, null_yield);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: " << __func__ << "(" << oid << ") returned ret=" << ret << dendl;
  }
  
  return 0;
}

int RGWOrphanStore::read_entries(const string& oid, const string& marker, map<string, bufferlist> *entries, bool *truncated)
{
#define MAX_OMAP_GET 100
  int ret = ioctx.omap_get_vals(oid, marker, MAX_OMAP_GET, entries);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "ERROR: " << __func__ << "(" << oid << ") returned ret=" << cpp_strerror(-ret) << std::endl;
  }

  *truncated = (entries->size() == MAX_OMAP_GET);

  return 0;
}

int RGWOrphanSearch::init(const DoutPrefixProvider *dpp, const string& job_name, RGWOrphanSearchInfo *info, bool _detailed_mode)
{
  int r = orphan_store.init(dpp);
  if (r < 0) {
    return r;
  }

  constexpr int64_t MAX_LIST_OBJS_ENTRIES=100;

  max_list_bucket_entries = std::max(store->ctx()->_conf->rgw_list_bucket_min_readahead,
                                     MAX_LIST_OBJS_ENTRIES);

  detailed_mode = _detailed_mode;
  RGWOrphanSearchState state;
  r = orphan_store.read_job(job_name, state);
  if (r < 0 && r != -ENOENT) {
    ldpp_dout(dpp, -1) << "ERROR: failed to read state ret=" << r << dendl;
    return r;
  }

  if (r == 0) {
    search_info = state.info;
    search_stage = state.stage;
  } else if (info) { /* r == -ENOENT, initiate a new job if info was provided */ 
    search_info = *info;
    search_info.job_name = job_name;
    search_info.num_shards = (info->num_shards ? info->num_shards : DEFAULT_NUM_SHARDS);
    search_info.start_time = ceph_clock_now();
    search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_INIT);

    r = save_state();
    if (r < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to write state ret=" << r << dendl;
      return r;
    }
  } else {
      ldpp_dout(dpp, -1) << "ERROR: job not found" << dendl;
      return r;
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

int RGWOrphanSearch::log_oids(const DoutPrefixProvider *dpp, map<int, string>& log_shards, map<int, list<string> >& oids)
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
         ldpp_dout(dpp, 20) << "adding obj: " << *cur << dendl;
         entries[*cur] = bufferlist();
       }

       int ret = orphan_store.store_entries(dpp, cur_info.oid, entries);
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

int RGWOrphanSearch::build_all_oids_index(const DoutPrefixProvider *dpp)
{
  librados::IoCtx ioctx;

  int ret = rgw_init_ioctx(dpp, static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_rados_handle(), search_info.pool, ioctx);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << __func__ << ": rgw_init_ioctx() returned ret=" << ret << dendl;
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
    rgw_obj_key key;
    if (!rgw_obj_key::parse_raw_oid(stripped_oid, &key)) {
      cout << "cannot parse oid: " << oid << ", skipping" << std::endl;
      continue;
    }

    if (key.ns.empty()) {
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
      ret = log_oids(dpp, all_objs_index, oids);
      if (ret < 0) {
        cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
        return ret;
      }
      count = 0;
      oids.clear();
    }
  }
  ret = log_oids(dpp, all_objs_index, oids);
  if (ret < 0) {
    cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
    return ret;
  }
  
  return 0;
}

int RGWOrphanSearch::build_buckets_instance_index(const DoutPrefixProvider *dpp)
{
  void *handle;
  int max = 1000;
  string section = "bucket.instance";
  int ret = store->meta_list_keys_init(dpp, section, string(), &handle);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: can't get key: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<int, list<string> > instances;

  bool truncated;

  RGWObjectCtx obj_ctx(store);

  int count = 0;
  uint64_t total = 0;

  do {
    list<string> keys;
    ret = store->meta_list_keys_next(dpp, handle, max, keys, &truncated);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      ++total;
      ldpp_dout(dpp, 10) << "bucket_instance=" << *iter << " total=" << total << dendl;
      int shard = orphan_shard(*iter);
      instances[shard].push_back(*iter);

      if (++count >= COUNT_BEFORE_FLUSH) {
        ret = log_oids(dpp, buckets_instance_index, instances);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << __func__ << ": ERROR: log_oids() returned ret=" << ret << dendl;
          return ret;
        }
        count = 0;
        instances.clear();
      }
    }

  } while (truncated);

  store->meta_list_keys_complete(handle);

  ret = log_oids(dpp, buckets_instance_index, instances);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << __func__ << ": ERROR: log_oids() returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWOrphanSearch::handle_stat_result(const DoutPrefixProvider *dpp, map<int, list<string> >& oids, RGWRados::Object::Stat::Result& result)
{
  set<string> obj_oids;
  rgw_bucket& bucket = result.obj.bucket;
  if (!result.manifest) { /* a very very old object, or part of a multipart upload during upload */
    const string loc = bucket.bucket_id + "_" + result.obj.get_oid();
    obj_oids.insert(obj_fingerprint(loc));

    /*
     * multipart parts don't have manifest on them, it's in the meta object. Instead of reading the
     * meta object, just add a "shadow" object to the mix
     */
    obj_oids.insert(obj_fingerprint(loc, "shadow"));
  } else {
    RGWObjManifest& manifest = *result.manifest;

    if (!detailed_mode &&
        manifest.get_obj_size() <= manifest.get_head_size()) {
      ldpp_dout(dpp, 5) << "skipping object as it fits in a head" << dendl;
      return 0;
    }

    RGWObjManifest::obj_iterator miter;
    for (miter = manifest.obj_begin(dpp); miter != manifest.obj_end(dpp); ++miter) {
      const rgw_raw_obj& loc = miter.get_location().get_raw_obj(store->getRados());
      string s = loc.oid;
      obj_oids.insert(obj_fingerprint(s));
    }
  }

  for (set<string>::iterator iter = obj_oids.begin(); iter != obj_oids.end(); ++iter) {
    ldpp_dout(dpp, 20) << __func__ << ": oid for obj=" << result.obj << ": " << *iter << dendl;

    int shard = orphan_shard(*iter);
    oids[shard].push_back(*iter);
  }

  return 0;
}

int RGWOrphanSearch::pop_and_handle_stat_op(const DoutPrefixProvider *dpp, map<int, list<string> >& oids, std::deque<RGWRados::Object::Stat>& ops)
{
  RGWRados::Object::Stat& front_op = ops.front();

  int ret = front_op.wait(dpp);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
    }
    goto done;
  }
  ret = handle_stat_result(dpp, oids, front_op.result);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: handle_stat_response() returned error: " << cpp_strerror(-ret) << dendl;
  }
done:
  ops.pop_front();
  return ret;
}

int RGWOrphanSearch::build_linked_oids_for_bucket(const DoutPrefixProvider *dpp, const string& bucket_instance_id, map<int, list<string> >& oids)
{
  RGWObjectCtx obj_ctx(store);
  rgw_bucket orphan_bucket;
  int shard_id;
  int ret = rgw_bucket_parse_bucket_key(store->ctx(), bucket_instance_id,
                                        &orphan_bucket, &shard_id);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << " failed to parse bucket instance: "
                 << bucket_instance_id << " skipping" << dendl;
    return ret;
  }

  std::unique_ptr<rgw::sal::Bucket> cur_bucket;
  ret = store->load_bucket(dpp, orphan_bucket, &cur_bucket, null_yield);
  if (ret < 0) {
    if (ret == -ENOENT) {
      /* probably raced with bucket removal */
      return 0;
    }
    ldpp_dout(dpp, -1) << __func__ << ": ERROR: RGWRados::get_bucket_instance_info() returned ret=" << ret << dendl;
    return ret;
  }

  if (cur_bucket->get_bucket_id() != orphan_bucket.bucket_id) {
    ldpp_dout(dpp, 0) << __func__ << ": Skipping stale bucket instance: "
                           << orphan_bucket.name << ": "
                           << orphan_bucket.bucket_id << dendl;
    return 0;
  }

  if (cur_bucket->get_info().layout.resharding != rgw::BucketReshardState::None) {
    ldpp_dout(dpp, 0) << __func__ << ": reshard in progress. Skipping "
                           << orphan_bucket.name << ": "
                           << orphan_bucket.bucket_id << dendl;
    return 0;
  }

  rgw_bucket b;
  rgw_bucket_parse_bucket_key(store->ctx(), bucket_instance_id, &b, nullptr);
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->load_bucket(dpp, b, &bucket, null_yield);
  if (ret < 0) {
    if (ret == -ENOENT) {
      /* probably raced with bucket removal */
      return 0;
    }
    ldpp_dout(dpp, -1) << __func__ << ": ERROR: RGWRados::get_bucket_instance_info() returned ret=" << ret << dendl;
    return ret;
  }

  ldpp_dout(dpp, 10) << "building linked oids for bucket instance: " << bucket_instance_id << dendl;
  RGWRados::Bucket target(store->getRados(), cur_bucket->get_info());
  RGWRados::Bucket::List list_op(&target);

  string marker;
  list_op.params.marker = rgw_obj_key(marker);
  list_op.params.list_versions = true;
  list_op.params.enforce_ns = false;

  bool truncated;

  deque<RGWRados::Object::Stat> stat_ops;

  do {
    vector<rgw_bucket_dir_entry> result;

    ret = list_op.list_objects(dpp, max_list_bucket_entries,
                               &result, nullptr, &truncated, null_yield);
    if (ret < 0) {
      cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    for (vector<rgw_bucket_dir_entry>::iterator iter = result.begin(); iter != result.end(); ++iter) {
      rgw_bucket_dir_entry& entry = *iter;
      if (entry.key.instance.empty()) {
        ldpp_dout(dpp, 20) << "obj entry: " << entry.key.name << dendl;
      } else {
        ldpp_dout(dpp, 20) << "obj entry: " << entry.key.name << " [" << entry.key.instance << "]" << dendl;
      }

      ldpp_dout(dpp, 20) << __func__ << ": entry.key.name=" << entry.key.name << " entry.key.instance=" << entry.key.instance << dendl;

      if (!detailed_mode &&
          entry.meta.accounted_size <= (uint64_t)store->ctx()->_conf->rgw_max_chunk_size) {
        ldpp_dout(dpp, 5) << __func__ << "skipping stat as the object " << entry.key.name
                              << "fits in a head" << dendl;
        continue;
      }

      rgw_obj obj(cur_bucket->get_key(), entry.key);

      RGWRados::Object op_target(store->getRados(), cur_bucket->get_info(), obj_ctx, obj);

      stat_ops.push_back(RGWRados::Object::Stat(&op_target));
      RGWRados::Object::Stat& op = stat_ops.back();

      ret = op.stat_async(dpp);
      if (ret < 0) {
        ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
        return ret;
      }
      if (stat_ops.size() >= max_concurrent_ios) {
        ret = pop_and_handle_stat_op(dpp, oids, stat_ops);
        if (ret < 0) {
          if (ret != -ENOENT) {
            ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
          }
        }
      }
      if (oids.size() >= COUNT_BEFORE_FLUSH) {
        ret = log_oids(dpp, linked_objs_index, oids);
        if (ret < 0) {
          cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
          return ret;
        }
        oids.clear();
      }
    }
  } while (truncated);

  while (!stat_ops.empty()) {
    ret = pop_and_handle_stat_op(dpp, oids, stat_ops);
    if (ret < 0) {
      if (ret != -ENOENT) {
        ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " << cpp_strerror(-ret) << dendl;
      }
    }
  }

  return 0;
}

int RGWOrphanSearch::build_linked_oids_index(const DoutPrefixProvider *dpp)
{
  map<int, list<string> > oids;
  map<int, string>::iterator iter = buckets_instance_index.find(search_stage.shard);
  for (; iter != buckets_instance_index.end(); ++iter) {
    ldpp_dout(dpp, 0) << "building linked oids index: " << iter->first << "/" << buckets_instance_index.size() << dendl;
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
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: read_entries() oid=" << oid << " returned ret=" << ret << dendl;
        return ret;
      }

      if (entries.empty()) {
        break;
      }

      for (map<string, bufferlist>::iterator eiter = entries.begin(); eiter != entries.end(); ++eiter) {
        ldpp_dout(dpp, 20) << " indexed entry: " << eiter->first << dendl;
        ret = build_linked_oids_for_bucket(dpp, eiter->first, oids);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << __func__ << ": ERROR: build_linked_oids_for_bucket() indexed entry=" << eiter->first
                              << " returned ret=" << ret << dendl;
          return ret;
        }
      }

      search_stage.shard = iter->first;
      search_stage.marker = entries.rbegin()->first; /* last entry */
    } while (truncated);

    search_stage.marker.clear();
  }

  int ret = log_oids(dpp, linked_objs_index, oids);
  if (ret < 0) {
    cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
    return ret;
  }

  ret = save_state();
  if (ret < 0) {
    cerr << __func__ << ": ERROR: failed to write state ret=" << ret << std::endl;
    return ret;
  }

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

int RGWOrphanSearch::compare_oid_indexes(const DoutPrefixProvider *dpp)
{
  ceph_assert(linked_objs_index.size() == all_objs_index.size());

  librados::IoCtx& ioctx = orphan_store.get_ioctx();

  librados::IoCtx data_ioctx;

  int ret = rgw_init_ioctx(dpp, static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_rados_handle(), search_info.pool, data_ioctx);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << __func__ << ": rgw_init_ioctx() returned ret=" << ret << dendl;
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
        ldpp_dout(dpp, 20) << "linked: " << key << dendl;
        continue;
      }

      time_t mtime;
      r = data_ioctx.stat(key, NULL, &mtime);
      if (r < 0) {
        if (r != -ENOENT) {
          ldpp_dout(dpp, -1) << "ERROR: ioctx.stat(" << key << ") returned ret=" << r << dendl;
        }
        continue;
      }
      if (stale_secs && (uint64_t)mtime >= time_threshold) {
        ldpp_dout(dpp, 20) << "skipping: " << key << " (mtime=" << mtime << " threshold=" << time_threshold << ")" << dendl;
        continue;
      }
      ldpp_dout(dpp, 20) << "leaked: " << key << dendl;
      cout << "leaked: " << key << std::endl;
    } while (!done);
  }

  return 0;
}

int RGWOrphanSearch::run(const DoutPrefixProvider *dpp)
{
  int r;

  switch (search_stage.stage) {
    
    case ORPHAN_SEARCH_STAGE_INIT:
      ldpp_dout(dpp, 0) << __func__ << "(): initializing state" << dendl;
      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_LSPOOL);
      r = save_state();
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through
    case ORPHAN_SEARCH_STAGE_LSPOOL:
      ldpp_dout(dpp, 0) << __func__ << "(): building index of all objects in pool" << dendl;
      r = build_all_oids_index(dpp);
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: build_all_objs_index returned ret=" << r << dendl;
        return r;
      }

      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_LSBUCKETS);
      r = save_state();
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through

    case ORPHAN_SEARCH_STAGE_LSBUCKETS:
      ldpp_dout(dpp, 0) << __func__ << "(): building index of all bucket indexes" << dendl;
      r = build_buckets_instance_index(dpp);
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: build_all_objs_index returned ret=" << r << dendl;
        return r;
      }

      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_ITERATE_BI);
      r = save_state();
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through


    case ORPHAN_SEARCH_STAGE_ITERATE_BI:
      ldpp_dout(dpp, 0) << __func__ << "(): building index of all linked objects" << dendl;
      r = build_linked_oids_index(dpp);
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: build_all_objs_index returned ret=" << r << dendl;
        return r;
      }

      search_stage = RGWOrphanSearchStage(ORPHAN_SEARCH_STAGE_COMPARE);
      r = save_state();
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through

    case ORPHAN_SEARCH_STAGE_COMPARE:
      r = compare_oid_indexes(dpp);
      if (r < 0) {
        ldpp_dout(dpp, -1) << __func__ << ": ERROR: build_all_objs_index returned ret=" << r << dendl;
        return r;
      }

      break;

    default:
      ceph_abort();
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


int RGWRadosList::handle_stat_result(const DoutPrefixProvider *dpp,
				     RGWRados::Object::Stat::Result& result,
				     std::string& bucket_name,
				     rgw_obj_key& obj_key,
                                     std::set<string>& obj_oids)
{
  obj_oids.clear();

  rgw_bucket& bucket = result.obj.bucket;

  ldpp_dout(dpp, 20) << "RGWRadosList::" << __func__ <<
    " bucket=" << bucket <<
    ", has_manifest=" << result.manifest.has_value() <<
    dendl;

  // iterator to store result of dlo/slo attribute find
  decltype(result.attrs)::iterator attr_it = result.attrs.end();
  const std::string oid = bucket.marker + "_" + result.obj.get_oid();
  ldpp_dout(dpp, 20) << "radoslist processing object=\"" <<
      oid << "\"" << dendl;
  if (visited_oids.find(oid) != visited_oids.end()) {
    // apparently we hit a loop; don't continue with this oid
    ldpp_dout(dpp, 15) <<
      "radoslist stopped loop at already visited object=\"" <<
      oid << "\"" << dendl;
    return 0;
  }

  bucket_name = bucket.name;
  obj_key = result.obj.key;

  if (!result.manifest) {
    /* a very very old object, or part of a multipart upload during upload */
    obj_oids.insert(oid);

    /*
     * multipart parts don't have manifest on them, it's in the meta
     * object; we'll process them in
     * RGWRadosList::do_incomplete_multipart
     */
  } else if ((attr_it = result.attrs.find(RGW_ATTR_USER_MANIFEST)) !=
	     result.attrs.end()) {
    // *** handle DLO object ***

    obj_oids.insert(oid);
    visited_oids.insert(oid); // prevent dlo loops
    ldpp_dout(dpp, 15) << "radoslist added to visited list DLO=\"" <<
      oid << "\"" << dendl;

    char* prefix_path_c = attr_it->second.c_str();
    const std::string& prefix_path = prefix_path_c;

    const size_t sep_pos = prefix_path.find('/');
    if (string::npos == sep_pos) {
      return -EINVAL;
    }

    const std::string bucket_name = prefix_path.substr(0, sep_pos);
    const std::string prefix = prefix_path.substr(sep_pos + 1);

    add_bucket_prefix(bucket_name, prefix);
    ldpp_dout(dpp, 25) << "radoslist DLO oid=\"" << oid <<
      "\" added bucket=\"" << bucket_name << "\" prefix=\"" <<
      prefix << "\" to process list" << dendl;
  } else if ((attr_it = result.attrs.find(RGW_ATTR_USER_MANIFEST)) !=
	     result.attrs.end()) {
    // *** handle SLO object ***

    obj_oids.insert(oid);
    visited_oids.insert(oid); // prevent slo loops
    ldpp_dout(dpp, 15) << "radoslist added to visited list SLO=\"" <<
      oid << "\"" << dendl;

    RGWSLOInfo slo_info;
    bufferlist::const_iterator bliter = attr_it->second.begin();
    try {
      ::decode(slo_info, bliter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) <<
	"ERROR: failed to decode slo manifest for " << oid << dendl;
      return -EIO;
    }

    for (const auto& iter : slo_info.entries) {
      const string& path_str = iter.path;

      const size_t sep_pos = path_str.find('/', 1 /* skip initial slash */);
      if (string::npos == sep_pos) {
	return -EINVAL;
      }

      std::string bucket_name;
      std::string obj_name;

      bucket_name = url_decode(path_str.substr(1, sep_pos - 1));
      obj_name = url_decode(path_str.substr(sep_pos + 1));

      const rgw_obj_key obj_key(obj_name);
      add_bucket_filter(bucket_name, obj_key);
      ldpp_dout(dpp, 25) << "radoslist SLO oid=\"" << oid <<
	"\" added bucket=\"" << bucket_name << "\" obj_key=\"" <<
	obj_key << "\" to process list" << dendl;
    }
  } else {
    RGWObjManifest& manifest = *result.manifest;

    // in multipart, the head object contains no data and just has the
    // manifest AND empty objects have no manifest, but they're
    // realized as empty rados objects
    if (0 == manifest.get_max_head_size() ||
	manifest.obj_begin(dpp) == manifest.obj_end(dpp)) {
      obj_oids.insert(oid);
      // first_insert = true;
    }

    RGWObjManifest::obj_iterator miter;
    for (miter = manifest.obj_begin(dpp); miter != manifest.obj_end(dpp); ++miter) {
      const rgw_raw_obj& loc =
	miter.get_location().get_raw_obj(store->getRados());
      string s = loc.oid;
      obj_oids.insert(s);
    }
  }

  return 0;
} // RGWRadosList::handle_stat_result

int RGWRadosList::pop_and_handle_stat_op(
  const DoutPrefixProvider *dpp,
  RGWObjectCtx& obj_ctx,
  std::deque<RGWRados::Object::Stat>& ops)
{
  std::string bucket_name;
  rgw_obj_key obj_key;
  std::set<std::string> obj_oids;
  RGWRados::Object::Stat& front_op = ops.front();

  int ret = front_op.wait(dpp);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
	cpp_strerror(-ret) << dendl;
    }
    goto done;
  }

  ret = handle_stat_result(dpp, front_op.result, bucket_name, obj_key, obj_oids);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: handle_stat_result() returned error: " <<
      cpp_strerror(-ret) << dendl;
  }

  // output results
  for (const auto& o : obj_oids) {
    if (include_rgw_obj_name) {
      std::cout << o <<
	field_separator << bucket_name <<
	field_separator << obj_key <<
	std::endl;
    } else {
      std::cout << o << std::endl;
    }
  }

done:

  // invalidate object context for this object to avoid memory leak
  // (see pr https://github.com/ceph/ceph/pull/30174)
  obj_ctx.invalidate(front_op.result.obj);

  ops.pop_front();
  return ret;
}


#if 0 // code that may be the basis for expansion
int RGWRadosList::build_buckets_instance_index()
{
  void *handle;
  int max = 1000;
  string section = "bucket.instance";
  int ret = store->meta_mgr->list_keys_init(section, &handle);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: can't get key: " << cpp_strerror(-ret) << dendl;
    return ret;
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
      return ret;
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
#endif


int RGWRadosList::process_bucket(
  const DoutPrefixProvider *dpp,
  const std::string& bucket_instance_id,
  const std::string& prefix,
  const std::set<rgw_obj_key>& entries_filter)
{
  ldpp_dout(dpp, 10) << "RGWRadosList::" << __func__ <<
    " bucket_instance_id=" << bucket_instance_id <<
    ", prefix=" << prefix <<
    ", entries_filter.size=" << entries_filter.size() << dendl;

  RGWBucketInfo bucket_info;
  int ret = store->getRados()->get_bucket_instance_info(bucket_instance_id,
							bucket_info,
							nullptr,
							nullptr,
							null_yield,
                                                        dpp);
  if (ret < 0) {
    if (ret == -ENOENT) {
      // probably raced with bucket removal
      return 0;
    }
    ldpp_dout(dpp, -1) << __func__ <<
      ": ERROR: RGWRados::get_bucket_instance_info() returned ret=" <<
      ret << dendl;
    return ret;
  }

  RGWRados::Bucket target(store->getRados(), bucket_info);
  RGWRados::Bucket::List list_op(&target);

  std::string marker;
  list_op.params.marker = rgw_obj_key(marker);
  list_op.params.list_versions = true;
  list_op.params.enforce_ns = false;
  list_op.params.allow_unordered = false;
  list_op.params.prefix = prefix;

  bool truncated;

  std::deque<RGWRados::Object::Stat> stat_ops;
  std::string prev_versioned_key_name = "";

  RGWObjectCtx obj_ctx(store);

  do {
    std::vector<rgw_bucket_dir_entry> result;
    constexpr int64_t LIST_OBJS_MAX_ENTRIES = 100;
    ret = list_op.list_objects(dpp, LIST_OBJS_MAX_ENTRIES, &result,
			       NULL, &truncated, null_yield);
    if (ret == -ENOENT) {
      // race with bucket delete?
      ret = 0;
      break;
    } else if (ret < 0) {
      std::cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) <<
	std::endl;
      return ret;
    }

    for (std::vector<rgw_bucket_dir_entry>::iterator iter = result.begin();
	 iter != result.end();
	 ++iter) {
      rgw_bucket_dir_entry& entry = *iter;

      if (entry.key.instance.empty()) {
        ldpp_dout(dpp, 20) << "obj entry: " << entry.key.name << dendl;
      } else {
        ldpp_dout(dpp, 20) << "obj entry: " << entry.key.name <<
	  " [" << entry.key.instance << "]" << dendl;
      }

      ldpp_dout(dpp, 20) << __func__ << ": entry.key.name=" <<
	entry.key.name << " entry.key.instance=" << entry.key.instance <<
	dendl;

      // ignore entries that are not in the filter if there is a filter
      if (!entries_filter.empty() &&
	  entries_filter.find(entry.key) == entries_filter.cend()) {
	continue;
      }

      auto bucket = store->get_bucket(bucket_info);
      // we need to do this in two cases below, so use a lambda
      auto do_stat_key =
	[&](const rgw_obj_key& key) -> int {
	  int ret;

	  rgw_obj obj(bucket_info.bucket, key);
	  RGWRados::Object op_target(store->getRados(), bucket_info,
				     obj_ctx, obj);

	  stat_ops.push_back(RGWRados::Object::Stat(&op_target));
	  RGWRados::Object::Stat& op = stat_ops.back();

	  ret = op.stat_async(dpp);
	  if (ret < 0) {
	    ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
	      cpp_strerror(-ret) << dendl;
	    return ret;
	  }

	  if (stat_ops.size() >= max_concurrent_ios) {
	    ret = pop_and_handle_stat_op(dpp, obj_ctx, stat_ops);
	    if (ret < 0) {
	      if (ret != -ENOENT) {
		ldpp_dout(dpp, -1) <<
		  "ERROR: pop_and_handle_stat_op() returned error: " <<
		  cpp_strerror(-ret) << dendl;
	      }

	      // clear error, so we'll continue processing directory
	      ret = 0;
	    }
	  }

	  return ret;
	}; // do_stat_key lambda

      // for versioned objects, make sure the head object is handled
      // as well by ignoring the instance identifier
      if (!entry.key.instance.empty() &&
	  entry.key.name != prev_versioned_key_name) {
	// don't do the same key twice; even though out bucket index
	// listing allows unordered, since all versions of an object
	// use the same bucket index key, they'll all end up together
	// and sorted
	prev_versioned_key_name = entry.key.name;

	rgw_obj_key uninstanced(entry.key.name);

	ret = do_stat_key(uninstanced);
	if (ret < 0) {
	  return ret;
	}
      }

      ret = do_stat_key(entry.key);
      if (ret < 0) {
	return ret;
      }
    } // for iter loop
  } while (truncated);

  while (!stat_ops.empty()) {
    ret = pop_and_handle_stat_op(dpp, obj_ctx, stat_ops);
    if (ret < 0) {
      if (ret != -ENOENT) {
        ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
	  cpp_strerror(-ret) << dendl;
      }
    }
  }

  return 0;
}


int RGWRadosList::run(const DoutPrefixProvider *dpp,
		      const bool yes_i_really_mean_it)
{
  int ret;
  void* handle = nullptr;

  ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
      " ERROR: list_keys_init returned " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  constexpr int max_keys = 1000;
  bool truncated = true;
  bool warned_indexless = false;

  do {
    std::list<std::string> buckets;
    ret = store->meta_list_keys_next(dpp, handle, max_keys, buckets, &truncated);

    for (std::string& bucket_id : buckets) {
      ret = run(dpp, bucket_id, true);
      if (ret == -ENOENT) {
	continue;
      } else if (ret == -EINVAL) {
	if (! warned_indexless) {
	  if (yes_i_really_mean_it) {
	    std::cerr <<
	      "WARNING: because there is at least one indexless bucket (" <<
	      bucket_id <<
	      ") the results of radoslist are *incomplete*; continuing due to --yes-i-really-mean-it" <<
	      std::endl;
	    warned_indexless = true;
	  } else {
	    std::cerr << "ERROR: because there is at least one indexless bucket (" <<
	      bucket_id <<
	      ") the results of radoslist are *incomplete*; use --yes-i-really-mean-it to bypass error" <<
	      std::endl;
	    return ret;
	  }
	}
	continue;
      } else if (ret < 0) {
	return ret;
      }
    }
  } while (truncated);

  return 0;
} // RGWRadosList::run(DoutPrefixProvider, bool)


int RGWRadosList::run(const DoutPrefixProvider *dpp,
		      const std::string& start_bucket_name,
		      const bool silent_indexless)
{
  int ret;

  add_bucket_entire(start_bucket_name);

  while (! bucket_process_map.empty()) {
    // pop item from map and capture its key data
    auto front = bucket_process_map.begin();
    std::string bucket_name = front->first;
    process_t process;
    std::swap(process, front->second);
    bucket_process_map.erase(front);

    std::unique_ptr<rgw::sal::Bucket> bucket;
    ret = store->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                             &bucket, null_yield);
    if (ret == -ENOENT) {
      std::cerr << "WARNING: bucket " << bucket_name <<
	" does not exist; could it have been deleted very recently?" <<
	std::endl;
      continue;
    } else if (ret < 0) {
      std::cerr << "ERROR: could not get info for bucket " << bucket_name <<
	" -- " << cpp_strerror(-ret) << std::endl;
      return ret;
    } else if (bucket->get_info().is_indexless()) {
      if (! silent_indexless) {
	std::cerr << "ERROR: unable to run radoslist on indexless bucket " <<
	  bucket_name << std::endl;
      }
      return -EINVAL;
    }

    const std::string bucket_id = bucket->get_key().get_key();

    static const std::set<rgw_obj_key> empty_filter;
    static const std::string empty_prefix;

    auto do_process_bucket =
      [dpp, &bucket_id, this]
      (const std::string& prefix,
       const std::set<rgw_obj_key>& entries_filter) -> int {
	int ret = process_bucket(dpp, bucket_id, prefix, entries_filter);
	if (ret == -ENOENT) {
	  // bucket deletion race?
	  return 0;
	} if (ret < 0) {
	  ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
	    ": ERROR: process_bucket(); bucket_id=" <<
	    bucket_id << " returned ret=" << ret << dendl;
	}

	return ret;
      };

    // either process the whole bucket *or* process the filters and/or
    // the prefixes
    if (process.entire_container) {
      ret = do_process_bucket(empty_prefix, empty_filter);
      if (ret < 0) {
	return ret;
      }
    } else {
      if (! process.filter_keys.empty()) {
	ret = do_process_bucket(empty_prefix, process.filter_keys);
	if (ret < 0) {
	  return ret;
	}
      }
      for (const auto& p : process.prefixes) {
	ret = do_process_bucket(p, empty_filter);
	if (ret < 0) {
	  return ret;
	}
      }
    }
  } // while (! bucket_process_map.empty())

  if (include_rgw_obj_name) {
    return 0;
  }

  // now handle incomplete multipart uploads by going back to the
  // initial bucket

  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->load_bucket(dpp, rgw_bucket(tenant_name, start_bucket_name),
                           &bucket, null_yield);
  if (ret == -ENOENT) {
    // bucket deletion race?
    return 0;
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
      ": ERROR: get_bucket_info returned ret=" << ret << dendl;
    return ret;
  }

  ret = do_incomplete_multipart(dpp, bucket.get());
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
      ": ERROR: do_incomplete_multipart returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
} // RGWRadosList::run(DoutPrefixProvider, string, bool)


int RGWRadosList::do_incomplete_multipart(const DoutPrefixProvider *dpp,
					  rgw::sal::Bucket* bucket)
{
  constexpr int max_uploads = 1000;
  constexpr int max_parts = 1000;
  std::string marker;
  vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  bool is_truncated;
  int ret;

  // use empty strings for params.{prefix,delim}

  do {
    ret = bucket->list_multiparts(dpp, string(), marker, string(), max_uploads, uploads, nullptr, &is_truncated, null_yield);
    if (ret == -ENOENT) {
      // could bucket have been removed while this is running?
      ldpp_dout(dpp, 5) << "RGWRadosList::" << __func__ <<
	": WARNING: call to list_objects of multipart namespace got ENOENT; "
	"assuming bucket removal race" << dendl;
      break;
    } else if (ret < 0) {
      ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
	": ERROR: list_objects op returned ret=" << ret << dendl;
      return ret;
    }

    if (!uploads.empty()) {
      // now process the uploads vector
      for (const auto& upload : uploads) {
	int parts_marker = 0;
	bool is_parts_truncated = false;

	do { // while (is_parts_truncated);
	  ret = upload->list_parts(dpp, store->ctx(), max_parts, parts_marker,
				   &parts_marker, &is_parts_truncated, null_yield);
	  if (ret == -ENOENT) {
	    ldpp_dout(dpp, 5) <<  "RGWRadosList::" << __func__ <<
	      ": WARNING: list_multipart_parts returned ret=-ENOENT "
	      "for " << upload->get_upload_id() << ", moving on" << dendl;
	    break;
	  } else if (ret < 0) {
	    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
	      ": ERROR: list_multipart_parts returned ret=" << ret <<
	      dendl;
	    return ret;
	  }

	  for (auto& p : upload->get_parts()) {
	    rgw::sal::RadosMultipartPart* part =
	      dynamic_cast<rgw::sal::RadosMultipartPart*>(p.second.get());
	    RGWObjManifest& manifest = part->get_manifest();
	    for (auto obj_it = manifest.obj_begin(dpp);
		 obj_it != manifest.obj_end(dpp);
		 ++obj_it) {
	      const rgw_raw_obj& loc =
		obj_it.get_location().get_raw_obj(store->getRados());
	      std::cout << loc.oid << std::endl;
	    } // for (auto obj_it
	  } // for (auto& p
	} while (is_parts_truncated);
      } // for (const auto& upload
    } // if objs not empty
  } while (is_truncated);

  return 0;
} // RGWRadosList::do_incomplete_multipart

void RGWOrphanSearchStage::dump(Formatter *f) const
{
  f->open_object_section("orphan_search_stage");
  string s;
  switch(stage){
  case ORPHAN_SEARCH_STAGE_INIT:
    s = "init";
    break;
  case ORPHAN_SEARCH_STAGE_LSPOOL:
    s = "lspool";
    break;
  case ORPHAN_SEARCH_STAGE_LSBUCKETS:
    s =  "lsbuckets";
    break;
  case ORPHAN_SEARCH_STAGE_ITERATE_BI:
    s = "iterate_bucket_index";
    break;
  case ORPHAN_SEARCH_STAGE_COMPARE:
    s = "comparing";
    break;
  default:
    s = "unknown";
  }
  f->dump_string("search_stage", s);
  f->dump_int("shard",shard);
  f->dump_string("marker",marker);
  f->close_section();
}

void RGWOrphanSearchInfo::dump(Formatter *f) const
{
  f->open_object_section("orphan_search_info");
  f->dump_string("job_name", job_name);
  encode_json("pool", pool, f);
  f->dump_int("num_shards", num_shards);
  encode_json("start_time", start_time, f);
  f->close_section();
}

void RGWOrphanSearchState::dump(Formatter *f) const
{
  f->open_object_section("orphan_search_state");
  encode_json("info", info, f);
  encode_json("stage", stage, f);
  f->close_section();
}


