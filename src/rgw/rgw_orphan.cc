

#include <string>

using namespace std;

#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_rados.h"
#include "rgw_orphan.h"

#define dout_subsys ceph_subsys_rgw

#define DEFAULT_NUM_SHARDS 10

int RGWOrphanStore::read_job(const string& job_name, RGWOrphanSearchState& state)
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

int RGWOrphanStore::init()
{
  const char *log_pool = store->get_zone_params().log_pool.name.c_str();
  librados::Rados *rados = store->get_rados();
  int r = rados->ioctx_create(log_pool, ioctx);
  if (r < 0) {
    cerr << "ERROR: failed to open log pool ret=" << r << std::endl;
    return r;
  }



  return 0;
}

int RGWOrphanStore::store_entries(const string& oid, map<string, bufferlist> entries)
{
  librados::ObjectWriteOperation op;
  op.omap_set(entries);
  cout << "storing " << entries.size() << " entries at " << oid << std::endl;
  int ret = ioctx.operate(oid, &op);
  if (ret < 0) {
    cerr << "ERROR: " << __func__ << "(" << oid << ") returned ret=" << ret << std::endl;
  }
  
  return 0;
}

int RGWOrphanSearch::init(const string& job_name, RGWOrphanSearchInfo *info) {
  int r = orphan_store.init();
  if (r < 0) {
    return r;
  }

  if (info) {
    search_info = *info;
    search_info.job_name = job_name;
    search_info.num_shards = (info->num_shards ? info->num_shards : DEFAULT_NUM_SHARDS);
    search_state = ORPHAN_SEARCH_INIT;
    r = save_state();
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed to write state ret=" << r << dendl;
      return r;
    }
  } else {
    RGWOrphanSearchState state;
    r = orphan_store.read_job(job_name, state);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed to read state ret=" << r << dendl;
      return r;
    }

    search_info = state.info;
    search_state = state.state;
  }

  index_objs_prefix = RGW_ORPHAN_INDEX_PREFIX + string(".");
  index_objs_prefix += job_name;

  for (int i = 0; i < search_info.num_shards; i++) {
    char buf[128];

    snprintf(buf, sizeof(buf), "%s.rados.%d", index_objs_prefix.c_str(), i);
    all_objs_index[i] = buf;

    snprintf(buf, sizeof(buf), "%s.buckets.%d", index_objs_prefix.c_str(), i);
    buckets_instance_index[i] = buf;
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
  librados::Rados *rados = store->get_rados();

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

  cout << "logging all objects in the pool" << std::endl;

  for (; i != i_end; ++i) {
    string nspace = i->get_nspace();
    string oid = i->get_oid();
    string locator = i->get_locator();

    string name = oid;
    if (locator.size())
      name += " (@" + locator + ")";  

    ssize_t pos = oid.find('_');
    if (pos < 0) {
      cerr << "ERROR: object does not have a bucket marker: " << oid << std::endl;
    }
    string obj_marker = oid.substr(0, pos);

    int shard = orphan_shard(oid);
    oids[shard].push_back(oid);

#define COUNT_BEFORE_FLUSH 1000
    if (++count >= COUNT_BEFORE_FLUSH) {
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
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  map<int, list<string> > instances;

  bool truncated;

  RGWObjectCtx obj_ctx(store);

  int count = 0;

  do {
    list<string> keys;
    ret = store->meta_mgr->list_keys_next(handle, max, keys, &truncated);
    if (ret < 0) {
      cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      // ssize_t pos = iter->find(':');
      // string bucket_id = iter->substr(pos + 1);

      int shard = orphan_shard(*iter);
      instances[shard].push_back(*iter);

      if (++count >= COUNT_BEFORE_FLUSH) {
        ret = log_oids(buckets_instance_index, instances);
        if (ret < 0) {
          cerr << __func__ << ": ERROR: log_oids() returned ret=" << ret << std::endl;
          return ret;
        }
        count = 0;
        instances.clear();
      }
    }
  } while (truncated);

  store->meta_mgr->list_keys_complete(handle);

  return 0;
}

int RGWOrphanSearch::run()
{
  int r;

  switch (search_state) {
    
    case ORPHAN_SEARCH_INIT:
      ldout(store->ctx(), 0) << __func__ << "(): initializing state" << dendl;
      search_state = ORPHAN_SEARCH_LSPOOL;
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through
    case ORPHAN_SEARCH_LSPOOL:
      ldout(store->ctx(), 0) << __func__ << "(): building index of all objects in pool" << dendl;
      r = build_all_oids_index();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: build_all_objs_index returnr ret=" << r << dendl;
        return r;
      }

      search_state = ORPHAN_SEARCH_LSBUCKETS;
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through

    case ORPHAN_SEARCH_LSBUCKETS:
      ldout(store->ctx(), 0) << __func__ << "(): building index of all bucket indexes" << dendl;
      r = build_buckets_instance_index();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: build_all_objs_index returnr ret=" << r << dendl;
        return r;
      }

      search_state = ORPHAN_SEARCH_ITERATE_BI;
      r = save_state();
      if (r < 0) {
        lderr(store->ctx()) << __func__ << ": ERROR: failed to save state, ret=" << r << dendl;
        return r;
      }
      // fall through


    case ORPHAN_SEARCH_ITERATE_BI:
    case ORPHAN_SEARCH_DONE:
      break;

    default:
      assert(0);
  };

  return 0;
}


