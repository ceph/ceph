#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>

#include "common/errno.h"
#include "common/Formatter.h"

#include "rgw_rados.h"
#include "rgw_cache.h"
#include "rgw_acl.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"

#include "rgw_tools.h"

#include "common/Clock.h"

#include "include/rados/librados.hpp"
using namespace librados;

#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <map>
#include "auth/Crypto.h" // get_random_bytes()

#include "rgw_log.h"

#include "rgw_gc.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static RGWCache<RGWRados> cached_rados_provider;
static RGWRados rados_provider;

static string notify_oid_prefix = "notify";
static string *notify_oids = NULL;
static string shadow_ns = "shadow";
static string dir_oid_prefix = ".dir.";
static string default_storage_pool = ".rgw.buckets";
static string avail_pools = ".pools.avail";

static string cluster_info_oid = "cluster_info";


static RGWObjCategory shadow_category = RGW_OBJ_CATEGORY_SHADOW;
static RGWObjCategory main_category = RGW_OBJ_CATEGORY_MAIN;

#define RGW_USAGE_OBJ_PREFIX "usage."

#define RGW_DEFAULT_CLUSTER_ROOT_POOL ".rgw.root"


#define dout_subsys ceph_subsys_rgw

void RGWRadosParams::init_default()
{
  domain_root = ".rgw";
  control_pool = ".rgw.control";
  gc_pool = ".rgw.gc";
  log_pool = ".log";
  intent_log_pool = ".intent-log";
  usage_log_pool = ".usage";
  user_keys_pool = ".users";
  user_email_pool = ".users.email";
  user_swift_pool = ".users.swift";
  user_uid_pool = ".users.uid";
}

void RGWRadosParams::dump(Formatter *f) const
{
  f->open_object_section("cluster");
  f->dump_string("domain_root", domain_root.pool);
  f->dump_string("control_pool", control_pool.pool);
  f->dump_string("gc_pool", gc_pool.pool);
  f->dump_string("log_pool", log_pool.pool);
  f->dump_string("intent_log_pool", intent_log_pool.pool);
  f->dump_string("usage_log_pool", usage_log_pool.pool);
  f->dump_string("user_keys_pool", user_keys_pool.pool);
  f->dump_string("user_email_pool", user_email_pool.pool);
  f->dump_string("user_swift_pool", user_swift_pool.pool);
  f->dump_string("user_uid_pool ", user_uid_pool.pool);
  f->close_section();
}

int RGWRadosParams::init(CephContext *cct, RGWRados *store)
{
  string pool_name = cct->_conf->rgw_cluster_root_pool;
  if (pool_name.empty())
    pool_name = RGW_DEFAULT_CLUSTER_ROOT_POOL;

  rgw_bucket pool(pool_name.c_str());
  bufferlist bl;

  int ret = rgw_get_obj(store, NULL, pool, cluster_info_oid, bl);
  if (ret == -ENOENT) {
    init_default();
    return 0; // don't try to store obj, we're not fully initialized yet
  }
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*this, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: failed to decode cluster info from " << pool << ":" << cluster_info_oid << dendl;
    return -EIO;
  }

  return 0;
}

void RGWObjManifest::append(RGWObjManifest& m)
{
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  uint64_t base = obj_size;
  for (iter = m.objs.begin(); iter != m.objs.end(); ++iter) {
    RGWObjManifestPart& part = iter->second;
    objs[base + iter->first] = part;
  }
  obj_size += m.obj_size;
}

class RGWWatcher : public librados::WatchCtx {
  RGWRados *rados;
public:
  RGWWatcher(RGWRados *r) : rados(r) {}
  void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    ldout(rados->ctx(), 10) << "RGWWatcher::notify() opcode=" << (int)opcode << " ver=" << ver << " bl.length()=" << bl.length() << dendl;
    rados->watch_cb(opcode, ver, bl);
  }
};

RGWObjState *RGWRadosCtx::get_state(rgw_obj& obj) {
  if (obj.object.size()) {
    return &objs_state[obj];
  } else {
    rgw_obj new_obj(store->params.domain_root, obj.bucket.name);
    return &objs_state[new_obj];
  }
}

void RGWRadosCtx::set_atomic(rgw_obj& obj) {
  if (obj.object.size()) {
    objs_state[obj].is_atomic = true;
  } else {
    rgw_obj new_obj(store->params.domain_root, obj.bucket.name);
    objs_state[new_obj].is_atomic = true;
  }
}

void RGWRadosCtx::set_prefetch_data(rgw_obj& obj) {
  if (obj.object.size()) {
    objs_state[obj].prefetch_data = true;
  } else {
    rgw_obj new_obj(store->params.domain_root, obj.bucket.name);
    objs_state[new_obj].prefetch_data = true;
  }
}

void RGWRados::finalize()
{
  if (use_gc_thread) {
    gc->stop_processor();
    delete gc;
    gc = NULL;
  }
}

/** 
 * Initialize the RADOS instance and prepare to do other ops
 * Returns 0 on success, -ERR# on failure.
 */
int RGWRados::initialize()
{
  int ret;

  rados = new Rados();
  if (!rados)
    return -ENOMEM;

  ret = rados->init_with_context(cct);
  if (ret < 0)
   return ret;

  ret = rados->connect();
  if (ret < 0)
   return ret;

  params.init(cct, this);

  ret = open_root_pool_ctx();
  if (ret < 0)
    return ret;

  ret = open_gc_pool_ctx();
  if (ret < 0)
    return ret;

  pools_initialized = true;

  gc = new RGWGC();
  gc->initialize(cct, this);

  if (use_gc_thread)
    gc->start_processor();

  return ret;
}

void RGWRados::finalize_watch()
{
  for (int i = 0; i < num_watchers; i++) {
    string& notify_oid = notify_oids[i];
    if (notify_oid.empty())
      continue;
    uint64_t watch_handle = watch_handles[i];
    control_pool_ctx.unwatch(notify_oid, watch_handle);

    RGWWatcher *watcher = watchers[i];
    delete watcher;
  }

  delete[] notify_oids;
  delete[] watch_handles;
  delete[] watchers;
}

/**
 * Open the pool used as root for this gateway
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::open_root_pool_ctx()
{
  const string& pool = params.domain_root.name;
  const char *pool_str = pool.c_str();
  int r = rados->ioctx_create(pool_str, root_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(pool_str);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rados->ioctx_create(pool_str, root_pool_ctx);
  }

  return r;
}

int RGWRados::open_gc_pool_ctx()
{
  const char *gc_pool = params.gc_pool.name.c_str();
  int r = rados->ioctx_create(gc_pool, gc_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(gc_pool);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rados->ioctx_create(gc_pool, gc_pool_ctx);
  }

  return r;
}

int RGWRados::init_watch()
{
  const char *control_pool = params.control_pool.name.c_str();
  int r = rados->ioctx_create(control_pool, control_pool_ctx);
  if (r == -ENOENT) {
    r = rados->pool_create(control_pool);
    if (r == -EEXIST)
      r = 0;
    if (r < 0)
      return r;

    r = rados->ioctx_create(control_pool, control_pool_ctx);
    if (r < 0)
      return r;
  }

  num_watchers = cct->_conf->rgw_num_control_oids;

  bool compat_oid = (num_watchers == 0);

  if (num_watchers <= 0)
    num_watchers = 1;

  notify_oids = new string[num_watchers];
  watchers = new RGWWatcher *[num_watchers];
  watch_handles = new uint64_t[num_watchers];

  for (int i=0; i < num_watchers; i++) {
    string& notify_oid = notify_oids[i];
    notify_oid = notify_oid_prefix;
    if (!compat_oid) {
      char buf[16];
      snprintf(buf, sizeof(buf), ".%d", i);
      notify_oid.append(buf);
    }
    r = control_pool_ctx.create(notify_oid, false);
    if (r < 0 && r != -EEXIST)
      return r;

    RGWWatcher *watcher = new RGWWatcher(this);
    watchers[i] = watcher;

    r = control_pool_ctx.watch(notify_oid, 0, &watch_handles[i], watcher);
    if (r < 0)
      return r;
  }

  return 0;
}

void RGWRados::pick_control_oid(const string& key, string& notify_oid)
{
  uint32_t r = ceph_str_hash_linux(key.c_str(), key.size());

  int i = r % num_watchers;
  char buf[16];
  snprintf(buf, sizeof(buf), ".%d", i);

  notify_oid = notify_oid_prefix;
  notify_oid.append(buf);
}

int RGWRados::open_bucket_ctx(rgw_bucket& bucket, librados::IoCtx&  io_ctx)
{
  int r = rados->ioctx_create(bucket.pool.c_str(), io_ctx);
  if (r != -ENOENT)
    return r;

  if (!pools_initialized)
    return r;

  /* couldn't find bucket, might be a racing bucket creation,
     where client haven't gotten updated map, try to read
     the bucket object .. which will trigger update of osdmap
     if that is the case */
  time_t mtime;
  r = root_pool_ctx.stat(bucket.name, NULL, &mtime);
  if (r < 0)
    return -ENOENT;

  r = rados->ioctx_create(bucket.pool.c_str(), io_ctx);

  return r;
}

/**
 * set up a bucket listing.
 * handle is filled in.
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_init(RGWAccessHandle *handle)
{
  librados::ObjectIterator *state = new librados::ObjectIterator(root_pool_ctx.objects_begin());
  *handle = (RGWAccessHandle)state;
  return 0;
}

/** 
 * get the next bucket in the listing.
 * obj is filled in,
 * handle is updated.
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::list_buckets_next(RGWObjEnt& obj, RGWAccessHandle *handle)
{
  librados::ObjectIterator *state = (librados::ObjectIterator *)*handle;

  do {
    if (*state == root_pool_ctx.objects_end()) {
      delete state;
      return -ENOENT;
    }

    obj.name = (*state)->first;
    (*state)++;
  } while (obj.name[0] == '.');

  /* FIXME: should read mtime/size vals for bucket */

  return 0;
}


/**** logs ****/

struct log_list_state {
  string prefix;
  librados::IoCtx io_ctx;
  librados::ObjectIterator obit;
};

int RGWRados::log_list_init(const string& prefix, RGWAccessHandle *handle)
{
  log_list_state *state = new log_list_state;
  const char *log_pool = params.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->prefix = prefix;
  state->obit = state->io_ctx.objects_begin();
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_list_next(RGWAccessHandle handle, string *name)
{
  log_list_state *state = (log_list_state *)handle;
  while (true) {
    if (state->obit == state->io_ctx.objects_end()) {
      delete state;
      return -ENOENT;
    }
    if (state->prefix.length() &&
	state->obit->first.find(state->prefix) != 0) {
      state->obit++;
      continue;
    }
    *name = state->obit->first;
    state->obit++;
    break;
  }
  return 0;
}

int RGWRados::log_remove(const string& name)
{
  librados::IoCtx io_ctx;
  const char *log_pool = params.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, io_ctx);
  if (r < 0)
    return r;
  return io_ctx.remove(name);
}

struct log_show_state {
  librados::IoCtx io_ctx;
  bufferlist bl;
  bufferlist::iterator p;
  string name;
  uint64_t pos;
  bool eof;
  log_show_state() : pos(0), eof(false) {}
};

int RGWRados::log_show_init(const string& name, RGWAccessHandle *handle)
{
  log_show_state *state = new log_show_state;
  const char *log_pool = params.log_pool.name.c_str();
  int r = rados->ioctx_create(log_pool, state->io_ctx);
  if (r < 0) {
    delete state;
    return r;
  }
  state->name = name;
  *handle = (RGWAccessHandle)state;
  return 0;
}

int RGWRados::log_show_next(RGWAccessHandle handle, rgw_log_entry *entry)
{
  log_show_state *state = (log_show_state *)handle;
  off_t off = state->p.get_off();

  ldout(cct, 10) << "log_show_next pos " << state->pos << " bl " << state->bl.length()
	   << " off " << off
	   << " eof " << (int)state->eof
	   << dendl;
  // read some?
  unsigned chunk = 1024*1024;
  if ((state->bl.length() - off) < chunk/2 && !state->eof) {
    bufferlist more;
    int r = state->io_ctx.read(state->name, more, chunk, state->pos);
    if (r < 0)
      return r;
    state->pos += r;
    bufferlist old;
    try {
      old.substr_of(state->bl, off, state->bl.length() - off);
    } catch (buffer::error& err) {
      return -EINVAL;
    }
    state->bl.clear();
    state->bl.claim(old);
    state->bl.claim_append(more);
    state->p = state->bl.begin();
    if ((unsigned)r < chunk)
      state->eof = true;
    ldout(cct, 10) << " read " << r << dendl;
  }

  if (state->p.end())
    return 0;  // end of file
  try {
    ::decode(*entry, state->p);
  }
  catch (const buffer::error &e) {
    return -EINVAL;
  }
  return 1;
}

/**
 * usage_log_hash: get usage log key hash, based on name and index
 *
 * Get the usage object name. Since a user may have more than 1
 * object holding that info (multiple shards), we use index to
 * specify that shard number. Once index exceeds max shards it
 * wraps.
 * If name is not being set, results for all users will be returned
 * and index will wrap only after total shards number.
 *
 * @param cct [in] ceph context
 * @param name [in] user name
 * @param hash [out] hash value
 * @param index [in] shard index number 
 */
static void usage_log_hash(CephContext *cct, const string& name, string& hash, uint32_t index)
{
  uint32_t val = index;

  if (!name.empty()) {
    int max_user_shards = max(cct->_conf->rgw_usage_max_user_shards, 1);
    val %= max_user_shards;
    val += ceph_str_hash_linux(name.c_str(), name.size());
  }
  char buf[16];
  int max_shards = max(cct->_conf->rgw_usage_max_shards, 1);
  snprintf(buf, sizeof(buf), RGW_USAGE_OBJ_PREFIX "%u", (unsigned)(val % max_shards));
  hash = buf;
}

int RGWRados::log_usage(map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
  uint32_t index = 0;

  map<string, rgw_usage_log_info> log_objs;

  string hash;
  string last_user;

  /* restructure usage map, cluster by object hash */
  map<rgw_user_bucket, RGWUsageBatch>::iterator iter;
  for (iter = usage_info.begin(); iter != usage_info.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    RGWUsageBatch& info = iter->second;

    if (ub.user.empty()) {
      ldout(cct, 0) << "WARNING: RGWRados::log_usage(): user name empty (bucket=" << ub.bucket << "), skipping" << dendl;
      continue;
    }

    if (ub.user != last_user) {
      /* index *should* be random, but why waste extra cycles
         in most cases max user shards is not going to exceed 1,
         so just incrementing it */
      usage_log_hash(cct, ub.user, hash, index++);
    }
    last_user = ub.user;
    vector<rgw_usage_log_entry>& v = log_objs[hash].entries;

    map<utime_t, rgw_usage_log_entry>::iterator miter;
    for (miter = info.m.begin(); miter != info.m.end(); ++miter) {
      v.push_back(miter->second);
    }
  }

  map<string, rgw_usage_log_info>::iterator liter;

  for (liter = log_objs.begin(); liter != log_objs.end(); ++liter) {
    int r = cls_obj_usage_log_add(liter->first, liter->second);
    if (r < 0)
      return r;
  }
  return 0;
}

int RGWRados::read_usage(string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                         bool *is_truncated, RGWUsageIter& usage_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  uint32_t num = max_entries;
  string hash, first_hash;
  usage_log_hash(cct, user, first_hash, 0);

  if (usage_iter.index) {
    usage_log_hash(cct, user, hash, usage_iter.index);
  } else {
    hash = first_hash;
  }

  usage.clear();

  do {
    map<rgw_user_bucket, rgw_usage_log_entry> ret_usage;
    map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;

    int ret =  cls_obj_usage_log_read(hash, user, start_epoch, end_epoch, num,
                                    usage_iter.read_iter, ret_usage, is_truncated);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

    num -= ret_usage.size();

    for (iter = ret_usage.begin(); iter != ret_usage.end(); ++iter) {
      usage[iter->first].aggregate(iter->second);
    }

next:
    if (!*is_truncated) {
      usage_iter.read_iter.clear();
      usage_log_hash(cct, user, hash, ++usage_iter.index);
    }
  } while (num && !*is_truncated && hash != first_hash);
  return 0;
}

int RGWRados::trim_usage(string& user, uint64_t start_epoch, uint64_t end_epoch)
{
  uint32_t index = 0;
  string hash, first_hash;
  usage_log_hash(cct, user, first_hash, index);

  hash = first_hash;

  do {
    int ret =  cls_obj_usage_log_trim(hash, user, start_epoch, end_epoch);
    if (ret == -ENOENT)
      goto next;

    if (ret < 0)
      return ret;

next:
    usage_log_hash(cct, user, hash, ++index);
  } while (hash != first_hash);

  return 0;
}

int RGWRados::decode_policy(bufferlist& bl, ACLOwner *owner)
{
  bufferlist::iterator i = bl.begin();
  RGWAccessControlPolicy policy(cct);
  try {
    policy.decode_owner(i);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  *owner = policy.get_owner();
  return 0;
}

/** 
 * get listing of the objects in a bucket.
 * bucket: bucket to list contents of
 * max: maximum number of results to return
 * prefix: only return results that match this prefix
 * delim: do not include results that match this string.
 *     Any skipped results will have the matching portion of their name
 *     inserted in common_prefixes with a "true" mark.
 * marker: if filled in, begin the listing with this object.
 * result: the objects are put in here.
 * common_prefixes: if delim is filled in, any matching prefixes are placed
 *     here.
 */
int RGWRados::list_objects(rgw_bucket& bucket, int max, string& prefix, string& delim,
			   string& marker, vector<RGWObjEnt>& result, map<string, bool>& common_prefixes,
			   bool get_content_type, string& ns, bool *is_truncated, RGWAccessListFilter *filter)
{
  int count = 0;
  bool truncated;

  if (bucket_is_system(bucket)) {
    return -EINVAL;
  }
  result.clear();

  rgw_obj marker_obj, prefix_obj;
  marker_obj.set_ns(ns);
  marker_obj.set_obj(marker);
  string cur_marker = marker_obj.object;

  prefix_obj.set_ns(ns);
  prefix_obj.set_obj(prefix);
  string cur_prefix = prefix_obj.object;

  do {
    std::map<string, RGWObjEnt> ent_map;
    int r = cls_bucket_list(bucket, cur_marker, cur_prefix, max - count, ent_map,
                            &truncated, &cur_marker);
    if (r < 0)
      return r;

    std::map<string, RGWObjEnt>::iterator eiter;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      string obj = eiter->first;
      string key = obj;

      if (!rgw_obj::translate_raw_obj_to_obj_in_ns(obj, ns)) {
        if (!ns.empty()) {
          /* we've iterated past the namespace we're searching -- done now */
          truncated = false;
          goto done;
        }

        /* we're not looking at the namespace this object is in, next! */
        continue;
      }

      if (filter && !filter->filter(obj, key))
        continue;

      if (prefix.size() &&  ((obj).compare(0, prefix.size(), prefix) != 0))
        continue;

      if (!delim.empty()) {
        int delim_pos = obj.find(delim, prefix.size());

        if (delim_pos >= 0) {
          common_prefixes[obj.substr(0, delim_pos + 1)] = true;
          continue;
        }
      }

      RGWObjEnt ent = eiter->second;
      ent.name = obj;
      result.push_back(ent);
      count++;
    }
  } while (truncated && count < max);

done:
  if (is_truncated)
    *is_truncated = truncated;

  return 0;
}

/**
 * create a rados pool, associated meta info
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_pool(rgw_bucket& bucket) 
{
  int ret = 0;

  ret = rados->pool_create(bucket.pool.c_str(), 0);
  if (ret == -EEXIST)
    ret = 0;
  if (ret < 0)
    return ret;

  bucket.pool = bucket.name;

  return 0;
}
/**
 * create a bucket with name bucket and the given list of attrs
 * returns 0 on success, -ERR# otherwise.
 */
int RGWRados::create_bucket(string& owner, rgw_bucket& bucket, 
			    map<std::string, bufferlist>& attrs, 
			    bool exclusive)
{
  int ret = 0;

  ret = select_bucket_placement(bucket.name, bucket);
  if (ret < 0)
    return ret;
  librados::IoCtx io_ctx; // context for new bucket

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  bufferlist bl;
  uint32_t nop = 0;
  ::encode(nop, bl);

  const string& pool = params.domain_root.name;
  const char *pool_str = pool.c_str();
  librados::IoCtx id_io_ctx;
  r = rados->ioctx_create(pool_str, id_io_ctx);
  if (r < 0)
    return r;

  uint64_t iid = instance_id();
  uint64_t bid = next_bucket_id();
  char buf[32];
  snprintf(buf, sizeof(buf), "%llu.%llu", (long long)iid, (long long)bid); 
  bucket.marker = buf;
  bucket.bucket_id = bucket.marker;

  string dir_oid =  dir_oid_prefix;
  dir_oid.append(bucket.marker);

  librados::ObjectWriteOperation op;
  op.create(true);
  r = cls_rgw_init_index(io_ctx, op, dir_oid);
  if (r < 0 && r != -EEXIST)
    return r;

  RGWBucketInfo info;
  info.bucket = bucket;
  info.owner = owner;
  ret = store_bucket_info(info, &attrs, exclusive);
  if (ret == -EEXIST) {
    io_ctx.remove(dir_oid);
  }

  return ret;
}

int RGWRados::store_bucket_info(RGWBucketInfo& info, map<string, bufferlist> *pattrs, bool exclusive)
{
  bufferlist bl;
  ::encode(info, bl);

  int ret = rgw_put_system_obj(this, params.domain_root, info.bucket.name, bl.c_str(), bl.length(), exclusive, pattrs);
  if (ret < 0)
    return ret;

  ldout(cct, 20) << "store_bucket_info: bucket=" << info.bucket << " owner " << info.owner << dendl;
  return 0;
}


int RGWRados::select_bucket_placement(string& bucket_name, rgw_bucket& bucket)
{
  bufferlist map_bl;
  map<string, bufferlist> m;
  string pool_name;
  bool write_map = false;

  rgw_obj obj(params.domain_root, avail_pools);

  int ret = rgw_get_obj(this, NULL, params.domain_root, avail_pools, map_bl);
  if (ret < 0) {
    goto read_omap;
  }

  try {
    bufferlist::iterator iter = map_bl.begin();
    ::decode(m, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

read_omap:
  if (!m.size()) {
    bufferlist header;
    ret = omap_get_all(obj, header, m);

    write_map = true;
  }

  if (ret < 0 || !m.size()) {
    vector<string> names;
    names.push_back(default_storage_pool);
    vector<int> retcodes;
    bufferlist bl;
    ret = create_pools(names, retcodes);
    if (ret < 0)
      return ret;
    ret = omap_set(obj, default_storage_pool, bl);
    if (ret < 0)
      return ret;
    m[default_storage_pool] = bl;
  }

  if (write_map) {
    bufferlist new_bl;
    ::encode(m, new_bl);
    ret = put_obj_data(NULL, obj, new_bl.c_str(), -1, new_bl.length(), false);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
    }
  }

  map<string, bufferlist>::iterator miter;
  if (m.size() > 1) {
    vector<string> v;
    for (miter = m.begin(); miter != m.end(); ++miter) {
      v.push_back(miter->first);
    }

    uint32_t r;
    ret = get_random_bytes((char *)&r, sizeof(r));
    if (ret < 0)
      return ret;

    int i = r % v.size();
    pool_name = v[i];
  } else {
    miter = m.begin();
    pool_name = miter->first;
  }
  bucket.pool = pool_name;
  bucket.name = bucket_name;

  return 0;

}

int RGWRados::update_placement_map()
{
  bufferlist header;
  map<string, bufferlist> m;
  rgw_obj obj(params.domain_root, avail_pools);
  int ret = omap_get_all(obj, header, m);
  if (ret < 0)
    return ret;

  bufferlist new_bl;
  ::encode(m, new_bl);
  ret = put_obj_data(NULL, obj, new_bl.c_str(), -1, new_bl.length(), false);
  if (ret < 0) {
    ldout(cct, 0) << "WARNING: could not save avail pools map info ret=" << ret << dendl;
  }

  return ret;
}

int RGWRados::add_bucket_placement(std::string& new_pool)
{
  int ret = rados->pool_lookup(new_pool.c_str());
  if (ret < 0) // DNE, or something
    return ret;

  rgw_obj obj(params.domain_root, avail_pools);
  bufferlist empty_bl;
  ret = omap_set(obj, new_pool, empty_bl);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::remove_bucket_placement(std::string& old_pool)
{
  rgw_obj obj(params.domain_root, avail_pools);
  int ret = omap_del(obj, old_pool);

  // don't care about return value
  update_placement_map();

  return ret;
}

int RGWRados::list_placement_set(set<string>& names)
{
  bufferlist header;
  map<string, bufferlist> m;
  string pool_name;

  rgw_obj obj(params.domain_root, avail_pools);
  int ret = omap_get_all(obj, header, m);
  if (ret < 0)
    return ret;

  names.clear();
  map<string, bufferlist>::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    names.insert(miter->first);
  }

  return names.size();
}

int RGWRados::create_pools(vector<string>& names, vector<int>& retcodes)
{
  vector<string>::iterator iter;
  vector<librados::PoolAsyncCompletion *> completions;
  vector<int> rets;

  for (iter = names.begin(); iter != names.end(); ++iter) {
    librados::PoolAsyncCompletion *c = librados::Rados::pool_async_create_completion();
    completions.push_back(c);
    string& name = *iter;
    int ret = rados->pool_create_async(name.c_str(), c);
    rets.push_back(ret);
  }

  vector<int>::iterator riter;
  vector<librados::PoolAsyncCompletion *>::iterator citer;

  assert(rets.size() == completions.size());
  for (riter = rets.begin(), citer = completions.begin(); riter != rets.end(); ++riter, ++citer) {
    int r = *riter;
    PoolAsyncCompletion *c = *citer;
    if (r == 0) {
      c->wait();
      r = c->get_return_value();
      if (r < 0) {
        ldout(cct, 0) << "WARNING: async pool_create returned " << r << dendl;
      }
      c->release();
    }
    retcodes.push_back(r);
  }
  return 0;
}

/**
 * Write/overwrite an object to the bucket storage.
 * bucket: the bucket to store the object in
 * obj: the object name/key
 * data: the object contents/value
 * size: the amount of data to write (data must be this long)
 * mtime: if non-NULL, writes the given mtime to the bucket storage
 * attrs: all the given attrs are written to bucket storage for the given object
 * exclusive: create object exclusively
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_meta(void *ctx, rgw_obj& obj,  uint64_t size,
                  time_t *mtime, map<string, bufferlist>& attrs,
                  RGWObjCategory category, int flags,
                  map<string, bufferlist>* rmattrs,
                  const bufferlist *data,
                  RGWObjManifest *manifest,
		  const string *ptag,
                  list<string> *remove_objs)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;


  io_ctx.locator_set_key(key);

  ObjectWriteOperation op;

  RGWObjState *state = NULL;

  if (flags & PUT_OBJ_EXCL) {
    if (!(flags & PUT_OBJ_CREATE))
	return -EINVAL;
    op.create(true); // exclusive create
  } else {
    bool reset_obj = (flags & PUT_OBJ_CREATE) != 0;
    r = prepare_atomic_for_write(rctx, obj, op, &state, reset_obj, ptag);
    if (r < 0)
      return r;
  }

  if (data) {
    /* if we want to overwrite the data, we also want to overwrite the
       xattrs, so just remove the object */
    op.write_full(*data);
  }

  string etag;
  string content_type;
  bufferlist acl_bl;

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  if (manifest) {
    /* remove existing manifest attr */
    iter = attrs.find(RGW_ATTR_MANIFEST);
    if (iter != attrs.end())
      attrs.erase(iter);

    bufferlist bl;
    ::encode(*manifest, bl);
    op.setxattr(RGW_ATTR_MANIFEST, bl);
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = bl.c_str();
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = bl.c_str();
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }

  if (!op.size())
    return 0;

  string index_tag;
  uint64_t epoch;
  utime_t ut;
  r = prepare_update_index(NULL, bucket, obj, index_tag);
  if (r < 0)
    return r;

  r = io_ctx.operate(oid, &op);
  if (r < 0)
    goto done_cancel;

  epoch = io_ctx.get_last_version();

  r = complete_atomic_overwrite(rctx, state, obj);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: complete_atomic_overwrite returned r=" << r << dendl;
  }

  ut = ceph_clock_now(cct);
  r = complete_update_index(bucket, obj.object, index_tag, epoch, size,
                            ut, etag, content_type, &acl_bl, category, remove_objs);
  if (r < 0)
    goto done_cancel;


  if (mtime) {
    r = io_ctx.stat(oid, NULL, mtime);
    if (r < 0)
      return r;
  }

  return 0;

done_cancel:
  int ret = complete_update_index_cancel(bucket, obj.object, index_tag);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: complete_update_index_cancel() returned ret=" << ret << dendl;
  }
  return r;
}

/**
 * Write/overwrite an object to the bucket storage.
 * bucket: the bucket to store the object in
 * obj: the object name/key
 * data: the object contents/value
 * offset: the offet to write to in the object
 *         If this is -1, we will overwrite the whole object.
 * size: the amount of data to write (data must be this long)
 * attrs: all the given attrs are written to bucket storage for the given object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::put_obj_data(void *ctx, rgw_obj& obj,
			   const char *data, off_t ofs, size_t len, bool exclusive)
{
  void *handle;
  bufferlist bl;
  bl.append(data, len);
  int r = aio_put_obj_data(ctx, obj, bl, ofs, exclusive, &handle);
  if (r < 0)
    return r;
  return aio_wait(handle);
}

int RGWRados::aio_put_obj_data(void *ctx, rgw_obj& obj, bufferlist& bl,
			       off_t ofs, bool exclusive,
                               void **handle)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  *handle = c;
  
  ObjectWriteOperation op;

  if (exclusive)
    op.create(true);

  if (ofs == -1) {
    op.write_full(bl);
  } else {
    op.write(ofs, bl);
  }
  r = io_ctx.aio_operate(oid, c, &op);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::aio_wait(void *handle)
{
  AioCompletion *c = (AioCompletion *)handle;
  c->wait_for_complete();
  int ret = c->get_return_value();
  c->release();
  return ret;
}

bool RGWRados::aio_completed(void *handle)
{
  AioCompletion *c = (AioCompletion *)handle;
  return c->is_complete();
}
/**
 * Copy an object.
 * dest_obj: the object to copy into
 * src_obj: the object to copy from
 * attrs: if replace_attrs is set then these are placed on the new object
 * err: stores any errors resulting from the get of the original object
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::copy_obj(void *ctx,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
               time_t *mtime,
               const time_t *mod_ptr,
               const time_t *unmod_ptr,
               const char *if_match,
               const char *if_nomatch,
               bool replace_attrs,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               struct rgw_err *err)
{
  int ret;
  uint64_t total_len, obj_size;
  time_t lastmod;
  map<string, bufferlist>::iterator iter;
  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  append_rand_alpha(cct, dest_obj.object, shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  ldout(cct, 5) << "Copy object " << src_obj.bucket << ":" << src_obj.object << " => " << dest_obj.bucket << ":" << dest_obj.object << dendl;

  void *handle = NULL;

  map<string, bufferlist> attrset;
  off_t ofs = 0;
  off_t end = -1;
  ret = prepare_get_obj(ctx, src_obj, &ofs, &end, &attrset,
                mod_ptr, unmod_ptr, &lastmod, if_match, if_nomatch, &total_len, &obj_size, &handle, err);

  if (ret < 0)
    return ret;

  if (replace_attrs) {
    if (!attrs[RGW_ATTR_ETAG].length())
      attrs[RGW_ATTR_ETAG] = attrset[RGW_ATTR_ETAG];

    attrset = attrs;
  } else {
    /* copying attrs from source, however acls should not be copied */
    attrset[RGW_ATTR_ACL] = attrs[RGW_ATTR_ACL];
  }
  attrset.erase(RGW_ATTR_ID_TAG);

  RGWObjManifest manifest;
  RGWObjState *astate = NULL;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  ret = get_obj_state(rctx, src_obj, &astate);
  if (ret < 0)
    return ret;

  vector<rgw_obj> ref_objs;

  bool copy_data = !astate->has_manifest;
  bool copy_first = false;
  if (astate->has_manifest) {
    if (astate->manifest.objs.size() < 2) {
      copy_data = true;
    } else {
      map<uint64_t, RGWObjManifestPart>::iterator iter = astate->manifest.objs.begin();
      RGWObjManifestPart part = iter->second;
      if (part.loc == src_obj) {
	if (part.size > RGW_MAX_CHUNK_SIZE)  // should never happen
	  copy_data = true;
	else
          copy_first = true;
      }
    }
  }

  if (copy_data) { /* refcounting tail wouldn't work here, just copy the data */
    return copy_obj_data(ctx, handle, end, dest_obj, src_obj, mtime, attrset, category, err);
  }

  map<uint64_t, RGWObjManifestPart>::iterator miter = astate->manifest.objs.begin();

  if (copy_first) // we need to copy first chunk, not increase refcount
    ++miter;

  RGWObjManifestPart *first_part = &miter->second;
  string oid, key;
  rgw_bucket bucket;
  get_obj_bucket_and_oid_key(first_part->loc, bucket, oid, key);
  librados::IoCtx io_ctx;

  ret = open_bucket_ctx(bucket, io_ctx);
  if (ret < 0)
    return ret;

  bufferlist first_chunk;

  string tag;
  bool copy_itself = (dest_obj == src_obj);
  RGWObjManifest *pmanifest; 
  ldout(cct, 0) << "dest_obj=" << dest_obj << " src_obj=" << src_obj << " copy_itself=" << (int)copy_itself << dendl;

  if (!copy_itself) {
    append_rand_alpha(cct, tag, tag, 32);

    for (; miter != astate->manifest.objs.end(); ++miter) {
      RGWObjManifestPart& part = miter->second;
      ObjectWriteOperation op;
      manifest.objs[miter->first] = part;
      cls_refcount_get(op, tag, true);

      get_obj_bucket_and_oid_key(part.loc, bucket, oid, key);
      io_ctx.locator_set_key(key);

      ret = io_ctx.operate(oid, &op);
      if (ret < 0)
        goto done_ret;

      ref_objs.push_back(part.loc);
    }
    manifest.obj_size = total_len;

    pmanifest = &manifest;
  } else {
    pmanifest = &astate->manifest;
    tag = astate->obj_tag.c_str();

    /* don't send the object's tail for garbage collection */
    astate->keep_tail = true;
  }

  if (copy_first) {
    ret = get_obj(ctx, &handle, src_obj, first_chunk, 0, RGW_MAX_CHUNK_SIZE);
    if (ret < 0)
      goto done_ret;

    first_part = &pmanifest->objs[0];
    first_part->loc = dest_obj;
    first_part->loc_ofs = 0;
    first_part->size = first_chunk.length();
  }

  ret = put_obj_meta(ctx, dest_obj, end + 1, NULL, attrset, category, PUT_OBJ_CREATE, NULL, &first_chunk, pmanifest, &tag, NULL);

  if (mtime)
    obj_stat(ctx, dest_obj, NULL, mtime, NULL, NULL, NULL);

  return 0;

done_ret:
  if (!copy_itself) {
    vector<rgw_obj>::iterator riter;

    /* rollback reference */
    for (riter = ref_objs.begin(); riter != ref_objs.end(); ++riter) {
      ObjectWriteOperation op;
      cls_refcount_put(op, tag, true);

      get_obj_bucket_and_oid_key(*riter, bucket, oid, key);
      io_ctx.locator_set_key(key);

      int r = io_ctx.operate(oid, &op);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: cleanup after error failed to drop reference on obj=" << *riter << dendl;
      }
    }
  }
  return ret;
}


int RGWRados::copy_obj_data(void *ctx,
	       void *handle, off_t end,
               rgw_obj& dest_obj,
               rgw_obj& src_obj,
	       time_t *mtime,
               map<string, bufferlist>& attrs,
               RGWObjCategory category,
               struct rgw_err *err)
{
  bufferlist first_chunk;
  RGWObjManifest manifest;
  RGWObjManifestPart *first_part;

  rgw_obj shadow_obj = dest_obj;
  string shadow_oid;

  append_rand_alpha(cct, dest_obj.object, shadow_oid, 32);
  shadow_obj.init_ns(dest_obj.bucket, shadow_oid, shadow_ns);

  int ret, r;
  off_t ofs = 0;

  do {
    bufferlist bl;
    ret = get_obj(ctx, &handle, src_obj, bl, ofs, end);
    if (ret < 0)
      return ret;

    const char *data = bl.c_str();

    if (ofs < RGW_MAX_CHUNK_SIZE) {
      off_t len = min(RGW_MAX_CHUNK_SIZE - ofs, (off_t)ret);
      first_chunk.append(data, len);
      ofs += len;
      ret -= len;
      data += len;
    }

    // In the first call to put_obj_data, we pass ofs == -1 so that it will do
    // a write_full, wiping out whatever was in the object before this
    r = 0;
    if (ret > 0) {
      r = put_obj_data(ctx, shadow_obj, data, ((ofs == 0) ? -1 : ofs), ret, false);
    }
    if (r < 0)
      goto done_err;

    ofs += ret;
  } while (ofs <= end);

  first_part = &manifest.objs[0];
  first_part->loc = dest_obj;
  first_part->loc_ofs = 0;
  first_part->size = first_chunk.length();

  if (ofs > RGW_MAX_CHUNK_SIZE) {
    RGWObjManifestPart& tail = manifest.objs[RGW_MAX_CHUNK_SIZE];
    tail.loc = shadow_obj;
    tail.loc_ofs = RGW_MAX_CHUNK_SIZE;
    tail.size = ofs - RGW_MAX_CHUNK_SIZE;
  }
  manifest.obj_size = ofs;

  ret = put_obj_meta(ctx, dest_obj, end + 1, NULL, attrs, category, PUT_OBJ_CREATE, NULL, &first_chunk, &manifest, NULL, NULL);
  if (mtime)
    obj_stat(ctx, dest_obj, NULL, mtime, NULL, NULL, NULL);

  finish_get_obj(&handle);

  return ret;
done_err:
  delete_obj(ctx, shadow_obj);
  finish_get_obj(&handle);
  return r;
}

/**
 * Delete a bucket.
 * bucket: the name of the bucket to delete
 * Returns 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_bucket(rgw_bucket& bucket)
{
  librados::IoCtx list_ctx;
  int r = open_bucket_ctx(bucket, list_ctx);
  if (r < 0)
    return r;

  std::map<string, RGWObjEnt> ent_map;
  string marker, prefix;
  bool is_truncated;

  do {
#define NUM_ENTRIES 1000
    r = cls_bucket_list(bucket, marker, prefix, NUM_ENTRIES, ent_map,
                        &is_truncated, &marker);
    if (r < 0)
      return r;

    string ns;
    std::map<string, RGWObjEnt>::iterator eiter;
    string obj;
    for (eiter = ent_map.begin(); eiter != ent_map.end(); ++eiter) {
      obj = eiter->first;

      if (rgw_obj::translate_raw_obj_to_obj_in_ns(obj, ns))
        return -ENOTEMPTY;
    }
  } while (is_truncated);

  rgw_obj obj(params.domain_root, bucket.name);
  r = delete_obj(NULL, obj);
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  op.remove();
  string oid = dir_oid_prefix;
  oid.append(bucket.marker);
  librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);
  r = list_ctx.aio_operate(oid, completion, &op);
  completion->release();
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::set_buckets_enabled(vector<rgw_bucket>& buckets, bool enabled)
{
  int ret = 0;

  vector<rgw_bucket>::iterator iter;

  for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
    rgw_bucket& bucket = *iter;
    if (enabled)
      ldout(cct, 20) << "enabling bucket name=" << bucket.name << dendl;
    else
      ldout(cct, 20) << "disabling bucket name=" << bucket.name << dendl;

    RGWBucketInfo info;
    map<string, bufferlist> attrs;
    int r = get_bucket_info(NULL, bucket.name, info, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
    if (enabled) {
      info.flags &= ~BUCKET_SUSPENDED;
    } else {
      info.flags |= BUCKET_SUSPENDED;
    }

    r = put_bucket_info(bucket.name, info, false, &attrs);
    if (r < 0) {
      ldout(cct, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
      ret = r;
      continue;
    }
  }
  return ret;
}

int RGWRados::bucket_suspended(rgw_bucket& bucket, bool *suspended)
{
  RGWBucketInfo bucket_info;
  int ret = get_bucket_info(NULL, bucket.name, bucket_info);
  if (ret < 0) {
    return ret;
  }

  *suspended = ((bucket_info.flags & BUCKET_SUSPENDED) != 0);
  return 0;
}

int RGWRados::complete_atomic_overwrite(RGWRadosCtx *rctx, RGWObjState *state, rgw_obj& obj)
{
  if (!state || !state->has_manifest || state->keep_tail)
    return 0;

  cls_rgw_obj_chain chain;
  map<uint64_t, RGWObjManifestPart>::iterator iter;
  for (iter = state->manifest.objs.begin(); iter != state->manifest.objs.end(); ++iter) {
    rgw_obj& mobj = iter->second.loc;
    if (mobj == obj)
      continue;
    string oid, key;
    rgw_bucket bucket;
    get_obj_bucket_and_oid_key(mobj, bucket, oid, key);
    chain.push_obj(bucket.pool, oid, key);
  }

  string tag = state->obj_tag.c_str();
  int ret = gc->send_chain(chain, tag, false);  // do it async

  return ret;
}

int RGWRados::open_bucket(rgw_bucket& bucket, librados::IoCtx& io_ctx, string& bucket_oid)
{
  if (bucket_is_system(bucket))
    return -EINVAL;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  if (bucket.marker.empty()) {
    ldout(cct, 0) << "ERROR: empty marker for bucket operation" << dendl;
    return -EIO;
  }

  bucket_oid = dir_oid_prefix;
  bucket_oid.append(bucket.marker);

  return 0;
}

static void translate_raw_stats(rgw_bucket_dir_header& header, map<RGWObjCategory, RGWBucketStats>& stats)
{
  map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.begin();
  for (; iter != header.stats.end(); ++iter) {
    RGWObjCategory category = (RGWObjCategory)iter->first;
    RGWBucketStats& s = stats[category];
    struct rgw_bucket_category_stats& header_stats = iter->second;
    s.category = (RGWObjCategory)iter->first;
    s.num_kb = ((header_stats.total_size + 1023) / 1024);
    s.num_kb_rounded = ((header_stats.total_size_rounded + 1023) / 1024);
    s.num_objects = header_stats.num_entries;
  }
}

int RGWRados::bucket_check_index(rgw_bucket& bucket,
				 map<RGWObjCategory, RGWBucketStats> *existing_stats,
				 map<RGWObjCategory, RGWBucketStats> *calculated_stats)
{
  librados::IoCtx io_ctx;
  string oid;

  int ret = open_bucket(bucket, io_ctx, oid);
  if (ret < 0)
    return ret;

  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calculated_header;

  ret = cls_rgw_bucket_check_index_op(io_ctx, oid, &existing_header, &calculated_header);
  if (ret < 0)
    return ret;

  translate_raw_stats(existing_header, *existing_stats);
  translate_raw_stats(calculated_header, *calculated_stats);

  return 0;
}

int RGWRados::bucket_rebuild_index(rgw_bucket& bucket)
{
  librados::IoCtx io_ctx;
  string oid;

  int ret = open_bucket(bucket, io_ctx, oid);
  if (ret < 0)
    return ret;

  return cls_rgw_bucket_rebuild_index_op(io_ctx, oid);
}


int RGWRados::defer_gc(void *ctx, rgw_obj& obj)
{
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  if (!rctx)
    return 0;

  RGWObjState *state = NULL;

  int r = get_obj_state(rctx, obj, &state);
  if (r < 0)
    return r;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for obj=" << obj << " is not atomic, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  if (state->obj_tag.length() == 0) {// check for backward compatibility
    ldout(cct, 20) << "state->obj_tag is empty, not deferring gc operation" << dendl;
    return -EINVAL;
  }

  string tag = state->obj_tag.c_str();

  ldout(cct, 0) << "defer chain tag=" << tag << dendl;

  return gc->defer_chain(tag, false);
}


/**
 * Delete an object.
 * bucket: name of the bucket storing the object
 * obj: name of the object to delete
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::delete_obj_impl(void *ctx, rgw_obj& obj)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectWriteOperation op;

  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, obj, op, &state, false, NULL);
  if (r < 0)
    return r;

  bool ret_not_existed = (state && !state->exists);

  string tag;
  r = prepare_update_index(state, bucket, obj, tag);
  if (r < 0)
    return r;
  cls_refcount_put(op, tag, true);
  r = io_ctx.operate(oid, &op);
  bool removed = (r >= 0);

  if (r >= 0 || r == -ENOENT) {
    uint64_t epoch = io_ctx.get_last_version();
    r = complete_update_index_del(bucket, obj.object, tag, epoch);
  } else {
    int ret = complete_update_index_cancel(bucket, obj.object, tag);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: complete_update_index_cancel returned ret=" << ret << dendl;
    }
  }
  if (removed) {
    int ret = complete_atomic_overwrite(rctx, state, obj);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: complete_atomic_removal returned ret=" << ret << dendl;
    }
    /* other than that, no need to propagate error */
  }

  atomic_write_finish(state, r);

  if (r < 0)
    return r;

  if (ret_not_existed)
    return -ENOENT;

  return 0;
}

int RGWRados::delete_obj(void *ctx, rgw_obj& obj)
{
  int r;

  r = delete_obj_impl(ctx, obj);
  if (r == -ECANCELED)
    r = 0;

  return r;
}

int RGWRados::delete_obj_index(rgw_obj& obj)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

  string tag;
  int r = complete_update_index_del(bucket, obj.object, tag, 0);

  return r;
}

static void generate_fake_tag(CephContext *cct, map<string, bufferlist>& attrset, RGWObjManifest& manifest, bufferlist& manifest_bl, bufferlist& tag_bl)
{
  string tag;

  map<uint64_t, RGWObjManifestPart>::iterator mi = manifest.objs.begin();
  if (mi != manifest.objs.end()) {
    if (manifest.objs.size() > 1) // first object usually points at the head, let's skip to a more unique part
      ++mi;
    tag = mi->second.loc.object;
    tag.append("_");
  }

  unsigned char md5[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char md5_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  MD5 hash;
  hash.Update((const byte *)manifest_bl.c_str(), manifest_bl.length());

  map<string, bufferlist>::iterator iter = attrset.find(RGW_ATTR_ETAG);
  if (iter != attrset.end()) {
    bufferlist& bl = iter->second;
    hash.Update((const byte *)bl.c_str(), bl.length());
  }

  hash.Final(md5);
  buf_to_hex(md5, CEPH_CRYPTO_MD5_DIGESTSIZE, md5_str);
  tag.append(md5_str);

  ldout(cct, 10) << "generate_fake_tag new tag=" << tag << dendl;

  tag_bl.append(tag.c_str(), tag.size() + 1);
}

int RGWRados::get_obj_state(RGWRadosCtx *rctx, rgw_obj& obj, RGWObjState **state)
{
  RGWObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs)
    return 0;

  int r = obj_stat(rctx, obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL));
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = 0;
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;
  map<string, bufferlist>::iterator iter = s->attrset.find(RGW_ATTR_SHADOW_OBJ);
  if (iter != s->attrset.end()) {
    bufferlist bl = iter->second;
    bufferlist::iterator it = bl.begin();
    it.copy(bl.length(), s->shadow_obj);
    s->shadow_obj[bl.length()] = '\0';
  }
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];
  bufferlist manifest_bl = s->attrset[RGW_ATTR_MANIFEST];
  if (manifest_bl.length()) {
    bufferlist::iterator miter = manifest_bl.begin();
    try {
      ::decode(s->manifest, miter);
      s->has_manifest = true;
      s->size = s->manifest.obj_size;
    } catch (buffer::error& err) {
      ldout(cct, 20) << "ERROR: couldn't decode manifest" << dendl;
      return -EIO;
    }
    ldout(cct, 10) << "manifest: total_size = " << s->manifest.obj_size << dendl;
    map<uint64_t, RGWObjManifestPart>::iterator mi;
    for (mi = s->manifest.objs.begin(); mi != s->manifest.objs.end(); ++mi) {
      ldout(cct, 10) << "manifest: ofs=" << mi->first << " loc=" << mi->second.loc << dendl;
    }

    if (!s->obj_tag.length()) {
      /*
       * Uh oh, something's wrong, object with manifest should have tag. Let's
       * create one out of the manifest, would be unique
       */
      generate_fake_tag(cct, s->attrset, s->manifest, manifest_bl, s->obj_tag);
      s->fake_tag = true;
    }
  }
  if (s->obj_tag.length())
    ldout(cct, 20) << "get_obj_state: setting s->obj_tag to " << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_obj_state: s->obj_tag was set empty" << dendl;
  return 0;
}

/**
 * Get the attributes for an object.
 * bucket: name of the bucket holding the object.
 * obj: name of the object
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::get_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& dest)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  rgw_bucket actual_bucket = bucket;
  string actual_obj = oid;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  if (actual_obj.size() == 0) {
    actual_obj = bucket.name;
    actual_bucket = params.domain_root;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  if (rctx) {
    RGWObjState *state;
    r = get_obj_state(rctx, obj, &state);
    if (r < 0)
      return r;
    if (!state->exists)
      return -ENOENT;
    if (state->get_attr(name, dest))
      return 0;
    return -ENODATA;
  }
  
  r = io_ctx.getxattr(actual_obj, name, dest);
  if (r < 0)
    return r;

  return 0;
}

int RGWRados::append_atomic_test(RGWRadosCtx *rctx, rgw_obj& obj,
                            ObjectOperation& op, RGWObjState **pstate)
{
  if (!rctx)
    return 0;

  int r = get_obj_state(rctx, obj, pstate);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  if (!state->is_atomic) {
    ldout(cct, 20) << "state for obj=" << obj << " is not atomic, not appending atomic test" << dendl;
    return 0;
  }

  if (state->obj_tag.length() > 0 && !state->fake_tag) {// check for backward compatibility
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
  } else {
    ldout(cct, 20) << "state->obj_tag is empty, not appending atomic test" << dendl;
  }
  return 0;
}

int RGWRados::prepare_atomic_for_write_impl(RGWRadosCtx *rctx, rgw_obj& obj,
                            ObjectWriteOperation& op, RGWObjState **pstate,
			    bool reset_obj, const string *ptag)
{
  int r = get_obj_state(rctx, obj, pstate);
  if (r < 0)
    return r;

  RGWObjState *state = *pstate;

  bool need_guard = (state->has_manifest || (state->obj_tag.length() != 0)) && (!state->fake_tag);

  if (!state->is_atomic) {
    ldout(cct, 20) << "prepare_atomic_for_write_impl: state is not atomic. state=" << (void *)state << dendl;

    if (reset_obj) {
      op.create(false);
      op.remove(); // we're not dropping reference here, actually removing object
    }

    return 0;
  }

  if (state->obj_tag.length() == 0 ||
      state->shadow_obj.size() == 0) {
    ldout(cct, 10) << "can't clone object " << obj << " to shadow object, tag/shadow_obj haven't been set" << dendl;
    // FIXME: need to test object does not exist
  } else if (state->has_manifest) {
    ldout(cct, 10) << "obj contains manifest" << dendl;
  } else if (state->size <= RGW_MAX_CHUNK_SIZE) {
    ldout(cct, 10) << "not cloning object, object size (" << state->size << ")" << " <= chunk size" << dendl;
  } else {
    ldout(cct, 10) << "cloning object " << obj << " to name=" << state->shadow_obj << dendl;
    rgw_obj dest_obj(obj.bucket, state->shadow_obj);
    dest_obj.set_ns(shadow_ns);
    if (obj.key.size())
      dest_obj.set_key(obj.key);
    else
      dest_obj.set_key(obj.object);

    pair<string, bufferlist> cond(RGW_ATTR_ID_TAG, state->obj_tag);
    ldout(cct, 10) << "cloning: dest_obj=" << dest_obj << " size=" << state->size << " tag=" << state->obj_tag.c_str() << dendl;
    r = clone_obj_cond(NULL, dest_obj, 0, obj, 0, state->size, state->attrset, shadow_category, &state->mtime, false, true, &cond);
    if (r == -EEXIST)
      r = 0;
    if (r == -ECANCELED) {
      /* we lost in a race here, original object was replaced, we assume it was cloned
         as required */
      ldout(cct, 5) << "clone_obj_cond was cancelled, lost in a race" << dendl;
      state->clear();
      return r;
    } else {
      int ret = rctx->notify_intent(this, dest_obj, DEL_OBJ);
      if (ret < 0) {
        ldout(cct, 0) << "WARNING: failed to log intent ret=" << ret << dendl;
      }
    }
    if (r < 0) {
      ldout(cct, 0) << "ERROR: failed to clone object r=" << r << dendl;
      return r;
    }
  }

  if (need_guard) {
    /* first verify that the object wasn't replaced under */
    op.cmpxattr(RGW_ATTR_ID_TAG, LIBRADOS_CMPXATTR_OP_EQ, state->obj_tag);
    // FIXME: need to add FAIL_NOTEXIST_OK for racing deletion
  }

  if (reset_obj) {
    op.create(false);
    op.remove();
  }

  string tag;
  if (ptag) {
    tag = *ptag;
  } else {
    append_rand_alpha(cct, tag, tag, 32);
  }
  bufferlist bl;
  bl.append(tag.c_str(), tag.size() + 1);

  ldout(cct, 0) << "setting object tag=" << tag << dendl;

  op.setxattr(RGW_ATTR_ID_TAG, bl);

  string shadow = obj.object;
  shadow.append(".");
  shadow.append(tag);

  bufferlist shadow_bl;
  shadow_bl.append(shadow);
  op.setxattr(RGW_ATTR_SHADOW_OBJ, shadow_bl);

  return 0;
}

int RGWRados::prepare_atomic_for_write(RGWRadosCtx *rctx, rgw_obj& obj,
                            ObjectWriteOperation& op, RGWObjState **pstate,
			    bool reset_obj, const string *ptag)
{
  if (!rctx) {
    *pstate = NULL;
    return 0;
  }

  int r;
  r = prepare_atomic_for_write_impl(rctx, obj, op, pstate, reset_obj, ptag);

  return r;
}

/**
 * Set an attr on an object.
 * bucket: name of the bucket holding the object
 * obj: name of the object to set the attr on
 * name: the attr to set
 * bl: the contents of the attr
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWRados::set_attr(void *ctx, rgw_obj& obj, const char *name, bufferlist& bl)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  rgw_bucket actual_bucket = bucket;
  string actual_obj = oid;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;

  if (actual_obj.size() == 0) {
    actual_obj = bucket.name;
    actual_bucket = params.domain_root;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, obj, op, &state);
  if (r < 0)
    return r;

  op.setxattr(name, bl);

  io_ctx.locator_set_key(key);
  r = io_ctx.operate(actual_obj, &op);

  if (state && r >= 0)
    state->attrset[name] = bl;

  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    ldout(cct, 0) << "NOTICE: RGWRados::set_attr: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(obj.bucket, state->shadow_obj, loc, shadow_ns);
    r = set_attr(NULL, shadow, name, bl);
  }

  if (r < 0)
    return r;

  return 0;
}

int RGWRados::set_attrs(void *ctx, rgw_obj& obj,
                        map<string, bufferlist>& attrs,
                        map<string, bufferlist>* rmattrs)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  string actual_obj = oid;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  rgw_bucket actual_bucket = bucket;

  if (actual_obj.size() == 0) {
    actual_obj = bucket.name;
    actual_bucket = params.domain_root;
  }

  int r = open_bucket_ctx(actual_bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectWriteOperation op;
  RGWObjState *state = NULL;

  r = append_atomic_test(rctx, obj, op, &state);
  if (r < 0)
    return r;

  map<string, bufferlist>::iterator iter;
  if (rmattrs) {
    for (iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name.c_str());
    }
  }

  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name.c_str(), bl);
  }

  if (!op.size())
    return 0;

  r = io_ctx.operate(actual_obj, &op);

  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    ldout(cct, 0) << "NOTICE: RGWRados::set_obj_attrs: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(obj.bucket, state->shadow_obj, loc, shadow_ns);
    r = set_attrs(NULL, shadow, attrs, rmattrs);
  }

  if (r < 0)
    return r;

  return 0;
}

/**
 * Get data about an object out of RADOS and into memory.
 * bucket: name of the bucket the object is in.
 * obj: name/key of the object to read
 * data: if get_data==true, this pointer will be set
 *    to an address containing the object's data/value
 * ofs: the offset of the object to read from
 * end: the point in the object to stop reading
 * attrs: if non-NULL, the pointed-to map will contain
 *    all the attrs of the object when this function returns
 * mod_ptr: if non-NULL, compares the object's mtime to *mod_ptr,
 *    and if mtime is smaller it fails.
 * unmod_ptr: if non-NULL, compares the object's mtime to *unmod_ptr,
 *    and if mtime is >= it fails.
 * if_match/nomatch: if non-NULL, compares the object's etag attr
 *    to the string and, if it doesn't/does match, fails out.
 * get_data: if true, the object's data/value will be read out, otherwise not
 * err: Many errors will result in this structure being filled
 *    with extra informatin on the error.
 * Returns: -ERR# on failure, otherwise
 *          (if get_data==true) length of read data,
 *          (if get_data==false) length of the object
 */
int RGWRados::prepare_get_obj(void *ctx, rgw_obj& obj,
            off_t *pofs, off_t *pend,
            map<string, bufferlist> *attrs,
            const time_t *mod_ptr,
            const time_t *unmod_ptr,
            time_t *lastmod,
            const char *if_match,
            const char *if_nomatch,
            uint64_t *total_size,
            uint64_t *obj_size,
            void **handle,
            struct rgw_err *err)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  int r = -EINVAL;
  bufferlist etag;
  time_t ctime;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  RGWRadosCtx *new_ctx = NULL;
  RGWObjState *astate = NULL;
  off_t ofs = 0;
  off_t end = -1;

  map<string, bufferlist>::iterator iter;

  *handle = NULL;

  GetObjState *state = new GetObjState;
  if (!state)
    return -ENOMEM;

  *handle = state;

  r = open_bucket_ctx(bucket, state->io_ctx);
  if (r < 0)
    goto done_err;

  state->io_ctx.locator_set_key(key);

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  r = get_obj_state(rctx, obj, &astate);
  if (r < 0)
    goto done_err;

  if (!astate->exists) {
    r = -ENOENT;
    goto done_err;
  }

  if (attrs) {
    *attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
    if (r < 0)
      goto done_err;
  }

  /* Convert all times go GMT to make them compatible */
  if (mod_ptr || unmod_ptr) {
    ctime = astate->mtime;

    if (mod_ptr) {
      ldout(cct, 10) << "If-Modified-Since: " << *mod_ptr << " Last-Modified: " << ctime << dendl;
      if (ctime < *mod_ptr) {
        r = -ERR_NOT_MODIFIED;
        goto done_err;
      }
    }

    if (unmod_ptr) {
      ldout(cct, 10) << "If-UnModified-Since: " << *unmod_ptr << " Last-Modified: " << ctime << dendl;
      if (ctime > *unmod_ptr) {
        r = -ERR_PRECONDITION_FAILED;
        goto done_err;
      }
    }
  }
  if (if_match || if_nomatch) {
    r = get_attr(rctx, obj, RGW_ATTR_ETAG, etag);
    if (r < 0)
      goto done_err;

    if (if_match) {
      string if_match_str = rgw_string_unquote(if_match);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-Match: " << if_match_str << dendl;
      if (if_match_str.compare(etag.c_str()) != 0) {
        r = -ERR_PRECONDITION_FAILED;
        goto done_err;
      }
    }

    if (if_nomatch) {
      string if_nomatch_str = rgw_string_unquote(if_nomatch);
      ldout(cct, 10) << "ETag: " << etag.c_str() << " " << " If-NoMatch: " << if_nomatch_str << dendl;
      if (if_nomatch_str.compare(etag.c_str()) == 0) {
        r = -ERR_NOT_MODIFIED;
        goto done_err;
      }
      if_nomatch = if_nomatch_str.c_str();
    }
  }

  if (pofs)
    ofs = *pofs;
  if (pend)
    end = *pend;

  if (ofs < 0) {
    ofs += astate->size;
    if (ofs < 0)
      ofs = 0;
    end = astate->size - 1;
  } else if (end < 0) {
    end = astate->size - 1;
  }

  if (astate->size > 0) {
    if (ofs >= (off_t)astate->size) {
      r = -ERANGE;
      goto done_err;
    }
    if (end >= (off_t)astate->size) {
      end = astate->size - 1;
    }
  }

  if (pofs)
    *pofs = ofs;
  if (pend)
    *pend = end;
  if (total_size)
    *total_size = (ofs <= end ? end + 1 - ofs : 0);
  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  delete new_ctx;

  return 0;

done_err:
  delete new_ctx;
  delete state;
  *handle = NULL;
  return r;
}

int RGWRados::prepare_update_index(RGWObjState *state, rgw_bucket& bucket,
                                   rgw_obj& obj, string& tag)
{
  if (bucket_is_system(bucket))
    return 0;

  if (state && state->obj_tag.length()) {
    int len = state->obj_tag.length();
    char buf[len + 1];
    memcpy(buf, state->obj_tag.c_str(), len);
    buf[len] = '\0';
    tag = buf;
  } else {
    append_rand_alpha(cct, tag, tag, 32);
  }
  int ret = cls_obj_prepare_op(bucket, CLS_RGW_OP_ADD, tag,
                               obj.object, obj.key);

  return ret;
}

int RGWRados::complete_update_index(rgw_bucket& bucket, string& oid, string& tag, uint64_t epoch, uint64_t size,
                                    utime_t& ut, string& etag, string& content_type, bufferlist *acl_bl, RGWObjCategory category,
                                    list<string> *remove_objs)
{
  if (bucket_is_system(bucket))
    return 0;

  RGWObjEnt ent;
  ent.name = oid;
  ent.size = size;
  ent.mtime = ut;
  ent.etag = etag;
  ACLOwner owner;
  if (acl_bl && acl_bl->length()) {
    int ret = decode_policy(*acl_bl, &owner);
    if (ret < 0) {
      ldout(cct, 0) << "WARNING: could not decode policy ret=" << ret << dendl;
    }
  }
  ent.owner = owner.get_id();
  ent.owner_display_name = owner.get_display_name();
  ent.content_type = content_type;

  int ret = cls_obj_complete_add(bucket, tag, epoch, ent, category, remove_objs);

  return ret;
}


int RGWRados::clone_objs_impl(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        RGWObjCategory category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive,
                        pair<string, bufferlist> *xattr_cond)
{
  rgw_bucket bucket;
  std::string dst_oid, dst_key;
  get_obj_bucket_and_oid_key(dst_obj, bucket, dst_oid, dst_key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  uint64_t size = 0;
  string etag;
  string content_type;
  bufferlist acl_bl;
  bool update_index = (category == RGW_OBJ_CATEGORY_MAIN ||
                       category == RGW_OBJ_CATEGORY_MULTIMETA);

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  io_ctx.locator_set_key(dst_key);
  ObjectWriteOperation op;
  if (truncate_dest) {
    op.remove();
    op.set_op_flags(OP_FAILOK); // don't fail if object didn't exist
  }

  op.create(exclusive);


  map<string, bufferlist>::iterator iter;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;
    op.setxattr(name.c_str(), bl);

    if (name.compare(RGW_ATTR_ETAG) == 0) {
      etag = bl.c_str();
    } else if (name.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
      content_type = bl.c_str();
    } else if (name.compare(RGW_ATTR_ACL) == 0) {
      acl_bl = bl;
    }
  }
  RGWObjState *state;
  r = prepare_atomic_for_write(rctx, dst_obj, op, &state, true, NULL);
  if (r < 0)
    return r;

  vector<RGWCloneRangeInfo>::iterator range_iter;
  for (range_iter = ranges.begin(); range_iter != ranges.end(); ++range_iter) {
    RGWCloneRangeInfo range = *range_iter;
    vector<RGWCloneRangeInfo>::iterator next_iter = range_iter;

    // merge ranges
    while (++next_iter !=  ranges.end()) {
      RGWCloneRangeInfo& next = *next_iter;
      if (range.src_ofs + (int64_t)range.len != next.src_ofs ||
          range.dst_ofs + (int64_t)range.len != next.dst_ofs)
        break;
      range_iter++;
      range.len += next.len;
    }
    if (range.len) {
      ldout(cct, 20) << "calling op.clone_range(dst_ofs=" << range.dst_ofs << ", src.object=" <<  range.src.object << " range.src_ofs=" << range.src_ofs << " range.len=" << range.len << dendl;
      if (xattr_cond) {
        string src_cmp_obj, src_cmp_key;
        get_obj_bucket_and_oid_key(range.src, bucket, src_cmp_obj, src_cmp_key);
        op.src_cmpxattr(src_cmp_obj, xattr_cond->first.c_str(),
                        LIBRADOS_CMPXATTR_OP_EQ, xattr_cond->second);
      }
      string src_oid, src_key;
      get_obj_bucket_and_oid_key(range.src, bucket, src_oid, src_key);
      if (range.dst_ofs + range.len > size)
        size = range.dst_ofs + range.len;
      op.clone_range(range.dst_ofs, src_oid, range.src_ofs, range.len);
    }
  }
  time_t mt;
  utime_t ut;
  if (pmtime) {
    op.mtime(pmtime);
    ut = utime_t(*pmtime, 0);
  } else {
    ut = ceph_clock_now(cct);
    mt = ut.sec();
    op.mtime(&mt);
  }

  string tag;
  uint64_t epoch = 0;
  int ret;

  if (update_index) {
    ret = prepare_update_index(state, bucket, dst_obj, tag);
    if (ret < 0)
      goto done;
  }

  ret = io_ctx.operate(dst_oid, &op);

  epoch = io_ctx.get_last_version();

done:
  atomic_write_finish(state, ret);

  if (update_index) {
    if (ret >= 0) {
      ret = complete_update_index(bucket, dst_obj.object, tag, epoch, size,
                                  ut, etag, content_type, &acl_bl, category, NULL);
    } else {
      int r = complete_update_index_cancel(bucket, dst_obj.object, tag);
      if (r < 0) {
        ldout(cct, 0) << "ERROR: comlete_update_index_cancel() returned r=" << r << dendl;
      }
    }
  }

  return ret;
}

int RGWRados::clone_objs(void *ctx, rgw_obj& dst_obj,
                        vector<RGWCloneRangeInfo>& ranges,
                        map<string, bufferlist> attrs,
                        RGWObjCategory category,
                        time_t *pmtime,
                        bool truncate_dest,
                        bool exclusive,
                        pair<string, bufferlist> *xattr_cond)
{
  int r;

  r = clone_objs_impl(ctx, dst_obj, ranges, attrs, category, pmtime, truncate_dest, exclusive, xattr_cond);
  if (r == -ECANCELED)
    r = 0;

  return r;
}


int RGWRados::get_obj(void *ctx, void **handle, rgw_obj& obj,
                      bufferlist& bl, off_t ofs, off_t end)
{
  rgw_bucket bucket;
  std::string oid, key;
  rgw_obj read_obj = obj;
  uint64_t read_ofs = ofs;
  uint64_t len, read_len;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  RGWRadosCtx *new_ctx = NULL;
  bool reading_from_head = true;
  ObjectReadOperation op;

  GetObjState *state = *(GetObjState **)handle;
  RGWObjState *astate = NULL;

  bool merge_bl = false;
  bufferlist *pbl = &bl;
  bufferlist read_bl;

  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

  if (!rctx) {
    new_ctx = new RGWRadosCtx(this);
    rctx = new_ctx;
  }

  int r = get_obj_state(rctx, obj, &astate);
  if (r < 0)
    goto done_ret;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (astate->has_manifest &&
      astate->manifest.objs.size() > 0) {
    /* now get the relevant object part */
    map<uint64_t, RGWObjManifestPart>::iterator iter = astate->manifest.objs.upper_bound(ofs);
    /* we're now pointing at the next part (unless the first part starts at a higher ofs),
       so retract to previous part */
    if (iter != astate->manifest.objs.begin()) {
      --iter;
    }

    RGWObjManifestPart& part = iter->second;
    uint64_t part_ofs = iter->first;
    read_obj = part.loc;
    len = min(len, part.size - (ofs - part_ofs));
    read_ofs = part.loc_ofs + (ofs - part_ofs);
    reading_from_head = (read_obj == obj);

    if (!reading_from_head) {
      get_obj_bucket_and_oid_key(read_obj, bucket, oid, key);
    }
  }

  if (len > RGW_MAX_CHUNK_SIZE)
    len = RGW_MAX_CHUNK_SIZE;


  state->io_ctx.locator_set_key(key);

  read_len = len;

  if (reading_from_head) {
    /* only when reading from the head object do we need to do the atomic test */
    r = append_atomic_test(rctx, read_obj, op, &astate);
    if (r < 0)
      goto done_ret;

    if (astate) {
      if (!ofs && astate->data.length() >= len) {
        bl = astate->data;
        goto done;
      }

      if (ofs < astate->data.length()) {
        unsigned copy_len = min((uint64_t)astate->data.length() - ofs, len);
        astate->data.copy(ofs, copy_len, bl);
        read_len -= copy_len;
        read_ofs += copy_len;
        if (!read_len)
	  goto done;

        merge_bl = true;
        pbl = &read_bl;
      }
    }
  }

  ldout(cct, 20) << "rados->read obj-ofs=" << ofs << " read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
  op.read(read_ofs, read_len, pbl, NULL);

  r = state->io_ctx.operate(oid, &op, NULL);
  ldout(cct, 20) << "rados->read r=" << r << " bl.length=" << bl.length() << dendl;

  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    ldout(cct, 0) << "NOTICE: RGWRados::get_obj: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(bucket, astate->shadow_obj, loc, shadow_ns);
    r = get_obj(NULL, handle, shadow, bl, ofs, end);
    goto done_ret;
  }

  if (merge_bl)
    bl.append(read_bl);

done:
  if (bl.length() > 0) {
    r = bl.length();
  }
  if (r < 0 || !len || ((off_t)(ofs + len - 1) == end)) {
    delete state;
    *handle = NULL;
  }

done_ret:
  delete new_ctx;

  return r;
}

void RGWRados::finish_get_obj(void **handle)
{
  if (*handle) {
    GetObjState *state = *(GetObjState **)handle;
    delete state;
    *handle = NULL;
  }
}

/* a simple object read */
int RGWRados::read(void *ctx, rgw_obj& obj, off_t ofs, size_t size, bufferlist& bl)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  RGWRadosCtx *rctx = (RGWRadosCtx *)ctx;
  RGWObjState *astate = NULL;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  ObjectReadOperation op;

  r = append_atomic_test(rctx, obj, op, &astate);
  if (r < 0)
    return r;

  op.read(ofs, size, &bl, NULL);

  r = io_ctx.operate(oid, &op, NULL);
  if (r == -ECANCELED) {
    /* a race! object was replaced, we need to set attr on the original obj */
    ldout(cct, 0) << "NOTICE: RGWRados::get_obj: raced with another process, going to the shadow obj instead" << dendl;
    string loc = obj.loc();
    rgw_obj shadow(obj.bucket, astate->shadow_obj, loc, shadow_ns);
    r = read(NULL, shadow, ofs, size, bl);
  }
  return r;
}

int RGWRados::obj_stat(void *ctx, rgw_obj& obj, uint64_t *psize, time_t *pmtime, uint64_t *epoch, map<string, bufferlist> *attrs, bufferlist *first_chunk)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  map<string, bufferlist> attrset;
  uint64_t size = 0;
  time_t mtime = 0;

  ObjectReadOperation op;
  op.getxattrs(&attrset, NULL);
  op.stat(&size, &mtime, NULL);
  if (first_chunk) {
    op.read(0, RGW_MAX_CHUNK_SIZE, first_chunk, NULL);
  }
  bufferlist outbl;
  r = io_ctx.operate(oid, &op, &outbl);

  if (epoch)
    *epoch = io_ctx.get_last_version();

  if (r < 0)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (attrs)
    *attrs = attrset;

  return 0;
}

int RGWRados::get_bucket_stats(rgw_bucket& bucket, map<RGWObjCategory, RGWBucketStats>& stats)
{
  rgw_bucket_dir_header header;
  int r = cls_bucket_head(bucket, header);
  if (r < 0)
    return r;

  stats.clear();

  translate_raw_stats(header, stats);

  return 0;
}

int RGWRados::get_bucket_info(void *ctx, string& bucket_name, RGWBucketInfo& info, map<string, bufferlist> *pattrs)
{
  bufferlist bl;

  int ret = rgw_get_obj(this, ctx, params.domain_root, bucket_name, bl, pattrs);
  if (ret < 0) {
    if (ret != -ENOENT)
      return ret;

    info.bucket.name = bucket_name;
    info.bucket.pool = bucket_name; // for now
    return 0;
  }

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(info, iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode buffer info, caught buffer::error" << dendl;
    return -EIO;
  }

  ldout(cct, 20) << "rgw_get_bucket_info: bucket=" << info.bucket << " owner " << info.owner << dendl;

  return 0;
}

int RGWRados::put_bucket_info(string& bucket_name, RGWBucketInfo& info, bool exclusive, map<string, bufferlist> *pattrs)
{
  bufferlist bl;

  ::encode(info, bl);

  int ret = rgw_put_system_obj(this, params.domain_root, bucket_name, bl.c_str(), bl.length(), exclusive, pattrs);

  return ret;
}

int RGWRados::omap_get_all(rgw_obj& obj, bufferlist& header, std::map<string, bufferlist>& m)
{
  bufferlist bl;
  librados::IoCtx io_ctx;
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  string start_after;
  r = io_ctx.omap_get_vals(oid, start_after, -1, &m);
  if (r < 0)
    return r;

  return 0;
 
}

int RGWRados::omap_set(rgw_obj& obj, std::string& key, bufferlist& bl)
{
  rgw_bucket bucket;
  std::string oid, okey;
  get_obj_bucket_and_oid_key(obj, bucket, oid, okey);

  ldout(cct, 15) << "omap_set bucket=" << bucket << " oid=" << oid << " key=" << key << dendl;

  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(okey);

  map<string, bufferlist> m;
  m[key] = bl;

  r = io_ctx.omap_set(oid, m);

  return r;
}

int RGWRados::omap_set(rgw_obj& obj, std::map<std::string, bufferlist>& m)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);

  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(key);

  r = io_ctx.omap_set(oid, m);

  return r;
}

int RGWRados::omap_del(rgw_obj& obj, std::string& key)
{
  rgw_bucket bucket;
  std::string oid, okey;
  get_obj_bucket_and_oid_key(obj, bucket, oid, okey);

  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  io_ctx.locator_set_key(okey);

  set<string> k;
  k.insert(key);

  r = io_ctx.omap_rm_keys(oid, k);
  return r;
}

int RGWRados::update_containers_stats(map<string, RGWBucketEnt>& m)
{
  map<string, RGWBucketEnt>::iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt& ent = iter->second;
    rgw_bucket& bucket = ent.bucket;

    rgw_bucket_dir_header header;
    int r = cls_bucket_head(bucket, header);
    if (r < 0)
      return r;

    ent.count = 0;
    ent.size = 0;

    RGWObjCategory category = main_category;
    map<uint8_t, struct rgw_bucket_category_stats>::iterator iter = header.stats.find((uint8_t)category);
    if (iter != header.stats.end()) {
      struct rgw_bucket_category_stats& stats = iter->second;
      ent.count = stats.num_entries;
      ent.size = stats.total_size;
      ent.size_rounded = stats.total_size_rounded;
    }
  }

  return m.size();
}

int RGWRados::append_async(rgw_obj& obj, size_t size, bufferlist& bl)
{
  rgw_bucket bucket;
  std::string oid, key;
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  librados::IoCtx io_ctx;
  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;
  librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);

  io_ctx.locator_set_key(key);

  r = io_ctx.aio_append(oid, completion, bl, size);
  completion->release();
  return r;
}

int RGWRados::distribute(const string& key, bufferlist& bl)
{
  string notify_oid;
  pick_control_oid(key, notify_oid);

  ldout(cct, 10) << "distributing notification oid=" << notify_oid << " bl.length()=" << bl.length() << dendl;
  int r = control_pool_ctx.notify(notify_oid, 0, bl);
  return r;
}

int RGWRados::pool_iterate_begin(rgw_bucket& bucket, RGWPoolIterCtx& ctx)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::ObjectIterator& iter = ctx.iter;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  iter = io_ctx.objects_begin();

  return 0;
}

int RGWRados::pool_iterate(RGWPoolIterCtx& ctx, uint32_t num, vector<RGWObjEnt>& objs,
                           bool *is_truncated, RGWAccessListFilter *filter)
{
  librados::IoCtx& io_ctx = ctx.io_ctx;
  librados::ObjectIterator& iter = ctx.iter;

  if (iter == io_ctx.objects_end())
    return -ENOENT;

  uint32_t i;

  for (i = 0; i < num && iter != io_ctx.objects_end(); ++i, ++iter) {
    RGWObjEnt e;

    string oid = iter->first;
    ldout(cct, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter->filter(oid, oid))
      continue;

    e.name = oid;
    objs.push_back(e);
  }

  if (is_truncated)
    *is_truncated = (iter != io_ctx.objects_end());

  return objs.size();
}

int RGWRados::gc_operate(string& oid, librados::ObjectWriteOperation *op)
{
  return gc_pool_ctx.operate(oid, op);
}

int RGWRados::gc_aio_operate(string& oid, librados::ObjectWriteOperation *op)
{
  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  int r = gc_pool_ctx.aio_operate(oid, c, op);
  c->release();
  return r;
}

int RGWRados::gc_operate(string& oid, librados::ObjectReadOperation *op, bufferlist *pbl)
{
  return gc_pool_ctx.operate(oid, op, pbl);
}

int RGWRados::list_gc_objs(int *index, string& marker, uint32_t max, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  return gc->list(index, marker, max, result, truncated);
}

int RGWRados::process_gc()
{
  return gc->process();
}

int RGWRados::cls_rgw_init_index(librados::IoCtx& io_ctx, librados::ObjectWriteOperation& op, string& oid)
{
  bufferlist in;
  cls_rgw_bucket_init(op);
  int r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::cls_obj_prepare_op(rgw_bucket& bucket, uint8_t op, string& tag,
                                 string& name, string& locator)
{
  librados::IoCtx io_ctx;
  string oid;

  int r = open_bucket(bucket, io_ctx, oid);
  if (r < 0)
    return r;

  ObjectWriteOperation o;
  cls_rgw_bucket_prepare_op(o, op, tag, name, locator);
  r = io_ctx.operate(oid, &o);
  return r;
}

int RGWRados::cls_obj_complete_op(rgw_bucket& bucket, uint8_t op, string& tag, uint64_t epoch, RGWObjEnt& ent, RGWObjCategory category,
				  list<string> *remove_objs)
{
  librados::IoCtx io_ctx;
  string oid;

  int r = open_bucket(bucket, io_ctx, oid);
  if (r < 0)
    return r;

  ObjectWriteOperation o;
  rgw_bucket_dir_entry_meta dir_meta;
  dir_meta.size = ent.size;
  dir_meta.mtime = utime_t(ent.mtime, 0);
  dir_meta.etag = ent.etag;
  dir_meta.owner = ent.owner;
  dir_meta.owner_display_name = ent.owner_display_name;
  dir_meta.content_type = ent.content_type;
  dir_meta.category = category;
  cls_rgw_bucket_complete_op(o, op, tag, epoch, ent.name, dir_meta, remove_objs);

  AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
  r = io_ctx.aio_operate(oid, c, &o);
  c->release();
  return r;
}

int RGWRados::cls_obj_complete_add(rgw_bucket& bucket, string& tag, uint64_t epoch, RGWObjEnt& ent, RGWObjCategory category, list<string> *remove_objs)
{
  return cls_obj_complete_op(bucket, CLS_RGW_OP_ADD, tag, epoch, ent, category, remove_objs);
}

int RGWRados::cls_obj_complete_del(rgw_bucket& bucket, string& tag, uint64_t epoch, string& name)
{
  RGWObjEnt ent;
  ent.name = name;
  return cls_obj_complete_op(bucket, CLS_RGW_OP_DEL, tag, epoch, ent, RGW_OBJ_CATEGORY_NONE, NULL);
}

int RGWRados::cls_obj_complete_cancel(rgw_bucket& bucket, string& tag, string& name)
{
  RGWObjEnt ent;
  ent.name = name;
  return cls_obj_complete_op(bucket, CLS_RGW_OP_ADD, tag, 0, ent, RGW_OBJ_CATEGORY_NONE, NULL);
}

int RGWRados::cls_obj_set_bucket_tag_timeout(rgw_bucket& bucket, uint64_t timeout)
{
  librados::IoCtx io_ctx;
  string oid;

  int r = open_bucket(bucket, io_ctx, oid);
  if (r < 0)
    return r;

  ObjectWriteOperation o;
  cls_rgw_bucket_set_tag_timeout(o, timeout);

  r = io_ctx.operate(oid, &o);

  return r;
}

int RGWRados::cls_bucket_list(rgw_bucket& bucket, string start, string prefix,
		              uint32_t num, map<string, RGWObjEnt>& m,
			      bool *is_truncated, string *last_entry,
			      bool (*force_check_filter)(const string&  name))
{
  ldout(cct, 10) << "cls_bucket_list " << bucket << " start " << start << " num " << num << dendl;

  librados::IoCtx io_ctx;
  string oid;
  int r = open_bucket(bucket, io_ctx, oid);
  if (r < 0)
    return r;

  struct rgw_bucket_dir dir;
  r = cls_rgw_list_op(io_ctx, oid, start, prefix, num, &dir, is_truncated);
  if (r < 0)
    return r;

  map<string, struct rgw_bucket_dir_entry>::iterator miter;
  bufferlist updates;
  for (miter = dir.m.begin(); miter != dir.m.end(); ++miter) {
    RGWObjEnt e;
    rgw_bucket_dir_entry& dirent = miter->second;

    // fill it in with initial values; we may correct later
    e.name = dirent.name;
    e.size = dirent.meta.size;
    e.mtime = dirent.meta.mtime;
    e.etag = dirent.meta.etag;
    e.owner = dirent.meta.owner;
    e.owner_display_name = dirent.meta.owner_display_name;
    e.content_type = dirent.meta.content_type;

    /* oh, that shouldn't happen! */
    if (e.name.empty()) {
      ldout(cct, 0) << "WARNING: got empty dirent name, skipping" << dendl;
      continue;
    }

    bool force_check = force_check_filter && force_check_filter(dirent.name);

    if (!dirent.exists || !dirent.pending_map.empty() || force_check) {
      /* there are uncommitted ops. We need to check the current state,
       * and if the tags are old we need to do cleanup as well. */
      librados::IoCtx sub_ctx;
      sub_ctx.dup(io_ctx);
      r = check_disk_state(sub_ctx, bucket, dirent, e, updates);
      if (r < 0) {
        if (r == -ENOENT)
          continue;
        else
          return r;
      }
    }
    m[e.name] = e;
    ldout(cct, 10) << "RGWRados::cls_bucket_list: got " << e.name << dendl;
  }

  if (dir.m.size()) {
    *last_entry = dir.m.rbegin()->first;
  }

  if (updates.length()) {
    ObjectWriteOperation o;
    cls_rgw_suggest_changes(o, updates);
    // we don't care if we lose suggested updates, send them off blindly
    AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
    r = io_ctx.aio_operate(oid, c, &o);
    c->release();
  }
  return m.size();
}

int RGWRados::cls_obj_usage_log_add(const string& oid, rgw_usage_log_info& info)
{
  librados::IoCtx io_ctx;

  const char *usage_log_pool = params.usage_log_pool.name.c_str();
  int r = rados->ioctx_create(usage_log_pool, io_ctx);
  if (r == -ENOENT) {
    rgw_bucket pool(usage_log_pool);
    r = create_pool(pool);
    if (r < 0)
      return r;
 
    // retry
    r = rados->ioctx_create(usage_log_pool, io_ctx);
  }
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_rgw_usage_log_add(op, info);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::cls_obj_usage_log_read(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
                                     string& read_iter, map<rgw_user_bucket, rgw_usage_log_entry>& usage, bool *is_truncated)
{
  librados::IoCtx io_ctx;

  *is_truncated = false;

  const char *usage_log_pool = params.usage_log_pool.name.c_str();
  int r = rados->ioctx_create(usage_log_pool, io_ctx);
  if (r < 0)
    return r;

  r = cls_rgw_usage_log_read(io_ctx, oid, user, start_epoch, end_epoch,
			     max_entries, read_iter, usage, is_truncated);

  return r;
}

int RGWRados::cls_obj_usage_log_trim(string& oid, string& user, uint64_t start_epoch, uint64_t end_epoch)
{
  librados::IoCtx io_ctx;

  const char *usage_log_pool = params.usage_log_pool.name.c_str();
  int r = rados->ioctx_create(usage_log_pool, io_ctx);
  if (r < 0)
    return r;

  ObjectWriteOperation op;
  cls_rgw_usage_log_trim(op, user, start_epoch, end_epoch);

  r = io_ctx.operate(oid, &op);
  return r;
}

int RGWRados::remove_objs_from_index(rgw_bucket& bucket, list<string>& oid_list)
{
  librados::IoCtx io_ctx;

  int r = open_bucket_ctx(bucket, io_ctx);
  if (r < 0)
    return r;

  string dir_oid = dir_oid_prefix;
  dir_oid.append(bucket.marker);

  bufferlist updates;

  list<string>::iterator iter;

  for (iter = oid_list.begin(); iter != oid_list.end(); ++iter) {
    string& oid = *iter;
    dout(2) << "RGWRados::remove_objs_from_index bucket=" << bucket << " oid=" << oid << dendl;
    rgw_bucket_dir_entry entry;
    entry.epoch = (uint64_t)-1; // ULLONG_MAX, needed to that objclass doesn't skip out request
    entry.name = oid;
    updates.append(CEPH_RGW_REMOVE);
    ::encode(entry, updates);
  }

  bufferlist out;

  r = io_ctx.exec(dir_oid, "rgw", "dir_suggest_changes", updates, out);

  return r;
}

int RGWRados::check_disk_state(librados::IoCtx io_ctx,
                               rgw_bucket& bucket,
                               rgw_bucket_dir_entry& list_state,
                               RGWObjEnt& object,
                               bufferlist& suggested_updates)
{
  rgw_obj obj;
  std::string oid, key, ns;
  oid = list_state.name;
  if (!rgw_obj::strip_namespace_from_object(oid, ns)) {
    // well crap
    assert(0 == "got bad object name off disk");
  }
  obj.init(bucket, oid, list_state.locator, ns);
  get_obj_bucket_and_oid_key(obj, bucket, oid, key);
  io_ctx.locator_set_key(key);

  RGWObjState *astate = NULL;
  RGWRadosCtx rctx(this);
  int r = get_obj_state(&rctx, obj, &astate);
  if (r < 0)
    return r;

  list_state.pending_map.clear(); // we don't need this and it inflates size
  if (!astate->exists) {
      /* object doesn't exist right now -- hopefully because it's
       * marked as !exists and got deleted */
    if (list_state.exists) {
      /* FIXME: what should happen now? Work out if there are any
       * non-bad ways this could happen (there probably are, but annoying
       * to handle!) */
    }
    // encode a suggested removal of that key
    list_state.epoch = io_ctx.get_last_version();
    cls_rgw_encode_suggestion(CEPH_RGW_REMOVE, list_state, suggested_updates);
    return -ENOENT;
  }

  string etag;
  string content_type;
  ACLOwner owner;

  object.size = astate->size;
  object.mtime = astate->mtime;

  map<string, bufferlist>::iterator iter = astate->attrset.find(RGW_ATTR_ETAG);
  if (iter != astate->attrset.end()) {
    etag = iter->second.c_str();
  }
  iter = astate->attrset.find(RGW_ATTR_CONTENT_TYPE);
  if (iter != astate->attrset.end()) {
    content_type = iter->second.c_str();
  }
  iter = astate->attrset.find(RGW_ATTR_ACL);
  if (iter != astate->attrset.end()) {
    r = decode_policy(iter->second, &owner);
    if (r < 0) {
      dout(0) << "WARNING: could not decode policy for object: " << obj << dendl;
    }
  }

  if (astate->has_manifest) {
    map<uint64_t, RGWObjManifestPart>::iterator miter;
    RGWObjManifest& manifest = astate->manifest;
    for (miter = manifest.objs.begin(); miter != manifest.objs.end(); ++miter) {
      RGWObjManifestPart& part = miter->second;

      rgw_obj& loc = part.loc;

      if (loc.ns == RGW_OBJ_NS_MULTIPART) {
	dout(10) << "check_disk_state(): removing manifest part from index: " << loc << dendl;
	r = delete_obj_index(loc);
	if (r < 0) {
	  dout(0) << "WARNING: delete_obj_index() returned r=" << r << dendl;
	}
      }
    }
  }

  object.etag = etag;
  object.content_type = content_type;
  object.owner = owner.get_id();
  object.owner_display_name = owner.get_display_name();

  // encode suggested updates
  list_state.epoch = astate->epoch;
  list_state.meta.size = object.size;
  list_state.meta.mtime.set_from_double(double(object.mtime));
  list_state.meta.category = main_category;
  list_state.meta.etag = etag;
  list_state.meta.content_type = content_type;
  if (astate->obj_tag.length() > 0)
    list_state.meta.tag = astate->obj_tag.c_str();
  list_state.meta.owner = owner.get_id();
  list_state.meta.owner_display_name = owner.get_display_name();

  list_state.exists = true;
  cls_rgw_encode_suggestion(CEPH_RGW_UPDATE, list_state, suggested_updates);
  return 0;
}

int RGWRados::cls_bucket_head(rgw_bucket& bucket, struct rgw_bucket_dir_header& header)
{
  librados::IoCtx io_ctx;
  string oid;
  int r = open_bucket(bucket, io_ctx, oid);
  if (r < 0)
    return r;

  r = cls_rgw_get_dir_header(io_ctx, oid, &header);
  if (r < 0)
    return r;

  return 0;
}


class IntentLogNameFilter : public RGWAccessListFilter
{
  string prefix;
  bool filter_exact_date;
public:
  IntentLogNameFilter(const char *date, struct tm *tm) {
    prefix = date;
    filter_exact_date = !(tm->tm_hour || tm->tm_min || tm->tm_sec); /* if time was specified and is not 00:00:00
                                                                       we should look at objects from that date */
  }
  bool filter(string& name, string& key) {
    if (filter_exact_date)
      return name.compare(prefix) < 0;
    else
      return name.compare(0, prefix.size(), prefix) <= 0;
  }
};

enum IntentFlags { // bitmask
  I_DEL_OBJ = 1,
  I_DEL_DIR = 2,
};


int RGWRados::remove_temp_objects(string date, string time)
{
  struct tm tm;
  
  string format = "%Y-%m-%d";
  string datetime = date;
  if (datetime.size() != 10) {
    cerr << "bad date format" << std::endl;
    return -EINVAL;
  }

  if (!time.empty()) {
    if (time.size() != 5 && time.size() != 8) {
      cerr << "bad time format" << std::endl;
      return -EINVAL;
    }
    format.append(" %H:%M:%S");
    datetime.append(time.c_str());
  }
  memset(&tm, 0, sizeof(tm));
  const char *s = strptime(datetime.c_str(), format.c_str(), &tm);
  if (s && *s) {
    cerr << "failed to parse date/time" << std::endl;
    return -EINVAL;
  }
  time_t epoch = mktime(&tm);

  string prefix, delim, marker;
  vector<RGWObjEnt> objs;
  map<string, bool> common_prefixes;
  
  int max = 1000;
  bool is_truncated;
  IntentLogNameFilter filter(date.c_str(), &tm);
  RGWPoolIterCtx iter_ctx;
  int r = pool_iterate_begin(params.intent_log_pool, iter_ctx);
  if (r < 0) {
    cerr << "failed to list objects" << std::endl;
    return r;
  }
  do {
    objs.clear();
    r = pool_iterate(iter_ctx, max, objs, &is_truncated, &filter);
    if (r == -ENOENT)
      break;
    if (r < 0) {
      cerr << "failed to list objects" << std::endl;
    }
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      process_intent_log(params.intent_log_pool, (*iter).name, epoch, I_DEL_OBJ | I_DEL_DIR, true);
    }
  } while (is_truncated);

  return 0;
}

int RGWRados::process_intent_log(rgw_bucket& bucket, string& oid,
				 time_t epoch, int flags, bool purge)
{
  cout << "processing intent log " << oid << std::endl;
  rgw_obj obj(bucket, oid);

  unsigned chunk = 1024 * 1024;
  off_t pos = 0;
  bool eof = false;
  bool complete = true;
  int ret = 0;
  int r;

  bufferlist bl;
  bufferlist::iterator iter;
  off_t off;

  while (!eof || !iter.end()) {
    off = iter.get_off();
    if (!eof && (bl.length() - off) < chunk / 2) {
      bufferlist more;
      r = read(NULL, obj, pos, chunk, more);
      if (r < 0) {
        cerr << "error while reading from " <<  bucket << ":" << oid
	   << " " << cpp_strerror(-r) << std::endl;
        return -r;
      }
      eof = (more.length() < (off_t)chunk);
      pos += more.length();
      bufferlist old;
      old.substr_of(bl, off, bl.length() - off);
      bl.clear();
      bl.claim(old);
      bl.claim_append(more);
      iter = bl.begin();
    }
    
    struct rgw_intent_log_entry entry;
    try {
      ::decode(entry, iter);
    } catch (buffer::error& err) {
      cerr << "failed to decode intent log entry in " << bucket << ":" << oid << std::endl;
      cerr << "skipping log" << std::endl; // no use to continue
      ret = -EIO;
      complete = false;
      break;
    }
    if (entry.op_time.sec() > epoch) {
      cerr << "skipping entry for obj=" << obj << " entry.op_time=" << entry.op_time.sec() << " requested epoch=" << epoch << std::endl;
      cerr << "skipping log" << std::endl; // no use to continue
      complete = false;
      break;
    }
    switch (entry.intent) {
    case DEL_OBJ:
      if (!(flags & I_DEL_OBJ)) {
        complete = false;
        break;
      }
      r = delete_obj(NULL, entry.obj);
      if (r < 0 && r != -ENOENT) {
        cerr << "failed to remove obj: " << entry.obj << std::endl;
        complete = false;
      }
      break;
    case DEL_DIR:
      if (!(flags & I_DEL_DIR)) {
        complete = false;
        break;
      } else {
        librados::IoCtx io_ctx;
        int r = open_bucket_ctx(entry.obj.bucket, io_ctx);
        if (r < 0)
          return r;
        ObjectWriteOperation op;
        op.remove();
        string oid = dir_oid_prefix;
        oid.append(entry.obj.bucket.marker);
        librados::AioCompletion *completion = rados->aio_create_completion(NULL, NULL, NULL);
        r = io_ctx.aio_operate(oid, completion, &op);
        completion->release();
        if (r < 0 && r != -ENOENT) {
          cerr << "failed to remove pool: " << entry.obj.bucket.pool << std::endl;
          complete = false;
        }
      }
      break;
    default:
      complete = false;
    }
  }

  if (complete) {
    rgw_obj obj(bucket, oid);
    cout << "completed intent log: " << obj << (purge ? ", purging it" : "") << std::endl;
    if (purge) {
      r = delete_obj(NULL, obj);
      if (r < 0)
        cerr << "failed to remove obj: " << obj << std::endl;
    }
  }

  return ret;
}

uint64_t RGWRados::instance_id()
{
  return rados->get_instance_id();
}

uint64_t RGWRados::next_bucket_id()
{
  Mutex::Locker l(bucket_id_lock);
  return ++max_bucket_id;
}

RGWRados *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread)
{
  int use_cache = cct->_conf->rgw_cache_enabled;
  RGWRados *store = NULL;
  if (!use_cache) {
    store = new RGWRados;
  } else {
    store = new RGWCache<RGWRados>; 
  }

  if (store->initialize(cct, use_gc_thread) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

void RGWStoreManager::close_storage(RGWRados *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}

