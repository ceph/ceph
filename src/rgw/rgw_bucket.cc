#include <errno.h>

#include <string>

#include "common/errno.h"
#include "rgw_access.h"

#include "rgw_bucket.h"
#include "rgw_tools.h"



static rgw_bucket pi_buckets(BUCKETS_POOL_NAME);

static string avail_pools = ".pools.avail";
static string pool_name_prefix = "p";


int rgw_store_bucket_info(string& bucket_name, RGWBucketInfo& info)
{
  bufferlist bl;

  ::encode(info, bl);

  string uid;
  int ret = rgw_put_obj(uid, pi_buckets, bucket_name, bl.c_str(), bl.length());
  if (ret < 0)
    return ret;

  RGW_LOG(0) << "rgw_store_bucket_info: bucket=" << info.bucket << dendl;

  return 0;
}

int rgw_get_bucket_info(string& bucket_name, RGWBucketInfo& info)
{
  bufferlist bl;

  int ret = rgw_get_obj(pi_buckets, bucket_name, bl);
  if (ret < 0) {
    if (ret != -ENOENT)
      return ret;

    info.bucket.name = bucket_name;
    info.bucket.pool = bucket_name; // for now
    return 0;
  }

  bufferlist::iterator iter = bl.begin();
  ::decode(info, iter);

  RGW_LOG(0) << "rgw_get_bucket_info: bucket=" << info.bucket << dendl;

  return 0;
}

int rgw_remove_bucket_info(string& bucket_name)
{
  string uid;
  rgw_obj obj(pi_buckets, bucket_name);
  int ret = rgwstore->delete_obj(NULL, uid, obj);
  return ret;
}

static int generate_preallocated_pools(vector<string>& pools, int num)
{
  vector<string> names;

  for (int i = 0; i < num; i++) {
    string name = pool_name_prefix;
    append_rand_alpha(pool_name_prefix, name, 8);
    names.push_back(name);
  }
  string uid;
  vector<int> retcodes;
  int ret = rgwstore->create_pools(uid, names, retcodes);
  if (ret < 0)
    return ret;

  vector<int>::iterator riter;
  vector<string>::iterator niter;

  ret = -ENOENT;

  for (riter = retcodes.begin(), niter = names.begin(); riter != retcodes.end(); ++riter, ++niter) {
    int r = *riter;
    if (!r) {
      pools.push_back(*niter);
    } else if (!ret) {
      ret = r;
    }
  }
  if (!pools.size())
    return ret;

  return 0;
}

static int register_available_pools(vector<string>& pools)
{
  map<string, bufferlist> m;
  vector<string>::iterator iter;

  for (iter = pools.begin(); iter != pools.end(); ++iter) {
    bufferlist bl;
    string& name = *iter;
    m[name] = bl;
  }
  rgw_obj obj(pi_buckets, avail_pools);
  int ret = rgwstore->tmap_set(obj, m);
  if (ret == -ENOENT) {
    rgw_bucket new_bucket;
    map<string,bufferlist> attrs;
    string uid;
    ret = rgw_create_bucket(uid, pi_buckets.name, new_bucket, attrs, false);
    if (ret >= 0)
      ret = rgwstore->tmap_set(obj, m);
  }
  if (ret < 0) {
    RGW_LOG(0) << "rgwstore->tmap_set() failed" << dendl;
    return ret;
  }

  return 0;
}

static int generate_pool(string& bucket_name, rgw_bucket& bucket)
{
  vector<string> pools;
  int ret = generate_preallocated_pools(pools, g_conf->rgw_pools_preallocate_max);
  if (ret < 0) {
    RGW_LOG(0) << "generate_preallocad_pools returned " << ret << dendl;
    return ret;
  }
  bucket.pool = pools.back();
  pools.pop_back();
  bucket.name = bucket_name;

  ret = register_available_pools(pools);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

static int withdraw_pool(string& pool_name)
{
  rgw_obj obj(pi_buckets, avail_pools);
  bufferlist bl;
  return rgwstore->tmap_set(obj, pool_name, bl);
}

int rgw_bucket_maintain_pools()
{
  bufferlist header;
  map<string, bufferlist> m;
  string pool_name;

  rgw_obj obj(pi_buckets, avail_pools);
  int ret = rgwstore->tmap_get(obj, header, m);
  if (ret < 0 && ret != -ENOENT) {
      return ret;
  }

  if ((int)m.size() < g_conf->rgw_pools_preallocate_threshold) {
    RGW_LOG(0) << "rgw_bucket_maintain_pools allocating pools (m.size()=" << m.size() << " threshold="
               << g_conf->rgw_pools_preallocate_threshold << ")" << dendl;
    vector<string> pools;
    ret = generate_preallocated_pools(pools, g_conf->rgw_pools_preallocate_max - m.size());
    if (ret < 0) {
      RGW_LOG(0) << "failed to preallocate pools" << dendl;
      return ret;
    }
    ret = register_available_pools(pools);
    if (ret < 0) {
      RGW_LOG(0) << "failed to register available pools" << dendl;
      return ret;
    }
  }

  return 0;
}

int rgw_bucket_allocate_pool(string& bucket_name, rgw_bucket& bucket)
{
  bufferlist header;
  map<string, bufferlist> m;
  string pool_name;

  rgw_obj obj(pi_buckets, avail_pools);
  int ret = rgwstore->tmap_get(obj, header, m);
  if (ret < 0) {
    if (ret == -ENOENT) {
      return generate_pool(bucket_name, bucket);
    }
    return ret;
  }

  if (!m.size()) {
    return generate_pool(bucket_name, bucket);
  }

  map<string, bufferlist>::iterator miter = m.end();
  do {
    --miter;

    pool_name = miter->first;
    ret = rgwstore->tmap_del(obj, pool_name);
    if (!ret)
      break;

  } while (miter != m.begin());

  if (miter == m.begin()) {
    return generate_pool(bucket_name, bucket);
  }
  bucket.pool = pool_name;
  bucket.name = bucket_name;
  
  return 0;
}



int rgw_create_bucket(std::string& id, string& bucket_name, rgw_bucket& bucket,
                      map<std::string, bufferlist>& attrs, bool exclusive, uint64_t auid)
{
  /* system bucket name? */
  if (bucket_name[0] == '.') {
    bucket.name = bucket_name;
    bucket.pool = bucket_name;
    return rgwstore->create_bucket(id, bucket, attrs, true, exclusive, auid);
  }

  int ret = rgw_bucket_allocate_pool(bucket_name, bucket);
  if (ret < 0)
     return ret;

  ret = rgwstore->create_bucket(id, bucket, attrs, false, exclusive, auid);
  if (ret == -EEXIST) {
    return withdraw_pool(bucket.pool);
  }
  if (ret < 0)
    return ret;

  RGWBucketInfo info;
  info.bucket = bucket;
  ret = rgw_store_bucket_info(bucket_name, info);
  if (ret < 0) {
    RGW_LOG(0) << "failed to store bucket info, removing bucket" << dendl;
    rgwstore->delete_bucket(id, bucket, true);
    return ret;
  }

  return 0; 
}
