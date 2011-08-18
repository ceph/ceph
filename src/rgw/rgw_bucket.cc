#include <errno.h>

#include <string>

#include "common/errno.h"
#include "rgw_access.h"

#include "rgw_bucket.h"
#include "rgw_tools.h"



static rgw_bucket pi_buckets(BUCKETS_POOL_NAME);


int rgw_store_bucket_info(string& bucket_name, RGWBucketInfo& info)
{
  bufferlist bl;

  ::encode(info, bl);

  string uid;
  int ret = rgw_put_obj(uid, pi_buckets, bucket_name, bl.c_str(), bl.length());
  if (ret < 0)
    return ret;

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

  return 0;
}

int rgw_remove_bucket_info(string& bucket_name)
{
  string uid;
  rgw_obj obj(pi_buckets, bucket_name);
  int ret = rgwstore->delete_obj(NULL, uid, obj);
  return ret;
}

int rgw_bucket_allocate_pool(string& bucket_name, rgw_bucket& bucket)
{
}

