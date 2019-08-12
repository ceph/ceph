

#include "svc_bucket.h"

#define dout_subsys ceph_subsys_rgw

string RGWSI_Bucket::get_entrypoint_meta_key(const rgw_bucket& bucket)
{
  if (bucket.bucket_id.empty()) {
    return bucket.get_key();
  }

  rgw_bucket b(bucket);
  b.bucket_id.clear();

  return b.get_key();
}

string RGWSI_Bucket::get_bi_meta_key(const rgw_bucket& bucket)
{
  return bucket.get_key();
}

