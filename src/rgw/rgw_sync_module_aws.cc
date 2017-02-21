#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_aws.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"

#define dout_subsys ceph_subsys_rgw

static string aws_object_name(const RGWBucketInfo& bucket_info, const rgw_obj_key&key, bool user_buckets=false){
  string obj_name;
  if(!user_buckets){
    obj_name = bucket_info.owner.tenant + bucket_info.owner.id + "/" + bucket_info.bucket.name + "/" + key.name;
      } else {
    // for future when every tenant gets their own bucket
    obj_name = bucket_info.bucket.name + "/" + key.name;
  }
  return obj_name;
}
