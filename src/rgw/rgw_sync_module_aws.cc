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

struct AWSConfig {
  string id;
  RGWRESTConn *conn{nullptr};
};

// maybe use Fetch Remote Obj instead?
class RGWAWSHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  const AWSConfig& conf;
public:
  RGWAWSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                            RGWBucketInfo& _bucket_info,
                            rgw_obj_key& _key,
                            const AWSConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                         conf(_conf) {}
  int operate () override {
    auto store = sync_env->store;
    RGWRESTConn *conn = store->rest_master_conn;
    if (conn == nullptr)
      return -EIO;

    reenter(this) {
      ldout(sync_env->cct, 0) << "SYNC_BEGIN: stat of remote obj z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " attrs=" << attrs
                              << dendl;

      yield {
        // and here be dragons!
        // ultimately we should be using a form of  fetch obj that doesn't write to rados maybe?

        ldout(store->ctx(),0) << "abhi: If you're reading this, wait till things work!" << dendl;
        string obj_path = bucket_info.bucket.name + "/" + key.name;
        string res = nullptr;
        call(new RGWReadRESTResourceCR<string>(sync_env->cct,
                                               conn,
                                               sync_env->http_manager,
                                               obj_path,
                                               nullptr,
                                               &res));
        ldout(store->ctx(),0) << "abhi, printing obj" << dendl;
        ldout(store->ctx(),0) << res << dendl;

      }


    }
  }
};
