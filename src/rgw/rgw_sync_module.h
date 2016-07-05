#ifndef CEPH_RGW_SYNC_MODULE_H
#define CEPH_RGW_SYNC_MODULE_H

#include "rgw_common.h"

class RGWCoroutine;
class RGWBucketInfo;
class RGWRemoteDataLog;
struct RGWDataSyncEnv;
struct rgw_bucket_entry_owner;
struct rgw_obj_key;

class RGWDataSyncModule {
public:
  RGWDataSyncModule() {}
  virtual ~RGWDataSyncModule() {}

  virtual RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) = 0;
  virtual RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                      bool versioned, uint64_t versioned_epoch) = 0;
  virtual RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) = 0;
};

typedef std::shared_ptr<RGWDataSyncModule> RGWDataSyncModuleRef;

#endif
