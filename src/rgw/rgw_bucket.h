#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"

#define BUCKETS_POOL_NAME ".rgw"
#define DEFAULT_BUCKET_STORE_POOL ".rgw.buckets"

extern int rgw_get_bucket_info_id(uint64_t bucket_id, RGWBucketInfo& info);
extern int rgw_get_bucket_info(string& bucket_name, RGWBucketInfo& info);
extern int rgw_store_bucket_info(RGWBucketInfo& info);
extern int rgw_bucket_select_host_pool(string& bucket_name, rgw_bucket& bucket);
extern int rgw_create_bucket(std::string& id, string& bucket_name, rgw_bucket& bucket,
                      map<std::string, bufferlist>& attrs, bool exclusive = true, uint64_t auid = 0);


#endif


