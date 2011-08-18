#ifndef CEPH_RGW_BUCKET_H
#define CEPH_RGW_BUCKET_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"


#define BUCKETS_POOL_NAME ".buckets"


extern int rgw_get_bucket_info(string& bucket_name, RGWBucketInfo& info);
extern int rgw_store_bucket_info(string& bucket_name, RGWBucketInfo& info);
extern int rgw_remove_bucket_info(string& bucket_name);


#endif


