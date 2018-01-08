#ifndef CEPH_RGW_ADMIN_COMMON_H
#define CEPH_RGW_ADMIN_COMMON_H

#include "cls/rgw/cls_rgw_types.h"

#include "rgw_common.h"
#include "rgw_rados.h"

int read_input(const string& infile, bufferlist& bl);

int init_bucket(RGWRados *store, const string& tenant_name, const string& bucket_name, const string& bucket_id,
                RGWBucketInfo& bucket_info, rgw_bucket& bucket, map<string, bufferlist> *pattrs = nullptr);

#endif //CEPH_RGW_ADMIN_COMMON_H
