#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"


int rgw_put_obj(string& uid, rgw_bucket& bucket, string& oid, const char *data, size_t size);
int rgw_get_obj(void *ctx, rgw_bucket& bucket, string& key, bufferlist& bl);

#endif
