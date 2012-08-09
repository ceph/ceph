#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"

class RGWRados;

int rgw_put_obj(RGWRados *rgwstore, string& uid, rgw_bucket& bucket, string& oid, const char *data, size_t size, bool exclusive, map<string, bufferlist> *pattrs = NULL);
int rgw_get_obj(RGWRados *rgwstore, void *ctx, rgw_bucket& bucket, string& key, bufferlist& bl, map<string, bufferlist> *pattrs = NULL);

int rgw_tools_init(CephContext *cct);
const char *rgw_find_mime_by_ext(string& ext);

#endif
