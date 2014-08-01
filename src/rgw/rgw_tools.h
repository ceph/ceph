// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "rgw_common.h"

class RGWRados;
struct RGWObjVersionTracker;

struct obj_version;

int rgw_put_system_obj(RGWRados *rgwstore, rgw_bucket& bucket, string& oid, const char *data, size_t size, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, time_t set_mtime, map<string, bufferlist> *pattrs = NULL);
int rgw_get_system_obj(RGWRados *rgwstore, void *ctx, rgw_bucket& bucket, const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, time_t *pmtime, map<string, bufferlist> *pattrs = NULL,
                       rgw_cache_entry_info *cache_info = NULL);

int rgw_tools_init(CephContext *cct);
void rgw_tools_cleanup();
const char *rgw_find_mime_by_ext(string& ext);

#endif
