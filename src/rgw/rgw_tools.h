// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_TOOLS_H
#define CEPH_RGW_TOOLS_H

#include <string>

#include "include/types.h"
#include "common/ceph_time.h"
#include "rgw_common.h"

class RGWRados;
class RGWObjectCtx;
struct RGWObjVersionTracker;

struct obj_version;

int rgw_put_system_obj(RGWRados *rgwstore, const rgw_pool& pool, const string& oid, const bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime, map<string, bufferlist> *pattrs = NULL);
int rgw_get_system_obj(RGWRados *rgwstore, RGWObjectCtx& obj_ctx, const rgw_pool& pool, const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, real_time *pmtime, map<string, bufferlist> *pattrs = NULL,
                       rgw_cache_entry_info *cache_info = NULL,
		       boost::optional<obj_version> refresh_version = boost::none);
int rgw_delete_system_obj(RGWRados *rgwstore, const rgw_pool& pool, const string& oid,
                          RGWObjVersionTracker *objv_tracker);

const char *rgw_find_mime_by_ext(string& ext);

void rgw_filter_attrset(map<string, bufferlist>& unfiltered_attrset, const string& check_prefix,
                        map<string, bufferlist> *attrset);

int rgw_tools_init(CephContext *cct);
void rgw_tools_cleanup();

#endif
