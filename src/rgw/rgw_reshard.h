// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_RESHARD_H
#define RGW_RESHARD_H

#include <vector>
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/lock/cls_lock_client.h"

class CephContext;
class RGWRados;

class RGWReshard {
    CephContext *cct;
    RGWRados *store;
    string lock_name;
    int max_jobs;
    rados::cls::lock::Lock instance_lock;
    librados::IoCtx io_ctx;

    int get_io_ctx(librados::IoCtx& io_ctx);

  public:
    RGWReshard(CephContext* cct, RGWRados* _store);
    int add(cls_rgw_reshard_entry& entry);
    int get_head(cls_rgw_reshard_entry& entry);
    int get(cls_rgw_reshard_entry& entry);
    int remove(cls_rgw_reshard_entry& entry);
    int list(string& marker, uint32_t max, list<cls_rgw_reshard_entry>& entries, bool& is_truncated);
    int set_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry);
    int clear_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry);
};

#endif
