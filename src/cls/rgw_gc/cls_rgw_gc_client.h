#ifndef CEPH_CLS_RGW_GC_CLIENT_H
#define CEPH_CLS_RGW_GC_CLIENT_H

#include "include/rados/librados.hpp"
#include "cls/rgw_gc/cls_rgw_gc_types.h"
#include "cls/queue/cls_queue_ops.h"
#include "common/ceph_time.h"

void cls_rgw_gc_queue_init(librados::ObjectWriteOperation& op, uint64_t size, uint64_t num_deferred_entries);
int cls_rgw_gc_queue_get_capacity(librados::IoCtx& io_ctx, const string& oid, uint64_t& size);
void cls_rgw_gc_queue_enqueue(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info);
int cls_rgw_gc_queue_list_entries(librados::IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max, bool expired_only,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated, string& next_marker);
void cls_rgw_gc_queue_remove_entries(librados::ObjectWriteOperation& op, uint32_t num_entries);
void cls_rgw_gc_queue_defer_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info);

#endif
