#ifndef CEPH_CLS_QUEUE_CLIENT_H
#define CEPH_CLS_QUEUE_CLIENT_H

#include "include/str_list.h"
#include "include/rados/librados.hpp"
#include "cls/queue/cls_queue_types.h"
#include "cls_queue_ops.h"
#include "common/RefCountedObj.h"
#include "include/compat.h"
#include "common/ceph_time.h"
#include "common/Cond.h"

void cls_rgw_gc_init_queue(librados::ObjectWriteOperation& op, string& queue_name, uint64_t& size, uint64_t& num_urgent_data_entries);
int cls_rgw_gc_get_queue_size(librados::IoCtx& io_ctx, string& oid, uint64_t& size);
void cls_rgw_gc_enqueue(librados::ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info);
int cls_rgw_gc_list_queue(librados::IoCtx& io_ctx, string& oid, string& marker, uint32_t max, bool expired_only,
                    list<cls_rgw_gc_obj_info>& entries, bool *truncated, string& next_marker);
void cls_rgw_gc_remove_entries_queue(librados::ObjectWriteOperation& op, uint32_t num_entries);
void cls_rgw_gc_defer_entry_queue(librados::ObjectWriteOperation& op, uint32_t expiration_secs, cls_rgw_gc_obj_info& info);

#endif