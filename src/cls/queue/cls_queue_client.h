#ifndef CEPH_CLS_QUEUE_CLIENT_H
#define CEPH_CLS_QUEUE_CLIENT_H

#include "include/rados/librados.hpp"
#include "cls/queue/cls_queue_types.h"
#include "cls_queue_ops.h"
#include "common/ceph_time.h"

void cls_queue_init(librados::ObjectWriteOperation& op, const string& queue_name, uint64_t size);
int cls_queue_get_capacity(librados::IoCtx& io_ctx, const string& oid, uint64_t& size);
void cls_queue_enqueue(librados::ObjectWriteOperation& op, uint32_t expiration_secs, vector<bufferlist> bl_data_vec);
int cls_queue_list_entries(librados::IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max,
                    vector<cls_queue_entry>& entries, bool *truncated, string& next_marker);
void cls_queue_remove_entries(librados::ObjectWriteOperation& op, const string& end_marker);

#endif