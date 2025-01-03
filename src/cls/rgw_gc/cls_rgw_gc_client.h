// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/rados/librados.hpp"

#include "common/ceph_time.h"

#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw_gc/cls_rgw_gc_types.h"

void cls_rgw_gc_queue_init(librados::ObjectWriteOperation& op, uint64_t size, uint64_t num_deferred_entries);
int cls_rgw_gc_queue_get_capacity(librados::IoCtx& io_ctx, const std::string& oid, uint64_t& size);
void cls_rgw_gc_queue_enqueue(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info);
int cls_rgw_gc_queue_list_entries(librados::IoCtx& io_ctx, const std::string& oid, const std::string& marker, uint32_t max, bool expired_only,
				  std::list<cls_rgw_gc_obj_info>& entries, bool *truncated, std::string& next_marker);
void cls_rgw_gc_queue_remove_entries(librados::ObjectWriteOperation& op, uint32_t num_entries);
void cls_rgw_gc_queue_defer_entry(librados::ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info);
