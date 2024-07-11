// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_types.h"


// initialize the cls_rgw_gc queue
void gc_log_init2(librados::ObjectWriteOperation& op,
                  uint64_t max_size, uint64_t max_deferred);

// enqueue a gc entry to omap with cls_rgw
void gc_log_enqueue1(librados::ObjectWriteOperation& op,
                     uint32_t expiration, cls_rgw_gc_obj_info& info);

// enqueue a gc entry to the cls_rgw_gc queue
void gc_log_enqueue2(librados::ObjectWriteOperation& op,
                     uint32_t expiration, const cls_rgw_gc_obj_info& info);

// defer a gc entry in omap with cls_rgw
void gc_log_defer1(librados::ObjectWriteOperation& op,
                   uint32_t expiration, const cls_rgw_gc_obj_info& info);

// defer a gc entry in the cls_rgw_gc queue
void gc_log_defer2(librados::ObjectWriteOperation& op,
                   uint32_t expiration, const cls_rgw_gc_obj_info& info);
