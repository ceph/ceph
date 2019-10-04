// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_gc_log.h"

#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"
#include "cls/version/cls_version_client.h"


void gc_log_init2(librados::ObjectWriteOperation& op,
                  uint64_t max_size, uint64_t max_urgent)
{
  obj_version objv; // objv.ver = 0
  cls_version_check(op, objv, VER_COND_EQ);
  cls_rgw_gc_queue_init(op, max_size, max_urgent);
  objv.ver = 1;
  cls_version_set(op, objv);
}

void gc_log_enqueue1(librados::ObjectWriteOperation& op,
                     uint32_t expiration, cls_rgw_gc_obj_info& info)
{
  obj_version objv; // objv.ver = 0
  cls_version_check(op, objv, VER_COND_EQ);
  cls_rgw_gc_set_entry(op, expiration, info);
}

void gc_log_enqueue2(librados::ObjectWriteOperation& op,
                     uint32_t expiration, const cls_rgw_gc_obj_info& info)
{
  obj_version objv;
  objv.ver = 1;
  cls_version_check(op, objv, VER_COND_EQ);
  cls_rgw_gc_queue_enqueue(op, expiration, info);
}

void gc_log_defer1(librados::ObjectWriteOperation& op,
                   uint32_t expiration, const cls_rgw_gc_obj_info& info)
{
  obj_version objv; // objv.ver = 0
  cls_version_check(op, objv, VER_COND_EQ);
  cls_rgw_gc_defer_entry(op, expiration, info.tag);
}

void gc_log_defer2(librados::ObjectWriteOperation& op,
                   uint32_t expiration, const cls_rgw_gc_obj_info& info)
{
  obj_version objv;
  objv.ver = 1;
  cls_version_check(op, objv, VER_COND_EQ);
  cls_rgw_gc_queue_defer_entry(op, expiration, info);
  // TODO: conditional on whether omap is known to be empty
  cls_rgw_gc_remove(op, {info.tag});
}
