#ifndef CEPH_CLS_VERSION_CLIENT_H
#define CEPH_CLS_VERSION_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls_version_ops.h"

/*
 * version objclass
 */

void cls_version_set(librados::ObjectWriteOperation& op, obj_version& ver);

/* increase anyway */
void cls_version_inc(librados::ObjectWriteOperation& op);

/* conditional increase, return -EAGAIN if condition fails */
void cls_version_inc(librados::ObjectWriteOperation& op, obj_version& ver, VersionCond cond);

void cls_version_read(librados::ObjectReadOperation& op, obj_version *objv);

int cls_version_read(librados::IoCtx& io_ctx, string& oid, obj_version *ver);

void cls_version_check(librados::ObjectOperation& op, obj_version& ver, VersionCond cond);

#endif
