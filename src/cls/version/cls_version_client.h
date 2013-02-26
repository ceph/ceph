#ifndef CEPH_CLS_VERSION_CLIENT_H
#define CEPH_CLS_VERSION_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"

/*
 * version objclass
 */

void cls_version_set(librados::ObjectWriteOperation& op, obj_version& ver);

/* increase anyway */
void cls_version_inc(librados::ObjectWriteOperation& op);

/* inc only if ver matches (if not empty), otherwise return -EAGAIN */
void cls_version_inc_conditional(librados::ObjectWriteOperation& op, obj_version& ver);

int cls_refcount_read(librados::IoCtx& io_ctx, obj_version *ver);

#endif
