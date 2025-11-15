// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLS_REFCOUNT_CLIENT_H
#define CEPH_CLS_REFCOUNT_CLIENT_H

#include <list>
#include <string>

#include "include/rados/librados_fwd.hpp"
#include "include/types.h"

/*
 * refcount objclass
 *
 * The refcount objclass implements a refcounting scheme that allows having multiple references
 * to a single rados object. The canonical way to use it is to add a reference and to remove a
 * reference using a specific tag. This way we ensure that refcounting operations are idempotent,
 * that is, a single client can only increase/decrease the refcount once using a single tag, so
 * any replay of operations (implicit or explicit) is possible.
 *
 * So, the regular usage would be to create an object, to increase the refcount. Then, when
 * wanting to have another reference to it, increase the refcount using a different tag. When
 * removing a reference it is required to drop the refcount (using the same tag that was used
 * for that reference). When the refcount drops to zero, the object is removed automatically.
 *
 * When refcount is set for the first time we need to add the src-tag as well.
 * The src-tag can be repeated multiple times or omitted after the first call
 */

int cls_refcount_get(librados::ObjectWriteOperation& op, const std::string& tag, const std::string& src_tag);
int cls_refcount_put(librados::ObjectWriteOperation& op, const std::string& tag);
int cls_refcount_set(librados::ObjectWriteOperation& op, std::list<std::string>& refs);
// these overloads which call io_ctx.operate() or io_ctx.exec() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()/exec()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_refcount_read(librados::IoCtx& io_ctx, std::string& oid, std::list<std::string> *refs);
#endif

#endif
