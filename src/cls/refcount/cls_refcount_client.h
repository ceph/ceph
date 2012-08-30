#ifndef CEPH_CLS_REFCOUNT_CLIENT_H
#define CEPH_CLS_REFCOUNT_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"

void cls_refcount_get(librados::ObjectWriteOperation& op, const string& tag, bool implicit_ref = false);
void cls_refcount_put(librados::ObjectWriteOperation& op, const string& tag, bool implicit_ref = false);
void cls_refcount_set(librados::ObjectWriteOperation& op, list<string>& refs);
int cls_refcount_read(librados::IoCtx& io_ctx, string& oid, list<string> *refs, bool implicit_ref = false);

#endif
