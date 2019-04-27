#ifndef CEPH_CLS_CAS_CLIENT_H
#define CEPH_CLS_CAS_CLIENT_H

#include "include/types.h"
#include "include/rados/librados_fwd.hpp"
#include "common/hobject.h"

void cls_chunk_refcount_get(librados::ObjectWriteOperation& op, const hobject_t& soid);
void cls_chunk_refcount_put(librados::ObjectWriteOperation& op, const hobject_t& soid);
void cls_chunk_refcount_set(librados::ObjectWriteOperation& op, set<hobject_t>& refs);
int cls_chunk_refcount_read(librados::IoCtx& io_ctx, string& oid, set<hobject_t> *refs);
int cls_chunk_has_chunk(librados::IoCtx& io_ctx, string& oid, string& fp_oid);
#endif
