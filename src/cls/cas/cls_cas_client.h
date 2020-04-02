// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_CAS_CLIENT_H
#define CEPH_CLS_CAS_CLIENT_H

#include "include/types.h"
#include "include/rados/librados_fwd.hpp"
#include "common/hobject.h"

void cls_chunk_refcount_get(librados::ObjectWriteOperation& op,const hobject_t& soid);
void cls_chunk_refcount_put(librados::ObjectWriteOperation& op, const hobject_t& soid);
void cls_chunk_refcount_set(librados::ObjectWriteOperation& op, std::set<hobject_t>& refs);
int cls_chunk_refcount_read(librados::IoCtx& io_ctx, std::string& oid, std::set<hobject_t> *refs);
int cls_chunk_has_chunk(librados::IoCtx& io_ctx, std::string& oid, std::string& fp_oid);
#endif
