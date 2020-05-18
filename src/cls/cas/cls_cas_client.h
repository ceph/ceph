// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_CAS_CLIENT_H
#define CEPH_CLS_CAS_CLIENT_H

#include "include/types.h"
#include "include/rados/librados_fwd.hpp"
#include "common/hobject.h"

//
// basic methods
//

/// create a chunk, or get additional reference if it already exists
void cls_cas_chunk_create_or_get_ref(
  librados::ObjectWriteOperation& op,
  const hobject_t& soid,
  const bufferlist& data,
  bool verify=false);

/// get ref on existing chunk
void cls_cas_chunk_get_ref(
  librados::ObjectWriteOperation& op,
  const hobject_t& soid);

/// drop reference on existing chunk
void cls_cas_chunk_put_ref(
  librados::ObjectWriteOperation& op,
  const hobject_t& soid);


//
// advanced (used for scrub, repair, etc.)
//

/// read list of all chunk references
int cls_cas_chunk_read_refs(
  librados::IoCtx& io_ctx,
  std::string& oid,
  std::set<hobject_t> *refs);

/// force update on chunk references
void cls_cas_chunk_set_refs(
  librados::ObjectWriteOperation& op,
  std::set<hobject_t>& refs);

/// check if a tiered rados object links to a chunk
int cls_cas_references_chunk(
  librados::IoCtx& io_ctx,
  const std::string& oid,
  const std::string& chunk_oid);

#endif
