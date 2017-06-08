// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_USER_CLIENT_H
#define CEPH_CLS_USER_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_user_types.h"
#include "common/RefCountedObj.h"

class RGWGetUserHeader_CB : public RefCountedObject {
public:
  virtual ~RGWGetUserHeader_CB() {}
  virtual void handle_response(int r, cls_user_header& header) = 0;
};

/*
 * user objclass
 */

void cls_user_set_buckets(librados::ObjectWriteOperation& op, list<cls_user_bucket_entry>& entries, bool add);
void cls_user_complete_stats_sync(librados::ObjectWriteOperation& op);
void cls_user_remove_bucket(librados::ObjectWriteOperation& op,  const cls_user_bucket& bucket);
void cls_user_bucket_list(librados::ObjectReadOperation& op,
                       const string& in_marker,
                       const string& end_marker,
                       int max_entries,
                       list<cls_user_bucket_entry>& entries,
                       string *out_marker,
                       bool *truncated,
                       int *pret);
void cls_user_get_header(librados::ObjectReadOperation& op, cls_user_header *header, int *pret);
int cls_user_get_header_async(librados::IoCtx& io_ctx, string& oid, RGWGetUserHeader_CB *ctx);

#endif
