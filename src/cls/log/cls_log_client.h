#ifndef CEPH_CLS_LOG_CLIENT_H
#define CEPH_CLS_LOG_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"

/*
 * log objclass
 */

void cls_log_add(librados::ObjectWriteOperation& op, cls_log_entry& entry);

void cls_log_list(librados::ObjectReadOperation& op, utime_t& from, int max, list<cls_log_entry>& entries);

void cls_log_trim(librados::ObjectWriteOperation& op, utime_t& from, utime_t& to);
int cls_log_trim(librados::IoCtx& io_ctx, string& oid, utime_t& from, utime_t& to);

#endif
