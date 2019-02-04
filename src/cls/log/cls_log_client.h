#ifndef CEPH_CLS_LOG_CLIENT_H
#define CEPH_CLS_LOG_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls_log_types.h"

/*
 * log objclass
 */

void cls_log_add_prepare_entry(cls_log_entry& entry, const utime_t& timestamp,
                 const string& section, const string& name, bufferlist& bl);

void cls_log_add(librados::ObjectWriteOperation& op, list<cls_log_entry>& entries, bool monotonic_inc);
void cls_log_add(librados::ObjectWriteOperation& op, cls_log_entry& entry);
void cls_log_add(librados::ObjectWriteOperation& op, const utime_t& timestamp,
                 const string& section, const string& name, bufferlist& bl);

void cls_log_list(librados::ObjectReadOperation& op, utime_t& from, utime_t& to,
                  const string& in_marker, int max_entries,
		  list<cls_log_entry>& entries,
                  string *out_marker, bool *truncated);

void cls_log_trim(librados::ObjectWriteOperation& op, const utime_t& from_time, const utime_t& to_time,
                  const string& from_marker, const string& to_marker);
int cls_log_trim(librados::IoCtx& io_ctx, const string& oid, const utime_t& from_time, const utime_t& to_time,
                 const string& from_marker, const string& to_marker);

void cls_log_info(librados::ObjectReadOperation& op, cls_log_header *header);

#endif
