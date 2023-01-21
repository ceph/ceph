// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOG_CLIENT_H
#define CEPH_CLS_LOG_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls_log_types.h"

/*
 * log objclass
 */

void cls_log_add_prepare_entry(cls_log_entry& entry, const utime_t& timestamp,
                 const std::string& section, const std::string& name, ceph::buffer::list& bl);

void cls_log_add(librados::ObjectWriteOperation& op, std::vector<cls_log_entry>& entries, bool monotonic_inc);
void cls_log_add(librados::ObjectWriteOperation& op, cls_log_entry& entry);
void cls_log_add(librados::ObjectWriteOperation& op, const utime_t& timestamp,
                 const std::string& section, const std::string& name, ceph::buffer::list& bl);

void cls_log_list(librados::ObjectReadOperation& op, const utime_t& from,
		  const utime_t& to, const std::string& in_marker,
		  int max_entries, std::vector<cls_log_entry>& entries,
                  std::string *out_marker, bool *truncated);

void cls_log_trim(librados::ObjectWriteOperation& op, const utime_t& from_time, const utime_t& to_time,
                  const std::string& from_marker, const std::string& to_marker);

// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_log_trim(librados::IoCtx& io_ctx, const std::string& oid, const utime_t& from_time, const utime_t& to_time,
                 const std::string& from_marker, const std::string& to_marker);
#endif

void cls_log_info(librados::ObjectReadOperation& op, cls_log_header *header);

#endif
