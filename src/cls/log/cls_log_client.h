// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_LOG_CLIENT_H
#define CEPH_CLS_LOG_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls_log_types.h"

/*
 * log objclass
 */

void cls_log_add_prepare_entry(cls::log::entry& entry, ceph::real_time timestamp,
			       const std::string& section,
			       const std::string& name, ceph::buffer::list& bl);

void cls_log_add(librados::ObjectWriteOperation& op, std::vector<cls::log::entry>& entries, bool monotonic_inc);
void cls_log_add(librados::ObjectWriteOperation& op, cls::log::entry& entry);
void cls_log_add(librados::ObjectWriteOperation& op, ceph::real_time timestamp,
                 const std::string& section, const std::string& name, ceph::buffer::list& bl);

void cls_log_list(librados::ObjectReadOperation& op, ceph::real_time from,
		  ceph::real_time to, const std::string& in_marker,
		  int max_entries, std::vector<cls::log::entry>& entries,
                  std::string *out_marker, bool *truncated);

void cls_log_trim(librados::ObjectWriteOperation& op, ceph::real_time from_time,
		  ceph::real_time to_time, const std::string& from_marker,
		  const std::string& to_marker);

// these overloads which call io_ctx.operate() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_log_trim(librados::IoCtx& io_ctx, const std::string& oid,
		 ceph::real_time from_time, ceph::real_time to_time,
                 const std::string& from_marker, const std::string& to_marker);
#endif

void cls_log_info(librados::ObjectReadOperation& op, cls::log::header* header);

#endif
