// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RADOS_CLS_LOG_H
#define CEPH_RADOS_CLS_LOG_H

#include <string_view>
#include <tuple>
#include <vector>

#include <boost/system/error_code.hpp>

#include "include/RADOS/RADOS.hpp"

#include "include/buffer.h"

#include "common/ceph_time.h"

#include "cls/log/cls_log_types.h"

namespace RADOS::CLS::log {
namespace bs = boost::system;
namespace cb = ceph::buffer;

void add_prepare_entry(cls_log_entry& entry, ceph::real_time timestamp,
		       std::string_view section, std::string_view name,
		       cb::list&& bl);
void add(WriteOp& op, std::vector<cls_log_entry>&& entries,
         bool monotonic_inc);
void add(WriteOp& op, cls_log_entry&& entry);
void add(WriteOp& op, ceph::real_time timestamp, std::string_view section,
	 std::string_view name, cb::list&& bl);

WriteOp trim(ceph::real_time from_time, ceph::real_time to_time,
	     std::string_view from_marker, std::string_view to_marker);

ReadOp list(ceph::real_time from, ceph::real_time to,
	    std::string_view in_marker, int max_entries,
	    std::vector<cls_log_entry>* entries, std::string* marker,
	    bool* truncated, bs::error_code* oec = nullptr);

ReadOp info(cls_log_header* header, bs::error_code* oec = nullptr);
}

#endif // CEPH_RADOS_CLS_LOG_H
