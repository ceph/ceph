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

#ifndef CEPH_RADOS_CLS_TIMEINDEX_H
#define CEPH_RADOS_CLS_TIMEINDEX_H

#include <string_view>
#include <tuple>
#include <vector>

#include <boost/system/error_code.hpp>

#include "include/RADOS/RADOS.hpp"

#include "include/buffer.h"

#include "common/ceph_time.h"

#include "cls/timeindex/cls_timeindex_ops.h"

namespace RADOS::CLS::timeindex {
namespace bs = boost::system;
namespace cb = ceph::buffer;

cls_timeindex_entry add_prepare_entry(ceph::real_time key_timestamp,
				      std::optional<std::string_view> key_ext,
				      cb::list&& bl);

WriteOp add(std::vector<cls_timeindex_entry>&& entries);

WriteOp add(cls_timeindex_entry&& entry);

WriteOp add(ceph::real_time timestamp, std::optional<std::string_view> name,
	    cb::list&& bl);

ReadOp list(ceph::real_time from, ceph::real_time to,
	    std::string_view in_marker, int max_entries,
	    cb::list* bl, bs::error_code* ec);

WriteOp trim(ceph::real_time from_time, ceph::real_time to_time,
	     std::optional<std::string_view> from_marker,
	     std::optional<std::string_view> to_marker);

}

#endif // CEPH_RADOS_CLS_TIMEINDEX_H
