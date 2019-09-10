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

#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <boost/system/error_code.hpp>

#include "include/RADOS/RADOS.hpp"
#include "include/buffer.h"

#include "cls/user/cls_user_types.h"

namespace RADOS::CLS::user {
namespace bs = boost::system;
namespace cb = ceph::buffer;

void set_buckets(WriteOp& op, const std::vector<cls_user_bucket_entry>& entries,
                 bool add);
void complete_stats_sync(WriteOp& op);
void remove_bucket(WriteOp& op, const cls_user_bucket& bucket);
void reset_stats(WriteOp& op);
ReadOp bucket_list(std::string_view in_marker,
		   std::string_view end_marker,
		   int max_entries, cb::list* out,
		   bs::error_code* ec);
ReadOp get_header(cb::list* bl, bs::error_code* ec);

}
