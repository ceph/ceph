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

#ifndef CEPH_RADOS_CLS_RGW_H
#define CEPH_RADOS_CLS_RGW_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <boost/container/flat_map.hpp>
#include <boost/system/error_code.hpp>

#include "include/RADOS/RADOS.hpp"

#include "cls/rgw/cls_rgw_ops.h"

namespace RADOS::CLS::rgw {
namespace bc = boost::container;
namespace bs = boost::system;
namespace cb = ceph::buffer;

inline constexpr std::string_view KEY_VALUE_SEPARATOR{"#"};
inline constexpr std::string_view SHARDS_SEPARATOR{","};

ReadOp bucket_list(const cls_rgw_obj_key& start_obj,
		   std::string_view filter_prefix,
		   std::uint32_t num_entries,
		   bool list_versions,
		   fu2::function<void(bs::error_code, cb::list)>&& f);

ReadOp check_bucket(fu2::function<void(bs::error_code, cb::list)>&& f);

ReadOp get_bucket_resharding(cb::list* out);
WriteOp rebuild_bucket();
WriteOp set_bucket_resharding(const cls_rgw_bucket_instance_entry& entry);
WriteOp set_tag_timeout(uint64_t timeout);


using bi_shards_mgr = bc::flat_map<int, std::string>;

std::optional<bi_shards_mgr>
make_bi_shards_mgr(std::string_view& composed_marker, int shard_id) noexcept;
std::string to_string(const bi_shards_mgr& val);
std::string_view get_shard_marker(std::string_view marker);
std::optional<std::string_view>
get_bi_shard(const bi_shards_mgr& mgr, int shard);


WriteOp suggest_changes(cb::list updates);

ReadOp bucket_list(cls_rgw_obj_key marker, std::string prefix,
		   uint32_t num_entries, bool list_versions,
		   rgw_cls_list_ret* result,
		   bs::error_code* oec = nullptr);

ReadOp usage_log_read(std::string user, std::string bucket,
		      uint64_t start_epoch, uint64_t end_epoch,
		      uint32_t max_entries,
		      std::string read_iter,
		      std::map<rgw_user_bucket, rgw_usage_log_entry>* usage,
		      std::string* next_iter,
		      bool *is_truncated, bs::error_code* oec);

ReadOp get_dir_header(std::function<void(
			int r, rgw_bucket_dir_header&)> handle_response);

WriteOp bucket_index_init();
WriteOp bi_log_resync();
WriteOp bi_log_stop();
WriteOp bi_log_trim(std::optional<std::string_view> start,
		    std::optional<std::string_view> end,
		    bs::error_code* ec);
ReadOp bi_log_list(std::optional<std::string_view> marker,
		   std::uint32_t max,
		   fu2::unique_function<void(bs::error_code, cb::list)>&& f);


}

#endif // CEPH_RADOS_CLS_RGW_H
