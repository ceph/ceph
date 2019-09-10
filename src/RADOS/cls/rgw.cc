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

#include <atomic>
#include <fmt/format.h>

#include "cls/rgw/cls_rgw_const.h"

#include "rgw.h"

namespace RADOS::CLS::rgw {
namespace {
template<typename T>
auto unsharded_decoder(bs::error_code* ecp, T* p) {
  return [p, ecp](bs::error_code ec, cb::list b) {
	   if (!ec && p) try {
	       decode(*p, b);
	     } catch (const bs::system_error& e) {
	       ec = e.code();
	     }
	   if (ecp)
	     *ecp = ec;
	 };
}

}

ReadOp bucket_list(const cls_rgw_obj_key& start_obj,
		   std::string_view filter_prefix,
		   std::uint32_t num_entries,
		   bool list_versions,
		   fu2::function<void(bs::error_code, cb::list)>&& f)
{
  ReadOp op;
  cb::list in;
  rgw_cls_list_op call;
  call.start_obj = start_obj;
  call.filter_prefix = filter_prefix;
  call.num_entries = num_entries;
  call.list_versions = list_versions;
  encode(call, in);
  op.exec(RGW_CLASS, RGW_BUCKET_LIST, in, std::move(f));
  return op;
}

ReadOp check_bucket(fu2::function<void(bs::error_code, cb::list)>&& f)
{
  ReadOp op;
  op.exec(RGW_CLASS, RGW_BUCKET_CHECK_INDEX, {}, std::move(f));
  return op;
}

WriteOp bucket_index_init()
{
  WriteOp op;
  op.create(true);
  op.exec(RGW_CLASS, RGW_BUCKET_INIT_INDEX, {});
  return op;
}

std::optional<std::string_view>
get_bi_shard(const bi_shards_mgr& mgr, int shard)
{
  if (auto i = mgr.find(shard); i != mgr.end())
    return i->second;
  return std::nullopt;
}

WriteOp bi_log_resync()
{
  WriteOp op;
  op.exec("rgw", "bi_log_resync", {});
  return op;
}

WriteOp bi_log_stop()
{
  WriteOp op;
  op.exec("rgw", "bi_log_stop", {});
  return op;
}

WriteOp bi_log_trim(std::optional<std::string_view> start,
		    std::optional<std::string_view> end,
		    bs::error_code* ec)
{
  WriteOp op;
  cls_rgw_bi_log_trim_op call;
  if (start)
    call.start_marker = *start;
  if (end)
    call.end_marker = *end;
  cb::list in;
  encode(call, in);
  op.exec(RGW_CLASS, RGW_BI_LOG_TRIM, in, ec);
  return op;
}

ReadOp get_bucket_resharding(cb::list* out)
{
  ReadOp op;
  cb::list in;
  cls_rgw_get_bucket_resharding_op call;
  encode(call, in);
  op.exec("rgw", "get_bucket_resharding", in, out);
  return op;
}

WriteOp rebuild_bucket()
{
  WriteOp op;
  op.exec(RGW_CLASS, RGW_BUCKET_REBUILD_INDEX, {});
  return op;
}

WriteOp set_bucket_resharding(const cls_rgw_bucket_instance_entry& entry)
{
  WriteOp op;
  cb::list in;
  cls_rgw_set_bucket_resharding_op call;
  call.entry = entry;
  encode(call, in);
  op.exec("rgw", "set_bucket_resharding", in);
  return op;
}


WriteOp set_tag_timeout(uint64_t timeout)
{
  WriteOp op;
  cb::list in;
  rgw_cls_tag_timeout_op call;
  call.tag_timeout = timeout;
  encode(call, in);
  op.exec(RGW_CLASS, RGW_BUCKET_SET_TAG_TIMEOUT, in);
  return op;
}

ReadOp bucket_list(cls_rgw_obj_key marker, std::string prefix,
		   uint32_t num_entries, bool list_versions,
		   rgw_cls_list_ret* result, bs::error_code* oec)
{
  ReadOp op;
  bufferlist in;
  rgw_cls_list_op call;
  call.start_obj = std::move(marker);
  call.filter_prefix = std::move(prefix);
  call.num_entries = num_entries;
  call.list_versions = list_versions;
  encode(call, in);
  op.exec(RGW_CLASS, RGW_BUCKET_LIST, in,
	  unsharded_decoder(oec, result));
  return op;
}

ReadOp bucket_list(cls_rgw_obj_key marker, std::string prefix,
		   uint32_t num_entries, bool list_versions,
		   fu2::unique_function<void(bs::error_code, cb::list)>&& f)
{
  ReadOp op;
  cb::list in;
  rgw_cls_list_op call;
  call.start_obj = marker;
  call.filter_prefix = prefix;
  call.num_entries = num_entries;
  call.list_versions = list_versions;
  encode(call, in);
  op.exec(RGW_CLASS, RGW_BUCKET_LIST, in, std::move(f));
  return op;
}

/*
 * convert from string. There are two options of how the string looks like:
 *
 * 1. Single shard, no shard id specified, e.g. 000001.23.1
 *
 * for this case, if passed shard_id >= 0, use this shard id,
 * otherwise assume that it's a bucket with no shards.
 *
 * 2. One or more shards, shard id specified for each shard, e.g.,
 * 0#00002.12,1#00003.23.2
 *
 */
std::optional<bi_shards_mgr>
make_bi_shards_mgr(std::string_view& composed_marker, int shard_id) noexcept {
  bi_shards_mgr ret;
  auto add = [&](int i, std::string_view v) {
	       if (!(ret.emplace(i, std::string(v)).second)) {
		 throw std::exception();
	       }
	     };
  // For early return!
  try {
    for_each_substr(composed_marker, SHARDS_SEPARATOR,
		  [&](std::string_view shard) {
		    size_t pos = shard.find(KEY_VALUE_SEPARATOR);
		    if (pos == std::string::npos) {
		      if (!ret.empty()) {
			throw std::exception{};
		      }
		      if (shard_id < 0) {
			add(0, shard);
		      } else {
			add(shard_id, shard);
		      }
		    } else {
		      auto shard_str = shard.substr(0, pos);
		      std::string err;
		      int shardno = (int)strict_strtol(shard_str, 10, &err);
		      if (!err.empty()) {
			throw std::exception();
		      }
		      add(shardno, shard.substr(pos + 1));
		    }
		  });
  } catch (const std::exception& e) {
    return std::nullopt;
  }
  return ret;
}

std::string to_string(const bi_shards_mgr& val) {
  std::string out;
  for (const auto& [shard, dat] : val) {
    if (out.length()) {
      // Not the first item, append a separator first
      out.append(SHARDS_SEPARATOR);
    }
    out.append(fmt::format("{0:d}{}{}", shard, KEY_VALUE_SEPARATOR, dat));
  }
  return out;
}

std::string_view get_shard_marker(std::string_view marker) {
  auto p = marker.find(KEY_VALUE_SEPARATOR);
  if (p == marker.npos) {
    return marker;
  }
  return marker.substr(p + 1);
}



ReadOp bi_log_list(std::optional<std::string_view> marker,
		   std::uint32_t max,
		   fu2::unique_function<void(bs::error_code, cb::list)>&& f)
{
  ReadOp op;
  cls_rgw_bi_log_list_op call;
  if (marker)
    call.marker = *marker;
  call.max = max;
  return op;
  bufferlist in;
  encode(call, in);
  op.exec(RGW_CLASS, RGW_BI_LOG_LIST, in, std::move(f));
  return op;
}

WriteOp suggest_changes(cb::list updates)
{
  WriteOp op;
  op.exec(RGW_CLASS, RGW_DIR_SUGGEST_CHANGES, updates);
  return op;
}

ReadOp usage_log_read(std::string user, std::string bucket,
		      uint64_t start_epoch, uint64_t end_epoch,
		      uint32_t max_entries,
		      std::string read_iter,
		      std::map<rgw_user_bucket, rgw_usage_log_entry>* usage,
		      std::string* next_iter,
		      bool *is_truncated, bs::error_code* oec)
{
  if (is_truncated)
    *is_truncated = false;

  ReadOp op;
  bufferlist in;
  rgw_cls_usage_log_read_op call;
  call.start_epoch = start_epoch;
  call.end_epoch = end_epoch;
  call.owner = std::move(user);
  call.max_entries = max_entries;
  call.bucket = std::move(bucket);
  call.iter = std::move(read_iter);
  encode(call, in);
  op.exec(RGW_CLASS, RGW_USER_USAGE_LOG_READ, in,
	  [=](bs::error_code ec, cb::list bl) {
	    if (!ec) try {
		rgw_cls_usage_log_read_ret result;
		auto iter = bl.cbegin();
		decode(result, iter);
		if (next_iter)
		  *next_iter = std::move(result.next_iter);
		if (is_truncated)
		  *is_truncated = result.truncated;
		if (usage)
		  *usage = std::move(result.usage);
	      } catch (const cb::error& e) {
		ec = e.code();
	      }
	    if (oec)
	      *oec = ec;
	  });
  return op;
}

ReadOp get_dir_header(std::function<void(
			int r, rgw_bucket_dir_header&)> handle_response)
{
  bufferlist in, out;
  rgw_cls_list_op call;
  call.num_entries = 0;
  encode(call, in);
  ReadOp op;
  op.exec(RGW_CLASS, RGW_BUCKET_LIST, in,
	  [handle_response](bs::error_code ec, cb::list bl) {
	    rgw_cls_list_ret ret;
	    if (!ec) try {
		auto iter = bl.cbegin();
		decode(ret, iter);
	      } catch (const cb::error& err) {
		ec = err.code();
	      }
	    handle_response(ceph::from_error_code(ec),
			    ret.dir.header);
	  });
  return op;
}


}
