/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include <algorithm>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <concepts>
#include <memory>
#include <fmt/ranges.h>
#include <string_view>
#include <utility>
#include "common/dout.h"
#include "common/inline_variant.h"

#include "rgw/yield_completion.h"

#include "d4n_directory.h"

namespace rgw::d4n {

std::map<std::string, std::string> BlockDirectory::encode_fields(const CacheBlock& block)
{
  std::map<std::string, std::string> values;

  // We have a lot of boilerplate conversion, let's somewhat ameliorate it--
  const auto add = overloaded {
    [&]<typename ValueT>(const std::string_view key, ValueT&& value)
    requires std::convertible_to<ValueT, std::string_view>
    { values.emplace(key, std::forward<ValueT>(value));
    },

    [&]<std::integral ValueT>(const std::string_view key, const ValueT value) {
      values.emplace(key, std::to_string(value));
    },

    [&](const std::string_view key, const buffer::list& value) {
      values.emplace(key, value.to_str());
    }
  };

  add("blockID", block.blockID);
  add("version", block.version);
  add("deleteMarker", block.deleteMarker);
  add("invalid", block.invalid);
  add("size", block.size);
  add("globalWeight", block.globalWeight);
  add("objName", block.cacheObj.objName);
  add("bucketName", block.cacheObj.bucketName);
  add("creationTime", block.cacheObj.creationTime);
  add("dirty", block.cacheObj.dirty);
  add("hosts", fmt::format("{}", fmt::join(block.cacheObj.hostsList, "_")));
  add("etag", block.cacheObj.etag);
  add("objSize", block.cacheObj.size);
  add("userId", block.cacheObj.user_id);
  add("displayName", block.cacheObj.display_name);
  add("acl", block.cacheObj.acl);
  add("attrsCount", block.cacheObj.attrs.size());

  for (const auto& [key, value] : block.cacheObj.attrs) {
    add("attr_" + key, value);
  }

  return values;
}

int BlockDirectory::get(const DoutPrefixProvider *dpp, optional_yield y, CacheBlock *block,
                        std::optional<Transaction> txn)
{
  if (!block) {
    return -EINVAL;
  }

  auto result = find(dpp, y, *block, txn);
  if (!result || !*result) {
    return result ? -ENOENT : result.error();
  }

  *block = std::move(**result);

  return 0;
}

std::string ObjectDirectory::build_index(const std::string& bucket_id, const std::string& obj_name) 
{
  auto key = url_encode(bucket_id, true);

  key.push_back('#');

  // Accept logical names and preserve the existing get_oid() spelling of version roots:
  if (obj_name.starts_with('_')) {
    key.push_back('_');
  }

  url_encode(obj_name, key, true);

  return key;
}

std::string BlockDirectory::build_index(const CacheBlock* block) 
{
  std::string key = url_encode(block->cacheObj.bucketName, true) + "#" + url_encode(block->cacheObj.objName, true) + "/block/";

  if (block->size > 0) {
    key += block->version + "/";
  }

  key += std::to_string(block->blockID) + "/" + std::to_string(block->size);

  return key;
}

} // namespace rgw::d4n
