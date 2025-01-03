// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "rgw_basic_types.h"
#include "rgw_tools.h"
#include "rgw_sal_config.h"

namespace rgw::rados {

// write options that control object creation
enum class Create {
  MustNotExist, // fail with EEXIST if the object already exists
  MayExist, // create if the object didn't exist, overwrite if it did
  MustExist, // fail with ENOENT if the object doesn't exist
};

struct ConfigImpl {
  librados::Rados rados;

  const rgw_pool realm_pool;
  const rgw_pool period_pool;
  const rgw_pool zonegroup_pool;
  const rgw_pool zone_pool;

  ConfigImpl(const ceph::common::ConfigProxy& conf);

  int read(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& oid,
           bufferlist& bl, RGWObjVersionTracker* objv);

  template <typename T>
  int read(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& oid,
           T& data, RGWObjVersionTracker* objv)
  {
    bufferlist bl;
    int r = read(dpp, y, pool, oid, bl, objv);
    if (r < 0) {
      return r;
    }
    try {
      auto p = bl.cbegin();
      decode(data, p);
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from "
          << pool << ":" << oid << dendl;
      return -EIO;
    }
    return 0;
  }

  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, Create create,
            const bufferlist& bl, RGWObjVersionTracker* objv);

  template <typename T>
  int write(const DoutPrefixProvider* dpp, optional_yield y,
            const rgw_pool& pool, const std::string& oid, Create create,
            const T& data, RGWObjVersionTracker* objv)
  {
    bufferlist bl;
    encode(data, bl);

    return write(dpp, y, pool, oid, create, bl, objv);
  }

  int remove(const DoutPrefixProvider* dpp, optional_yield y,
             const rgw_pool& pool, const std::string& oid,
             RGWObjVersionTracker* objv);

  int list(const DoutPrefixProvider* dpp, optional_yield y,
           const rgw_pool& pool, const std::string& marker,
           std::regular_invocable<std::string> auto filter,
           std::span<std::string> entries,
           sal::ListResult<std::string>& result)
  {
    librados::IoCtx ioctx;
    int r = rgw_init_ioctx(dpp, &rados, pool, ioctx, true, false);
    if (r < 0) {
      return r;
    }
    librados::ObjectCursor oc;
    if (!oc.from_str(marker)) {
      ldpp_dout(dpp, 10) << "failed to parse cursor: " << marker << dendl;
      return -EINVAL;
    }
    std::size_t count = 0;
    try {
      auto iter = ioctx.nobjects_begin(oc);
      const auto end = ioctx.nobjects_end();
      for (; count < entries.size() && iter != end; ++iter) {
        std::string entry = filter(iter->get_oid());
        if (!entry.empty()) {
          entries[count++] = std::move(entry);
        }
      }
      if (iter == end) {
        result.next.clear();
      } else {
        result.next = iter.get_cursor().to_str();
      }
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 10) << "NObjectIterator exception " << e.what() << dendl;
      return -EIO;
    }
    result.entries = entries.first(count);
    return 0;
  }

  int notify(const DoutPrefixProvider* dpp, optional_yield y,
             const rgw_pool& pool, const std::string& oid,
             bufferlist& bl, uint64_t timeout_ms);
};

inline std::string_view name_or_default(std::string_view name,
                                        std::string_view default_name)
{
  if (!name.empty()) {
    return name;
  }
  return default_name;
}

} // namespace rgw::rados
