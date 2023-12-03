// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <algorithm>
#include <iterator>
#include <list>
#include <string>
#include <vector>
#include "services/svc_sys_obj.h"

class DoutPrefixProvider;

class RGWMetadataLister {
  RGWSI_SysObj::Pool pool;
  RGWSI_SysObj::Pool::Op listing;

  virtual void filter_transform(std::vector<std::string>& oids,
                                std::list<std::string>& keys) {
    // use all oids as keys
    std::move(oids.begin(), oids.end(), std::back_inserter(keys));
  }

 public:
  explicit RGWMetadataLister(RGWSI_SysObj::Pool pool)
    : pool(pool), listing(this->pool) {}
  virtual ~RGWMetadataLister() {}

  int init(const DoutPrefixProvider* dpp,
           const std::string& marker,
           const std::string& prefix)
  {
    return listing.init(dpp, marker, prefix);
  }

  int get_next(const DoutPrefixProvider* dpp, int max,
               std::list<std::string>& keys, bool* truncated)
  {
    std::vector<std::string> oids;
    int r = listing.get_next(dpp, max, &oids, truncated);
    if (r == -ENOENT) {
      if (truncated) {
        *truncated = false;
      }
      return 0;
    }
    if (r < 0) {
      return r;
    }
    filter_transform(oids, keys);
    return 0;
  }

  std::string get_marker()
  {
    std::string marker;
    listing.get_marker(&marker);
    return marker;
  }
};
