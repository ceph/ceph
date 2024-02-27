// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include <functional>
#include <string>

namespace rgw {
using AccessListFilter =
  std::function<bool(const std::string&, std::string&)>;

inline auto AccessListFilterPrefix(std::string prefix) {
  return [prefix = std::move(prefix)](const std::string& name,
				      std::string& key) {
    return (prefix.compare(key.substr(0, prefix.size())) == 0);
  };
}

namespace sal {

  class Driver;
  class User;
  class Bucket;
  class BucketList;
  class Object;
  class MultipartUpload;
  class Lifecycle;
  class Notification;
  class Writer;
  class PlacementTier;
  class ZoneGroup;
  class Zone;
  class LuaManager;
  struct RGWRoleInfo;
  class DataProcessor;
  class ObjectProcessor;
  class ReadStatsCB;

  class ConfigStore;
  class RealmWriter;
  class ZoneGroupWriter;
  class ZoneWriter;

} } // namespace rgw::sal
