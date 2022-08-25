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


namespace rgw { namespace sal {

  class Store;
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

} } // namespace rgw::sal
