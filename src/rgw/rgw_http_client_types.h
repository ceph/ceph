
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include <atomic>

struct rgw_io_id {
  int64_t id{0};
  int channels{0};

  rgw_io_id() {}
  rgw_io_id(int64_t _id, int _channels) : id(_id), channels(_channels) {}

  bool intersects(const rgw_io_id& rhs) {
    return (id == rhs.id && ((channels | rhs.channels) != 0));
  }

  bool operator<(const rgw_io_id& rhs) const {
    if (id < rhs.id) {
      return true;
    }
    return (id == rhs.id &&
            channels < rhs.channels);
  }
};

class RGWIOIDProvider
{
  std::atomic<int64_t> max = {0};

public:
  RGWIOIDProvider() {}
  int64_t get_next() {
    return ++max;
  }
};

class RGWIOProvider
{
  int64_t id{-1};

public:
  RGWIOProvider() {}
  virtual ~RGWIOProvider() = default;

  void assign_io(RGWIOIDProvider& io_id_provider, int io_type = -1);
  rgw_io_id get_io_id(int io_type) {
    return rgw_io_id{id, io_type};
  }

  virtual void set_io_user_info(void *_user_info) = 0;
  virtual void *get_io_user_info() = 0;
};

