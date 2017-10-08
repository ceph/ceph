// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <core/sharded.hh>

#include "include/buffer_raw.h"

using temporary_buffer = seastar::temporary_buffer<char>;

namespace ceph {
namespace buffer {

class raw_seastar_foreign_ptr : public raw {
  seastar::foreign_ptr<temporary_buffer> ptr;
 public:
  raw_seastar_foreign_ptr(temporary_buffer&& buf)
    : raw(buf.get_write(), buf.size()), ptr(std::move(buf)) {}
  raw* clone_empty() override {
    return create(len);
  }
};

raw* create_foreign(temporary_buffer&& buf) {
  return new raw_seastar_foreign_ptr(std::move(buf));
}

class raw_seastar_local_ptr : public raw {
  temporary_buffer buf;
 public:
  raw_seastar_local_ptr(temporary_buffer&& buf)
    : raw(buf.get_write(), buf.size()), buf(std::move(buf)) {}
  raw* clone_empty() override {
    return create(len);
  }
};

raw* create(temporary_buffer&& buf) {
  return new raw_seastar_local_ptr(std::move(buf));
}

} // namespace buffer
} // namespace ceph
