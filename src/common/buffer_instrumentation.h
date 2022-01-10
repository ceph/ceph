// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/buffer.h"

namespace ceph::buffer_instrumentation {

struct instrumented_bptr : public ceph::buffer::ptr {
  const ceph::buffer::raw* get_raw() const {
    return _raw;
  }
};

} // namespace ceph::buffer_instrumentation
