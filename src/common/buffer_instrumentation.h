// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/buffer.h"
#include "include/buffer_raw.h"

namespace ceph::buffer_instrumentation {

// this is nothing more than an intermediary for a class hierarchy which
// can placed between a user's custom raw and the `ceph::buffer::raw` to
// detect whether a given `ceph::buffer::ptr` instance wraps a particular
// raw's implementation (via `dynamic_cast` or `typeid`).
//
// users are supposed to define marker type (e.g. `class my_marker{}`).
// this marker. i
template <class MarkerT>
struct instrumented_raw : public ceph::buffer::raw {
  using raw::raw;
};

struct instrumented_bptr : public ceph::buffer::ptr {
  const ceph::buffer::raw* get_raw() const {
    return _raw;
  }

  template <class MarkerT>
  bool is_raw_marked() const {
    return dynamic_cast<const instrumented_raw<MarkerT>*>(get_raw()) != nullptr;
  }
};

} // namespace ceph::buffer_instrumentation
