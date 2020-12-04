// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/buffer.h"
#include "node_types.h"

namespace crimson::os::seastore::onode {

/**
 * DeltaRecorder
 *
 * An abstracted class to encapsulate different implementations to apply delta
 * to a specific node layout.
 */
class DeltaRecorder {
 public:
  virtual ~DeltaRecorder() {
    assert(is_empty());
  }

  bool is_empty() const {
    return encoded.length() == 0;
  }

  ceph::bufferlist get_delta() {
    assert(!is_empty());
    return std::move(encoded);
  }

  virtual node_type_t node_type() const = 0;
  virtual field_type_t field_type() const = 0;
  virtual void apply_delta(ceph::bufferlist::const_iterator&,
                           NodeExtentMutable&) = 0;

 protected:
  DeltaRecorder() = default;
  ceph::bufferlist encoded;
};

}
