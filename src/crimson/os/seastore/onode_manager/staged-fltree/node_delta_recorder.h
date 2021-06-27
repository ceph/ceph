// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/buffer.h"
#include "node_types.h"
#include "value.h"

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
    /* May be non-empty if transaction is abandoned without
     * being submitted -- conflicts are a particularly common
     * example (denoted generally by returning crimson::ct_error::eagain).
     */
  }

  bool is_empty() const {
    return encoded.length() == 0;
  }

  ceph::bufferlist get_delta() {
    assert(!is_empty());
    return std::move(encoded);
  }

  ValueDeltaRecorder* get_value_recorder() const {
    assert(value_recorder);
    return value_recorder.get();
  }

  virtual node_type_t node_type() const = 0;
  virtual field_type_t field_type() const = 0;
  virtual void apply_delta(ceph::bufferlist::const_iterator&,
                           NodeExtentMutable&,
                           const NodeExtent&) = 0;

 protected:
  DeltaRecorder() = default;
  DeltaRecorder(const ValueBuilder& vb)
    : value_recorder{vb.build_value_recorder(encoded)} {}

  ceph::bufferlist encoded;
  std::unique_ptr<ValueDeltaRecorder> value_recorder;
};

}
