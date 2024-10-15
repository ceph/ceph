// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_STREAM_INTERFACE_H
#define CEPH_LIBRBD_MIGRATION_STREAM_INTERFACE_H

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "librbd/io/Types.h"

struct Context;

namespace librbd {
namespace migration {

struct StreamInterface {
  virtual ~StreamInterface() {
  }

  virtual void open(Context* on_finish) = 0;
  virtual void close(Context* on_finish) = 0;

  virtual void get_size(uint64_t* size, Context* on_finish) = 0;

  virtual void read(io::Extents&& byte_extents, bufferlist* data,
                    Context* on_finish) = 0;

  virtual void list_sparse_extents(io::Extents&& byte_extents,
                                   io::SparseExtents* sparse_extents,
                                   Context* on_finish) = 0;
};

} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_STREAM_INTERFACE_H
