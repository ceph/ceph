// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_MIGRATION_MOCK_STREAM_INTERFACE_H
#define CEPH_TEST_LIBRBD_MOCK_MIGRATION_MOCK_STREAM_INTERFACE_H

#include "include/buffer.h"
#include "gmock/gmock.h"
#include "librbd/migration/StreamInterface.h"

namespace librbd {
namespace migration {

struct MockStreamInterface : public StreamInterface {
  MOCK_METHOD1(open, void(Context*));
  MOCK_METHOD1(close, void(Context*));

  MOCK_METHOD2(get_size, void(uint64_t*, Context*));

  MOCK_METHOD3(read, void(const io::Extents&, bufferlist*, Context*));
  void read(io::Extents&& byte_extents, bufferlist* bl, Context* on_finish) {
    read(byte_extents, bl, on_finish);
  }

  MOCK_METHOD3(list_sparse_extents, void(const io::Extents&,
                                         io::SparseExtents*, Context*));
  void list_sparse_extents(io::Extents&& byte_extents,
                           io::SparseExtents* sparse_extents,
                           Context* on_finish) {
    list_sparse_extents(byte_extents, sparse_extents, on_finish);
  }
};

} // namespace migration
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_MIGRATION_MOCK_STREAM_INTERFACE_H
