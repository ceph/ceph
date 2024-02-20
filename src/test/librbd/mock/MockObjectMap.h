// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_OBJECT_MAP_H
#define CEPH_TEST_LIBRBD_MOCK_OBJECT_MAP_H

#include "librbd/Utils.h"
#include "gmock/gmock.h"

namespace librbd {

struct MockObjectMap {
  MOCK_METHOD1(at, uint8_t(uint64_t));
  uint8_t operator[](uint64_t object_no) {
    return at(object_no);
  }

  MOCK_CONST_METHOD0(size, uint64_t());

  MOCK_METHOD1(open, void(Context *on_finish));
  MOCK_METHOD1(close, void(Context *on_finish));

  MOCK_METHOD3(aio_resize, void(uint64_t new_size, uint8_t default_object_state,
                                Context *on_finish));

  void get() {}
  void put() {}

  template <typename T, void(T::*MF)(int) = &T::complete>
  bool aio_update(uint64_t snap_id, uint64_t start_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
                  const ZTracer::Trace &parent_trace, bool ignore_enoent,
                  bool force, T *callback_object) {
    return aio_update<T, MF>(snap_id, start_object_no, start_object_no + 1,
                             new_state, current_state, parent_trace,
                             ignore_enoent, force, callback_object);
  }

  template <typename T, void(T::*MF)(int) = &T::complete>
  bool aio_update(uint64_t snap_id, uint64_t start_object_no,
                  uint64_t end_object_no, uint8_t new_state,
                  const boost::optional<uint8_t> &current_state,
                  const ZTracer::Trace &parent_trace, bool ignore_enoent,
                  bool force, T *callback_object) {
    auto ctx = util::create_context_callback<T, MF>(callback_object);
    bool updated = aio_update(snap_id, start_object_no, end_object_no,
                              new_state, current_state, parent_trace,
                              ignore_enoent, force, ctx);
    if (!updated) {
      delete ctx;
    }
    return updated;
  }
  MOCK_METHOD9(aio_update, bool(uint64_t snap_id, uint64_t start_object_no,
                                uint64_t end_object_no, uint8_t new_state,
                                const boost::optional<uint8_t> &current_state,
                                const ZTracer::Trace &parent_trace,
                                bool ignore_enoent, bool force,
				Context *on_finish));

  MOCK_METHOD2(snapshot_add, void(uint64_t snap_id, Context *on_finish));
  MOCK_METHOD2(snapshot_remove, void(uint64_t snap_id, Context *on_finish));
  MOCK_METHOD2(rollback, void(uint64_t snap_id, Context *on_finish));

  MOCK_CONST_METHOD1(object_may_exist, bool(uint64_t));

};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_OBJECT_MAP_H
