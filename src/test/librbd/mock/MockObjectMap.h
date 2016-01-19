// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_OBJECT_MAP_H
#define CEPH_TEST_LIBRBD_MOCK_OBJECT_MAP_H

#include "gmock/gmock.h"

namespace librbd {

struct MockObjectMap {
  MOCK_CONST_METHOD1(enabled, bool(const RWLock &object_map_lock));

  MOCK_METHOD1(open, void(Context *on_finish));

  MOCK_METHOD1(lock, void(Context *on_finish));
  MOCK_METHOD1(unlock, void(Context *on_finish));

  MOCK_METHOD2(snapshot_add, void(uint64_t snap_id, Context *on_finish));
  MOCK_METHOD2(snapshot_remove, void(uint64_t snap_id, Context *on_finish));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_OBJECT_MAP_H
