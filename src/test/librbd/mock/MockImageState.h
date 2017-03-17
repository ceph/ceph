// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IMAGE_STATE_H
#define CEPH_TEST_LIBRBD_MOCK_IMAGE_STATE_H

#include <gmock/gmock.h>

#include "cls/rbd/cls_rbd_types.h"

class Context;

namespace librbd {

struct MockImageState {
  MOCK_CONST_METHOD0(is_refresh_required, bool());
  MOCK_METHOD1(refresh, void(Context*));

  MOCK_METHOD2(open, void(bool, Context*));

  MOCK_METHOD0(close, int());
  MOCK_METHOD1(close, void(Context*));

  MOCK_METHOD3(snap_set, void(const cls::rbd::SnapshotNamespace &, const std::string &, Context*));

  MOCK_METHOD1(prepare_lock, void(Context*));
  MOCK_METHOD0(handle_prepare_lock_complete, void());
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IMAGE_STATE_H
