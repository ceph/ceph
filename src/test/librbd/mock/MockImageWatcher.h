// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IMAGE_WATCHER_H
#define CEPH_TEST_LIBRBD_MOCK_IMAGE_WATCHER_H

#include "gmock/gmock.h"

namespace librbd {

struct MockImageWatcher {
  MOCK_CONST_METHOD0(is_lock_owner, bool());
  MOCK_CONST_METHOD1(is_lock_supported, bool(const RWLock &));
  MOCK_METHOD1(assert_header_locked, void (librados::ObjectWriteOperation *));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IMAGE_WATCHER_H
