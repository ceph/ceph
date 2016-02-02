// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_READAHEAD_H
#define CEPH_TEST_LIBRBD_MOCK_READAHEAD_H

#include "include/int_types.h"
#include "gmock/gmock.h"

class Context;

namespace librbd {

struct MockReadahead {
  MOCK_METHOD1(set_max_readahead_size, void(uint64_t));
  MOCK_METHOD1(wait_for_pending, void(Context *));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_READAHEAD_H
