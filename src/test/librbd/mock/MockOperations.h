// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_OPERATIONS_H
#define CEPH_TEST_LIBRBD_MOCK_OPERATIONS_H

#include "include/int_types.h"
#include "gmock/gmock.h"

class Context;

namespace librbd {

struct MockOperations {
  MOCK_METHOD2(flatten, void(ProgressContext &prog_ctx, Context *on_finish));
  MOCK_METHOD2(rebuild_object_map, void(ProgressContext &prog_ctx,
                                        Context *on_finish));
  MOCK_METHOD2(rename, void(const char *dstname, Context *on_finish));
  MOCK_METHOD4(resize, void(uint64_t size, ProgressContext &prog_ctx,
                            Context *on_finish, uint64_t journal_op_tid));
  MOCK_METHOD3(snap_create, void(const char *snap_name, Context *on_finish,
                                 uint64_t journal_op_tid));
  MOCK_METHOD2(snap_remove, void(const char *snap_name, Context *on_finish));
  MOCK_METHOD3(snap_rename, void(uint64_t src_snap_id, const char *snap_name,
                                 Context *on_finish));
  MOCK_METHOD3(snap_rollback, void(const char *snap_name,
                                   ProgressContext &prog_ctx,
                                   Context *on_finish));
  MOCK_METHOD2(snap_protect, void(const char *snap_name, Context *on_finish));
  MOCK_METHOD2(snap_unprotect, void(const char *snap_name, Context *on_finish));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_OPERATIONS_H
