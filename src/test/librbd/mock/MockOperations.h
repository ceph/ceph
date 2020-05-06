// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_OPERATIONS_H
#define CEPH_TEST_LIBRBD_MOCK_OPERATIONS_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/int_types.h"
#include "include/rbd/librbd.hpp"
#include "gmock/gmock.h"
#include <string>

class Context;

namespace librbd {

struct MockOperations {
  MOCK_METHOD2(execute_flatten, void(ProgressContext &prog_ctx,
                                     Context *on_finish));
  MOCK_METHOD2(execute_rebuild_object_map, void(ProgressContext &prog_ctx,
                                                Context *on_finish));
  MOCK_METHOD2(execute_rename, void(const std::string &dstname,
                                    Context *on_finish));
  MOCK_METHOD5(execute_resize, void(uint64_t size, bool allow_shrink,
                                    ProgressContext &prog_ctx,
                                    Context *on_finish,
                                    uint64_t journal_op_tid));
  MOCK_METHOD3(snap_create, void(const cls::rbd::SnapshotNamespace &snapshot_namespace,
				 const std::string &snap_name,
                                 Context *on_finish));
  MOCK_METHOD6(execute_snap_create, void(const cls::rbd::SnapshotNamespace &snapshot_namespace,
					 const std::string &snap_name,
                                         Context *on_finish,
                                         uint64_t journal_op_tid,
                                         bool skip_object_map,
                                         ProgressContext &prog_ctx));
  MOCK_METHOD3(snap_remove, void(const cls::rbd::SnapshotNamespace &snap_namespace,
				 const std::string &snap_name,
                                 Context *on_finish));
  MOCK_METHOD3(execute_snap_remove, void(const cls::rbd::SnapshotNamespace &snap_namespace,
					 const std::string &snap_name,
                                         Context *on_finish));
  MOCK_METHOD3(execute_snap_rename, void(uint64_t src_snap_id,
                                         const std::string &snap_name,
                                         Context *on_finish));
  MOCK_METHOD4(execute_snap_rollback, void(const cls::rbd::SnapshotNamespace &snap_namespace,
					   const std::string &snap_name,
                                           ProgressContext &prog_ctx,
                                           Context *on_finish));
  MOCK_METHOD3(execute_snap_protect, void(const cls::rbd::SnapshotNamespace &snap_namespace,
					  const std::string &snap_name,
                                          Context *on_finish));
  MOCK_METHOD3(execute_snap_unprotect, void(const cls::rbd::SnapshotNamespace &snap_namespace,
					    const std::string &snap_name,
                                            Context *on_finish));
  MOCK_METHOD2(execute_snap_set_limit, void(uint64_t limit,
					    Context *on_finish));
  MOCK_METHOD4(execute_update_features, void(uint64_t features, bool enabled,
                                             Context *on_finish,
                                             uint64_t journal_op_tid));
  MOCK_METHOD3(execute_metadata_set, void(const std::string &key,
                                          const std::string &value,
                                          Context *on_finish));
  MOCK_METHOD2(execute_metadata_remove, void(const std::string &key,
                                             Context *on_finish));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_OPERATIONS_H
