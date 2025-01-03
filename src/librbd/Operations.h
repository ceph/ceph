// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATIONS_H
#define CEPH_LIBRBD_OPERATIONS_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/int_types.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/operation/ObjectMapIterate.h"
#include <atomic>
#include <string>
#include <list>
#include <map>
#include <set>
#include <boost/function.hpp>

class Context;

namespace librbd {

class ImageCtx;
class ProgressContext;

enum Operation {
  OPERATION_CHECK_OBJECT_MAP,
  OPERATION_FLATTEN,
  OPERATION_METADATA_UPDATE,
  OPERATION_MIGRATE,
  OPERATION_REBUILD_OBJECT_MAP,
  OPERATION_RENAME,
  OPERATION_RESIZE,
  OPERATION_SNAP_CREATE,
  OPERATION_SNAP_PROTECT,
  OPERATION_SNAP_REMOVE,
  OPERATION_SNAP_RENAME,
  OPERATION_SNAP_ROLLBACK,
  OPERATION_SNAP_UNPROTECT,
  OPERATION_SPARSIFY,
  OPERATION_UPDATE_FEATURES,
};

template <typename ImageCtxT = ImageCtx>
class Operations {
public:
  Operations(ImageCtxT &image_ctx);

  void start_op(enum Operation op, Context *ctx);
  void finish_op(enum Operation op, int r);

  int flatten(ProgressContext &prog_ctx);
  void execute_flatten(ProgressContext &prog_ctx, Context *on_finish);

  int rebuild_object_map(ProgressContext &prog_ctx);
  void execute_rebuild_object_map(ProgressContext &prog_ctx,
                                  Context *on_finish);

  int check_object_map(ProgressContext &prog_ctx);
  void check_object_map(ProgressContext &prog_ctx, Context *on_finish);

  void object_map_iterate(ProgressContext &prog_ctx,
			  operation::ObjectIterateWork<ImageCtxT> handle_mismatch,
			  Context* on_finish);

  int rename(const char *dstname);
  void execute_rename(const std::string &dest_name, Context *on_finish);

  int resize(uint64_t size, bool allow_shrink, ProgressContext& prog_ctx);
  void execute_resize(uint64_t size, bool allow_shrink, ProgressContext &prog_ctx,
                      Context *on_finish, uint64_t journal_op_tid);

  int snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
		  const std::string& snap_name, uint64_t flags,
                  ProgressContext& prog_ctx);
  void snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
		   const std::string& snap_name, uint64_t flags,
                   ProgressContext& prog_ctx, Context *on_finish);
  void execute_snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
			   const std::string &snap_name, Context *on_finish,
                           uint64_t journal_op_tid, uint64_t flags,
                           ProgressContext &prog_ctx);

  int snap_rollback(const cls::rbd::SnapshotNamespace& snap_namespace,
		    const std::string& snap_name,
		    ProgressContext& prog_ctx);
  void execute_snap_rollback(const cls::rbd::SnapshotNamespace& snap_namespace,
			     const std::string &snap_name,
                             ProgressContext& prog_ctx, Context *on_finish);

  int snap_remove(const cls::rbd::SnapshotNamespace& snap_namespace,
		  const std::string& snap_name);
  void snap_remove(const cls::rbd::SnapshotNamespace& snap_namespace,
		   const std::string& snap_name,
		   Context *on_finish);
  void execute_snap_remove(const cls::rbd::SnapshotNamespace& snap_namespace,
			   const std::string &snap_name,
			   Context *on_finish);

  int snap_rename(const char *srcname, const char *dstname);
  void execute_snap_rename(const uint64_t src_snap_id,
                           const std::string &dest_snap_name,
                           Context *on_finish);

  int snap_protect(const cls::rbd::SnapshotNamespace& snap_namespace,
		   const std::string& snap_name);
  void execute_snap_protect(const cls::rbd::SnapshotNamespace& snap_namespace,
			    const std::string &snap_name,
			    Context *on_finish);

  int snap_unprotect(const cls::rbd::SnapshotNamespace& snap_namespace,
		     const std::string& snap_name);
  void execute_snap_unprotect(const cls::rbd::SnapshotNamespace& snap_namespace,
			      const std::string &snap_name,
			      Context *on_finish);

  int snap_set_limit(uint64_t limit);
  void execute_snap_set_limit(uint64_t limit, Context *on_finish);

  int update_features(uint64_t features, bool enabled);
  void execute_update_features(uint64_t features, bool enabled,
                               Context *on_finish, uint64_t journal_op_tid);

  int metadata_set(const std::string &key, const std::string &value);
  void execute_metadata_set(const std::string &key, const std::string &value,
                            Context *on_finish);

  int metadata_remove(const std::string &key);
  void execute_metadata_remove(const std::string &key, Context *on_finish);

  int migrate(ProgressContext &prog_ctx);
  void execute_migrate(ProgressContext &prog_ctx, Context *on_finish);

  int sparsify(size_t sparse_size, ProgressContext &prog_ctx);
  void execute_sparsify(size_t sparse_size, ProgressContext &prog_ctx,
                        Context *on_finish);

  int prepare_image_update(exclusive_lock::OperationRequestType request_type,
                           bool request_lock);

private:
  ImageCtxT &m_image_ctx;

  mutable ceph::mutex m_queue_lock;
  std::set<Operation> m_in_flight_ops;
  std::map<Operation, std::list<Context *>> m_queued_ops;

  int invoke_async_request(Operation op,
                           exclusive_lock::OperationRequestType request_type,
                           bool permit_snapshot,
                           const boost::function<void(Context*)>& local,
                           const boost::function<void(Context*)>& remote);
};

} // namespace librbd

extern template class librbd::Operations<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OPERATIONS_H
