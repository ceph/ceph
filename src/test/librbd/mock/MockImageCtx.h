// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
#define CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H

#include "include/rados/librados.hpp"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockImageWatcher.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librbd/mock/MockPluginRegistry.h"
#include "test/librbd/mock/MockReadahead.h"
#include "test/librbd/mock/io/MockImageDispatcher.h"
#include "test/librbd/mock/io/MockObjectDispatcher.h"
#include "common/WorkQueue.h"
#include "common/zipkin_trace.h"
#include "librbd/ImageCtx.h"
#include "gmock/gmock.h"
#include <string>

class MockSafeTimer;

namespace librbd {

namespace operation {
template <typename> class ResizeRequest;
}


namespace crypto {
  class MockEncryptionFormat;
}

struct MockImageCtx {
  static MockImageCtx *s_instance;
  static MockImageCtx *create(const std::string &image_name,
                              const std::string &image_id,
                              const char *snap, librados::IoCtx& p,
                              bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MockImageCtx(librbd::ImageCtx &image_ctx);
  virtual ~MockImageCtx();

  void wait_for_async_ops();
  void wait_for_async_requests() {
    async_ops_lock.lock();
    if (async_requests.empty()) {
      async_ops_lock.unlock();
      return;
    }

    C_SaferCond ctx;
    async_requests_waiters.push_back(&ctx);
    async_ops_lock.unlock();

    ctx.wait();
  }

  MOCK_METHOD1(init_layout, void(int64_t));

  MOCK_CONST_METHOD1(get_object_name, std::string(uint64_t));
  MOCK_CONST_METHOD0(get_object_size, uint64_t());
  MOCK_CONST_METHOD0(get_current_size, uint64_t());
  MOCK_CONST_METHOD1(get_image_size, uint64_t(librados::snap_t));
  MOCK_CONST_METHOD1(get_area_size, uint64_t(io::ImageArea));
  MOCK_CONST_METHOD1(get_object_count, uint64_t(librados::snap_t));
  MOCK_CONST_METHOD1(get_read_flags, int(librados::snap_t));
  MOCK_CONST_METHOD2(get_flags, int(librados::snap_t in_snap_id,
                     uint64_t *flags));
  MOCK_CONST_METHOD2(get_snap_id,
		     librados::snap_t(cls::rbd::SnapshotNamespace snap_namespace,
				      std::string in_snap_name));
  MOCK_CONST_METHOD1(get_snap_info, const SnapInfo*(librados::snap_t));
  MOCK_CONST_METHOD2(get_snap_name, int(librados::snap_t, std::string *));
  MOCK_CONST_METHOD2(get_snap_namespace, int(librados::snap_t,
					     cls::rbd::SnapshotNamespace *out_snap_namespace));
  MOCK_CONST_METHOD2(get_parent_spec, int(librados::snap_t in_snap_id,
                                          cls::rbd::ParentImageSpec *pspec));
  MOCK_CONST_METHOD1(get_parent_info, const ParentImageInfo*(librados::snap_t));
  MOCK_CONST_METHOD2(get_parent_overlap, int(librados::snap_t in_snap_id,
                                             uint64_t *raw_overlap));
  MOCK_CONST_METHOD2(reduce_parent_overlap,
                     std::pair<uint64_t, io::ImageArea>(uint64_t, bool));
  MOCK_CONST_METHOD4(prune_parent_extents,
                     uint64_t(std::vector<std::pair<uint64_t, uint64_t>>&,
                              io::ImageArea, uint64_t, bool));

  MOCK_CONST_METHOD2(is_snap_protected, int(librados::snap_t in_snap_id,
                                            bool *is_protected));
  MOCK_CONST_METHOD2(is_snap_unprotected, int(librados::snap_t in_snap_id,
                                              bool *is_unprotected));

  MOCK_CONST_METHOD0(get_create_timestamp, utime_t());
  MOCK_CONST_METHOD0(get_access_timestamp, utime_t());
  MOCK_CONST_METHOD0(get_modify_timestamp, utime_t());

  MOCK_METHOD1(set_access_timestamp, void(const utime_t at));
  MOCK_METHOD1(set_modify_timestamp, void(const utime_t at));

  MOCK_METHOD8(add_snap, void(cls::rbd::SnapshotNamespace in_snap_namespace,
			      std::string in_snap_name,
			      librados::snap_t id,
			      uint64_t in_size, const ParentImageInfo &parent,
			      uint8_t protection_status, uint64_t flags, utime_t timestamp));
  MOCK_METHOD3(rm_snap, void(cls::rbd::SnapshotNamespace in_snap_namespace,
			     std::string in_snap_name,
			     librados::snap_t id));

  MOCK_METHOD0(user_flushed, void());
  MOCK_METHOD1(flush_copyup, void(Context *));

  MOCK_CONST_METHOD1(test_features, bool(uint64_t test_features));
  MOCK_CONST_METHOD2(test_features, bool(uint64_t test_features,
                                         const ceph::shared_mutex &in_image_lock));

  MOCK_CONST_METHOD1(test_op_features, bool(uint64_t op_features));

  MOCK_METHOD1(cancel_async_requests, void(Context*));

  MOCK_METHOD0(create_exclusive_lock, MockExclusiveLock*());
  MOCK_METHOD1(create_object_map, MockObjectMap*(uint64_t));
  MOCK_METHOD0(create_journal, MockJournal*());

  MOCK_METHOD0(notify_update, void());
  MOCK_METHOD1(notify_update, void(Context *));

  MOCK_CONST_METHOD0(get_exclusive_lock_policy, exclusive_lock::Policy*());
  MOCK_METHOD1(set_exclusive_lock_policy, void(exclusive_lock::Policy*));
  MOCK_CONST_METHOD0(get_journal_policy, journal::Policy*());
  MOCK_METHOD1(set_journal_policy, void(journal::Policy*));

  MOCK_METHOD2(apply_metadata, int(const std::map<std::string, bufferlist> &,
                                   bool));

  MOCK_CONST_METHOD0(get_stripe_count, uint64_t());
  MOCK_CONST_METHOD0(get_stripe_period, uint64_t());

  MOCK_METHOD0(rebuild_data_io_context, void());
  IOContext get_data_io_context();
  IOContext duplicate_data_io_context();
  uint64_t get_data_offset() const;

  static void set_timer_instance(MockSafeTimer *timer, ceph::mutex *timer_lock);
  static void get_timer_instance(CephContext *cct, MockSafeTimer **timer,
                                 ceph::mutex **timer_lock);

  ImageCtx *image_ctx;
  CephContext *cct;
  PerfCounters *perfcounter;

  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;
  uint64_t snap_id;
  bool snap_exists;

  ::SnapContext snapc;
  std::vector<librados::snap_t> snaps;
  std::map<librados::snap_t, SnapInfo> snap_info;
  std::map<ImageCtx::SnapKey, librados::snap_t, ImageCtx::SnapKeyComparator> snap_ids;

  bool old_format;
  bool read_only;
  uint32_t read_only_flags;
  uint32_t read_only_mask;

  bool clone_copy_on_read;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  bool exclusive_locked;
  std::string lock_tag;

  std::shared_ptr<AsioEngine> asio_engine;
  neorados::RADOS& rados_api;

  librados::IoCtx md_ctx;
  librados::IoCtx data_ctx;

  ceph::shared_mutex &owner_lock;
  ceph::shared_mutex &image_lock;
  ceph::shared_mutex &timestamp_lock;
  ceph::mutex &async_ops_lock;
  ceph::mutex &copyup_list_lock;

  uint8_t order;
  uint64_t size;
  uint64_t features;
  uint64_t flags;
  uint64_t op_features;
  bool operations_disabled;
  uint64_t stripe_unit;
  uint64_t stripe_count;
  std::string object_prefix;
  std::string header_oid;
  std::string id;
  std::string name;
  ParentImageInfo parent_md;
  MigrationInfo migration_info;
  char *format_string;
  cls::rbd::GroupSpec group_spec;

  file_layout_t layout;

  xlist<operation::ResizeRequest<MockImageCtx>*> resize_reqs;
  xlist<AsyncRequest<MockImageCtx>*> async_requests;
  std::list<Context*> async_requests_waiters;

  std::map<uint64_t, io::CopyupRequest<MockImageCtx>*> copyup_list;

  io::MockImageDispatcher *io_image_dispatcher;
  io::MockObjectDispatcher *io_object_dispatcher;
  MockContextWQ *op_work_queue;

  MockPluginRegistry* plugin_registry;

  MockReadahead readahead;
  uint64_t readahead_max_bytes;

  EventSocket &event_socket;

  MockImageCtx *child = nullptr;
  MockImageCtx *parent;
  MockOperations *operations;
  MockImageState *state;

  MockImageWatcher *image_watcher;
  MockObjectMap *object_map;
  MockExclusiveLock *exclusive_lock;
  MockJournal *journal;

  ZTracer::Endpoint trace_endpoint;

  std::unique_ptr<crypto::MockEncryptionFormat> encryption_format;

  uint64_t sparse_read_threshold_bytes;
  uint32_t discard_granularity_bytes;
  int mirroring_replay_delay;
  bool non_blocking_aio;
  bool blkin_trace_all;
  bool enable_alloc_hint;
  uint32_t alloc_hint_flags;
  uint32_t read_flags;
  bool ignore_migrating;
  bool enable_sparse_copyup;
  uint64_t mtime_update_interval;
  uint64_t atime_update_interval;
  bool cache;

  ConfigProxy config;
  std::set<std::string> config_overrides;
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
