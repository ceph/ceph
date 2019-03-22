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
#include "test/librbd/mock/MockReadahead.h"
#include "test/librbd/mock/io/MockImageRequestWQ.h"
#include "test/librbd/mock/io/MockObjectDispatcher.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "common/zipkin_trace.h"
#include "librbd/ImageCtx.h"
#include "gmock/gmock.h"
#include <string>

class MockSafeTimer;

namespace librbd {

namespace cache { class MockImageCache; }
namespace operation {
template <typename> class ResizeRequest;
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
  MOCK_METHOD0(destroy, void());

  MockImageCtx(librbd::ImageCtx &image_ctx)
    : image_ctx(&image_ctx),
      cct(image_ctx.cct),
      perfcounter(image_ctx.perfcounter),
      snap_namespace(image_ctx.snap_namespace),
      snap_name(image_ctx.snap_name),
      snap_id(image_ctx.snap_id),
      snap_exists(image_ctx.snap_exists),
      snapc(image_ctx.snapc),
      snaps(image_ctx.snaps),
      snap_info(image_ctx.snap_info),
      snap_ids(image_ctx.snap_ids),
      old_format(image_ctx.old_format),
      read_only(image_ctx.read_only),
      clone_copy_on_read(image_ctx.clone_copy_on_read),
      lockers(image_ctx.lockers),
      exclusive_locked(image_ctx.exclusive_locked),
      lock_tag(image_ctx.lock_tag),
      owner_lock(image_ctx.owner_lock),
      md_lock(image_ctx.md_lock),
      snap_lock(image_ctx.snap_lock),
      timestamp_lock(image_ctx.timestamp_lock),
      parent_lock(image_ctx.parent_lock),
      object_map_lock(image_ctx.object_map_lock),
      async_ops_lock(image_ctx.async_ops_lock),
      copyup_list_lock(image_ctx.copyup_list_lock),
      order(image_ctx.order),
      size(image_ctx.size),
      features(image_ctx.features),
      flags(image_ctx.flags),
      op_features(image_ctx.op_features),
      operations_disabled(image_ctx.operations_disabled),
      stripe_unit(image_ctx.stripe_unit),
      stripe_count(image_ctx.stripe_count),
      object_prefix(image_ctx.object_prefix),
      header_oid(image_ctx.header_oid),
      id(image_ctx.id),
      name(image_ctx.name),
      parent_md(image_ctx.parent_md),
      format_string(image_ctx.format_string),
      group_spec(image_ctx.group_spec),
      layout(image_ctx.layout),
      io_work_queue(new io::MockImageRequestWQ()),
      io_object_dispatcher(new io::MockObjectDispatcher()),
      op_work_queue(new MockContextWQ()),
      readahead_max_bytes(image_ctx.readahead_max_bytes),
      event_socket(image_ctx.event_socket),
      parent(NULL), operations(new MockOperations()),
      state(new MockImageState()),
      image_watcher(NULL), object_map(NULL),
      exclusive_lock(NULL), journal(NULL),
      trace_endpoint(image_ctx.trace_endpoint),
      sparse_read_threshold_bytes(image_ctx.sparse_read_threshold_bytes),
      discard_granularity_bytes(image_ctx.discard_granularity_bytes),
      mirroring_replay_delay(image_ctx.mirroring_replay_delay),
      non_blocking_aio(image_ctx.non_blocking_aio),
      blkin_trace_all(image_ctx.blkin_trace_all),
      enable_alloc_hint(image_ctx.enable_alloc_hint),
      ignore_migrating(image_ctx.ignore_migrating),
      mtime_update_interval(image_ctx.mtime_update_interval),
      atime_update_interval(image_ctx.atime_update_interval),
      cache(image_ctx.cache),
      config(image_ctx.config)
  {
    md_ctx.dup(image_ctx.md_ctx);
    data_ctx.dup(image_ctx.data_ctx);

    if (image_ctx.image_watcher != NULL) {
      image_watcher = new MockImageWatcher();
    }
  }

  ~MockImageCtx() {
    wait_for_async_requests();
    image_ctx->md_ctx.aio_flush();
    image_ctx->data_ctx.aio_flush();
    image_ctx->op_work_queue->drain();
    delete state;
    delete operations;
    delete image_watcher;
    delete op_work_queue;
    delete io_work_queue;
    delete io_object_dispatcher;
  }

  void wait_for_async_requests() {
    async_ops_lock.Lock();
    if (async_requests.empty()) {
      async_ops_lock.Unlock();
      return;
    }

    C_SaferCond ctx;
    async_requests_waiters.push_back(&ctx);
    async_ops_lock.Unlock();

    ctx.wait();
  }

  MOCK_METHOD0(init_layout, void());

  MOCK_CONST_METHOD1(get_object_name, std::string(uint64_t));
  MOCK_CONST_METHOD0(get_object_size, uint64_t());
  MOCK_CONST_METHOD0(get_current_size, uint64_t());
  MOCK_CONST_METHOD1(get_image_size, uint64_t(librados::snap_t));
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
                                             uint64_t *overlap));
  MOCK_CONST_METHOD2(prune_parent_extents, uint64_t(vector<pair<uint64_t,uint64_t> >& ,
                                                    uint64_t));

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
  MOCK_METHOD1(flush_async_operations, void(Context *));
  MOCK_METHOD1(flush_copyup, void(Context *));

  MOCK_CONST_METHOD1(test_features, bool(uint64_t test_features));
  MOCK_CONST_METHOD2(test_features, bool(uint64_t test_features,
                                         const RWLock &in_snap_lock));

  MOCK_CONST_METHOD1(test_op_features, bool(uint64_t op_features));

  MOCK_METHOD1(cancel_async_requests, void(Context*));

  MOCK_METHOD0(create_exclusive_lock, MockExclusiveLock*());
  MOCK_METHOD1(create_object_map, MockObjectMap*(uint64_t));
  MOCK_METHOD0(create_journal, MockJournal*());

  MOCK_METHOD0(notify_update, void());
  MOCK_METHOD1(notify_update, void(Context *));

  MOCK_CONST_METHOD0(get_exclusive_lock_policy, exclusive_lock::Policy*());
  MOCK_CONST_METHOD0(get_journal_policy, journal::Policy*());
  MOCK_METHOD1(set_journal_policy, void(journal::Policy*));

  MOCK_METHOD2(apply_metadata, int(const std::map<std::string, bufferlist> &,
                                   bool));

  MOCK_CONST_METHOD0(get_stripe_count, uint64_t());
  MOCK_CONST_METHOD0(get_stripe_period, uint64_t());

  MOCK_CONST_METHOD0(is_writeback_cache_enabled, bool());

  static void set_timer_instance(MockSafeTimer *timer, Mutex *timer_lock);
  static void get_timer_instance(CephContext *cct, MockSafeTimer **timer,
                                 Mutex **timer_lock);

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
  std::map<std::pair<cls::rbd::SnapshotNamespace, std::string>, librados::snap_t> snap_ids;

  bool old_format;
  bool read_only;

  bool clone_copy_on_read;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  bool exclusive_locked;
  std::string lock_tag;

  librados::IoCtx md_ctx;
  librados::IoCtx data_ctx;

  RWLock &owner_lock;
  RWLock &md_lock;
  RWLock &snap_lock;
  RWLock &timestamp_lock;
  RWLock &parent_lock;
  RWLock &object_map_lock;
  Mutex &async_ops_lock;
  Mutex &copyup_list_lock;

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

  io::MockImageRequestWQ *io_work_queue;
  io::MockObjectDispatcher *io_object_dispatcher;
  MockContextWQ *op_work_queue;

  cache::MockImageCache *image_cache = nullptr;

  MockReadahead readahead;
  uint64_t readahead_max_bytes;

  EventSocket &event_socket;

  MockImageCtx *parent;
  MockOperations *operations;
  MockImageState *state;

  MockImageWatcher *image_watcher;
  MockObjectMap *object_map;
  MockExclusiveLock *exclusive_lock;
  MockJournal *journal;

  ZTracer::Endpoint trace_endpoint;

  uint64_t sparse_read_threshold_bytes;
  uint32_t discard_granularity_bytes;
  int mirroring_replay_delay;
  bool non_blocking_aio;
  bool blkin_trace_all;
  bool enable_alloc_hint;
  bool ignore_migrating;
  uint64_t mtime_update_interval;
  uint64_t atime_update_interval;
  bool cache;

  ConfigProxy config;
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
