// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
#define CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H

#include "test/librbd/mock/MockAioImageRequestWQ.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librbd/mock/MockExclusiveLock.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockImageWatcher.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librbd/mock/MockReadahead.h"
#include "common/RWLock.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "gmock/gmock.h"

namespace librbd {

namespace operation {
template <typename> class ResizeRequest;
}

struct MockImageCtx {
  MockImageCtx(librbd::ImageCtx &image_ctx)
    : image_ctx(&image_ctx),
      cct(image_ctx.cct),
      snap_name(image_ctx.snap_name),
      snap_id(image_ctx.snap_id),
      snap_exists(image_ctx.snap_exists),
      snapc(image_ctx.snapc),
      snaps(image_ctx.snaps),
      snap_info(image_ctx.snap_info),
      snap_ids(image_ctx.snap_ids),
      object_cacher(image_ctx.object_cacher),
      old_format(image_ctx.old_format),
      read_only(image_ctx.read_only),
      lockers(image_ctx.lockers),
      exclusive_locked(image_ctx.exclusive_locked),
      lock_tag(image_ctx.lock_tag),
      owner_lock("owner_lock"),
      md_lock("md_lock"),
      cache_lock("cache_lock"),
      snap_lock("snap_lock"),
      parent_lock("parent_lock"),
      object_map_lock("object_map_lock"),
      async_ops_lock("async_ops_lock"),
      order(image_ctx.order),
      size(image_ctx.size),
      features(image_ctx.features),
      flags(image_ctx.flags),
      object_prefix(image_ctx.object_prefix),
      header_oid(image_ctx.header_oid),
      id(image_ctx.id),
      parent_md(image_ctx.parent_md),
      layout(image_ctx.layout),
      aio_work_queue(new MockAioImageRequestWQ()),
      op_work_queue(new MockContextWQ()),
      parent(NULL), operations(new MockOperations()),
      state(new MockImageState()),
      image_watcher(NULL), object_map(NULL),
      exclusive_lock(NULL), journal(NULL),
      concurrent_management_ops(image_ctx.concurrent_management_ops),
      blacklist_on_break_lock(image_ctx.blacklist_on_break_lock),
      blacklist_expire_seconds(image_ctx.blacklist_expire_seconds),
      journal_order(image_ctx.journal_order),
      journal_splay_width(image_ctx.journal_splay_width),
      journal_commit_age(image_ctx.journal_commit_age),
      journal_object_flush_interval(image_ctx.journal_object_flush_interval),
      journal_object_flush_bytes(image_ctx.journal_object_flush_bytes),
      journal_object_flush_age(image_ctx.journal_object_flush_age),
      journal_pool(image_ctx.journal_pool)
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
    delete aio_work_queue;
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
  MOCK_CONST_METHOD0(get_current_size, uint64_t());
  MOCK_CONST_METHOD1(get_image_size, uint64_t(librados::snap_t));
  MOCK_CONST_METHOD1(get_snap_id, librados::snap_t(std::string in_snap_name));
  MOCK_CONST_METHOD1(get_snap_info, const SnapInfo*(librados::snap_t));
  MOCK_CONST_METHOD2(get_parent_spec, int(librados::snap_t in_snap_id,
                                          parent_spec *pspec));

  MOCK_CONST_METHOD2(is_snap_protected, int(librados::snap_t in_snap_id,
                                            bool *is_protected));
  MOCK_CONST_METHOD2(is_snap_unprotected, int(librados::snap_t in_snap_id,
                                              bool *is_unprotected));

  MOCK_METHOD6(add_snap, void(std::string in_snap_name, librados::snap_t id,
                              uint64_t in_size, parent_info parent,
                              uint8_t protection_status, uint64_t flags));
  MOCK_METHOD2(rm_snap, void(std::string in_snap_name, librados::snap_t id));

  MOCK_METHOD1(flush, void(Context *));
  MOCK_METHOD1(flush_copyup, void(Context *));

  MOCK_METHOD1(invalidate_cache, void(Context *));
  MOCK_METHOD1(shut_down_cache, void(Context *));

  MOCK_CONST_METHOD1(test_features, bool(uint64_t test_features));
  MOCK_CONST_METHOD2(test_features, bool(uint64_t test_features,
                                         const RWLock &in_snap_lock));

  MOCK_METHOD1(cancel_async_requests, void(Context*));

  MOCK_METHOD0(create_exclusive_lock, MockExclusiveLock*());
  MOCK_METHOD1(create_object_map, MockObjectMap*(uint64_t));
  MOCK_METHOD0(create_journal, MockJournal*());

  MOCK_METHOD0(notify_update, void());
  MOCK_METHOD1(notify_update, void(Context *));

  ImageCtx *image_ctx;
  CephContext *cct;

  std::string snap_name;
  uint64_t snap_id;
  bool snap_exists;

  ::SnapContext snapc;
  std::vector<librados::snap_t> snaps;
  std::map<librados::snap_t, SnapInfo> snap_info;
  std::map<std::string, librados::snap_t> snap_ids;

  ObjectCacher *object_cacher;

  bool old_format;
  bool read_only;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  bool exclusive_locked;
  std::string lock_tag;

  librados::IoCtx md_ctx;
  librados::IoCtx data_ctx;

  RWLock owner_lock;
  RWLock md_lock;
  Mutex cache_lock;
  RWLock snap_lock;
  RWLock parent_lock;
  RWLock object_map_lock;
  Mutex async_ops_lock;

  uint8_t order;
  uint64_t size;
  uint64_t features;
  uint64_t flags;
  std::string object_prefix;
  std::string header_oid;
  std::string id;
  parent_info parent_md;

  file_layout_t layout;

  xlist<operation::ResizeRequest<MockImageCtx>*> resize_reqs;
  xlist<AsyncRequest<MockImageCtx>*> async_requests;
  std::list<Context*> async_requests_waiters;


  MockAioImageRequestWQ *aio_work_queue;
  MockContextWQ *op_work_queue;

  MockReadahead readahead;

  MockImageCtx *parent;
  MockOperations *operations;
  MockImageState *state;

  MockImageWatcher *image_watcher;
  MockObjectMap *object_map;
  MockExclusiveLock *exclusive_lock;
  MockJournal *journal;

  int concurrent_management_ops;
  bool blacklist_on_break_lock;
  uint32_t blacklist_expire_seconds;
  uint8_t journal_order;
  uint8_t journal_splay_width;
  double journal_commit_age;
  int journal_object_flush_interval;
  uint64_t journal_object_flush_bytes;
  double journal_object_flush_age;
  std::string journal_pool;
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
