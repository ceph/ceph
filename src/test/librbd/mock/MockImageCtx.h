// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
#define CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H

#include "test/librbd/mock/MockAioImageRequestWQ.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librbd/mock/MockImageWatcher.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "common/RWLock.h"
#include "librbd/ImageCtx.h"
#include "gmock/gmock.h"

namespace librbd {

struct MockImageCtx {
  MockImageCtx(librbd::ImageCtx &image_ctx)
    : image_ctx(&image_ctx),
      cct(image_ctx.cct),
      snapc(image_ctx.snapc),
      snaps(image_ctx.snaps),
      snap_info(image_ctx.snap_info),
      old_format(image_ctx.old_format),
      read_only(image_ctx.read_only),
      owner_lock("owner_lock"),
      md_lock("md_lock"),
      snap_lock("snap_lock"),
      parent_lock("parent_lock"),
      object_map_lock("object_map_lock"),
      async_ops_lock("async_ops_lock"),
      size(image_ctx.size),
      features(image_ctx.features),
      header_oid(image_ctx.header_oid),
      id(image_ctx.id),
      parent_md(image_ctx.parent_md),
      aio_work_queue(new MockAioImageRequestWQ()),
      op_work_queue(new MockContextWQ()),
      image_watcher(NULL), journal(NULL),
      concurrent_management_ops(image_ctx.concurrent_management_ops)
  {
    md_ctx.dup(image_ctx.md_ctx);
    data_ctx.dup(image_ctx.data_ctx);

    if (image_ctx.image_watcher != NULL) {
      image_watcher = new MockImageWatcher();
    }
  }

  ~MockImageCtx() {
    wait_for_async_requests();
    delete image_watcher;
    delete op_work_queue;
    delete aio_work_queue;
  }

  void wait_for_async_requests() {
    Mutex::Locker async_ops_locker(async_ops_lock);
    while (!async_requests.empty()) {
      async_requests_cond.Wait(async_ops_lock);
    }
  }

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

  ImageCtx *image_ctx;
  CephContext *cct;

  ::SnapContext snapc;
  std::vector<librados::snap_t> snaps;
  std::map<librados::snap_t, SnapInfo> snap_info;


  bool old_format;
  bool read_only;

  librados::IoCtx md_ctx;
  librados::IoCtx data_ctx;

  RWLock owner_lock;
  RWLock md_lock;
  RWLock snap_lock;
  RWLock parent_lock;
  RWLock object_map_lock;
  Mutex async_ops_lock;

  uint64_t size;
  uint64_t features;
  std::string header_oid;
  std::string id;
  parent_info parent_md;

  xlist<AsyncRequest<MockImageCtx>*> async_requests;
  Cond async_requests_cond;

  MockAioImageRequestWQ *aio_work_queue;
  MockContextWQ *op_work_queue;

  MockImageWatcher *image_watcher;
  MockObjectMap object_map;

  MockJournal *journal;

  int concurrent_management_ops;
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IMAGE_CTX_H
