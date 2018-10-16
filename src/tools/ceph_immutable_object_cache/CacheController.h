// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_CONTROLLER_H
#define CEPH_CACHE_CONTROLLER_H

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include "include/ceph_assert.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "CacheServer.h"
#include "ObjectCacheStore.h"

#include <thread>

namespace ceph {
namespace immutable_obj_cache {

class CacheController {
 public:
  CacheController(CephContext *cct, const std::vector<const char*> &args);
  ~CacheController();

  int init();

  int shutdown();

  void handle_signal(int sinnum);

  void run();

  void handle_request(uint64_t sesstion_id, std::string msg);

 private:
  CacheServer *m_cache_server;
  std::vector<const char*> m_args;
  CephContext *m_cct;
  ObjectCacheStore *m_object_cache_store;
  ContextWQ* pcache_op_work_queue;
};

} // namespace immutable_obj_cache
} // namespace ceph

#endif
