#ifndef CACHE_CONTROLLER_H
#define CACHE_CONTROLLER_H

#include <thread>

#include "common/Formatter.h"
#include "common/admin_socket.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include "include/assert.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"

#include "CacheControllerSocket.hpp"
#include "ObjectCacheStore.h"


using boost::asio::local::stream_protocol;

namespace rbd {
namespace cache {

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

} // namespace rbd
} // namespace cache

#endif
