// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_REPLAYER_H
#define CEPH_RBD_MIRROR_POOL_REPLAYER_H

#include "common/Cond.h"
#include "common/ceph_mutex.h"
#include "include/rados/librados.hpp"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"

#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/NamespaceReplayer.h"
#include "tools/rbd_mirror/Throttler.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/leader_watcher/Types.h"
#include "tools/rbd_mirror/service_daemon/Types.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

class AdminSocketHook;

namespace journal { struct CacheManagerHandler; }

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class RemotePoolPoller;
namespace remote_pool_poller { struct Listener; }

struct PoolMetaCache;
template <typename> class ServiceDaemon;
template <typename> struct Threads;


/**
 * Controls mirroring for a single remote cluster.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class PoolReplayer {
public:
  PoolReplayer(Threads<ImageCtxT> *threads,
               ServiceDaemon<ImageCtxT> *service_daemon,
               journal::CacheManagerHandler *cache_manager_handler,
               PoolMetaCache* pool_meta_cache,
	       int64_t local_pool_id, const PeerSpec &peer,
	       const std::vector<const char*> &args);
  ~PoolReplayer();
  PoolReplayer(const PoolReplayer&) = delete;
  PoolReplayer& operator=(const PoolReplayer&) = delete;

  bool is_blacklisted() const;
  bool is_leader() const;
  bool is_running() const;

  void init(const std::string& site_name);
  void shut_down();

  void run();

  void print_status(Formatter *f);
  void start();
  void stop(bool manual);
  void restart();
  void flush();
  void release_leader();
  void reopen_logs();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   *  INIT
   *    |
   *    v
   * <follower> <---------------------\
   *    .                             |
   *    . (leader acquired)           |
   *    v                             |
   *  NOTIFY_NAMESPACE_WATCHERS     NOTIFY_NAMESPACE_WATCHERS
   *    |                             ^
   *    v                             .
   * <leader>                         .
   *    .                             .
   *    . (leader lost / shut down)   .
   *    . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  struct RemotePoolPollerListener;

  int init_rados(const std::string &cluster_name,
                 const std::string &client_name,
                 const std::string &mon_host,
                 const std::string &key,
                 const std::string &description, RadosRef *rados_ref,
                 bool strip_cluster_overrides);

  void update_namespace_replayers();
  int list_mirroring_namespaces(std::set<std::string> *namespaces);

  void namespace_replayer_acquire_leader(const std::string &name,
                                         Context *on_finish);

  void handle_post_acquire_leader(Context *on_finish);
  void handle_pre_release_leader(Context *on_finish);

  void handle_update_leader(const std::string &leader_instance_id);

  void handle_instances_added(const std::vector<std::string> &instance_ids);
  void handle_instances_removed(const std::vector<std::string> &instance_ids);

  // sync version, executed in the caller thread
  template <typename L>
  void with_namespace_replayers(L &&callback) {
    std::lock_guard locker{m_lock};

    if (m_namespace_replayers_locked) {
      ceph_assert(m_on_namespace_replayers_unlocked == nullptr);
      C_SaferCond cond;
      m_on_namespace_replayers_unlocked = &cond;
      m_lock.unlock();
      cond.wait();
      m_lock.lock();
    } else {
      m_namespace_replayers_locked = true;
    }

    ceph_assert(m_namespace_replayers_locked);
    callback(); // may temporary release the lock
    ceph_assert(m_namespace_replayers_locked);

    if (m_on_namespace_replayers_unlocked == nullptr) {
      m_namespace_replayers_locked = false;
      return;
    }

    m_threads->work_queue->queue(m_on_namespace_replayers_unlocked);
    m_on_namespace_replayers_unlocked = nullptr;
  }

  // async version
  template <typename L>
  void with_namespace_replayers(L &&callback, Context *on_finish) {
    std::lock_guard locker{m_lock};

    on_finish = librbd::util::create_async_context_callback(
      m_threads->work_queue, new LambdaContext(
          [this, on_finish](int r) {
            {
              std::lock_guard locker{m_lock};
              ceph_assert(m_namespace_replayers_locked);

              m_namespace_replayers_locked = false;

              if (m_on_namespace_replayers_unlocked != nullptr) {
                m_namespace_replayers_locked = true;
                m_threads->work_queue->queue(m_on_namespace_replayers_unlocked);
                m_on_namespace_replayers_unlocked = nullptr;
              }
            }
            on_finish->complete(r);
          }));

    auto on_lock = new LambdaContext(
        [this, callback, on_finish](int) {
          std::lock_guard locker{m_lock};
          ceph_assert(m_namespace_replayers_locked);

          callback(on_finish);
        });

    if (m_namespace_replayers_locked) {
      ceph_assert(m_on_namespace_replayers_unlocked == nullptr);
      m_on_namespace_replayers_unlocked = on_lock;
      return;
    }

    m_namespace_replayers_locked = true;
    m_threads->work_queue->queue(on_lock);
  }

  void handle_remote_pool_meta_updated(const RemotePoolMeta& remote_pool_meta);

  Threads<ImageCtxT> *m_threads;
  ServiceDaemon<ImageCtxT> *m_service_daemon;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;
  int64_t m_local_pool_id = -1;
  PeerSpec m_peer;
  std::vector<const char*> m_args;

  mutable ceph::mutex m_lock;
  ceph::condition_variable m_cond;
  std::string m_site_name;
  bool m_stopping = false;
  bool m_manual_stop = false;
  bool m_blacklisted = false;

  RadosRef m_local_rados;
  RadosRef m_remote_rados;

  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;

  std::string m_local_mirror_uuid;

  RemotePoolMeta m_remote_pool_meta;
  std::unique_ptr<remote_pool_poller::Listener> m_remote_pool_poller_listener;
  std::unique_ptr<RemotePoolPoller<ImageCtxT>> m_remote_pool_poller;

  std::unique_ptr<NamespaceReplayer<ImageCtxT>> m_default_namespace_replayer;
  std::map<std::string, NamespaceReplayer<ImageCtxT> *> m_namespace_replayers;

  std::string m_asok_hook_name;
  AdminSocketHook *m_asok_hook = nullptr;

  service_daemon::CalloutId m_callout_id = service_daemon::CALLOUT_ID_NONE;

  bool m_leader = false;
  bool m_namespace_replayers_locked = false;
  Context *m_on_namespace_replayers_unlocked = nullptr;

  class PoolReplayerThread : public Thread {
    PoolReplayer *m_pool_replayer;
  public:
    PoolReplayerThread(PoolReplayer *pool_replayer)
      : m_pool_replayer(pool_replayer) {
    }
    void *entry() override {
      m_pool_replayer->run();
      return 0;
    }
  } m_pool_replayer_thread;

  class LeaderListener : public leader_watcher::Listener {
  public:
    LeaderListener(PoolReplayer *pool_replayer)
      : m_pool_replayer(pool_replayer) {
    }

  protected:
    void post_acquire_handler(Context *on_finish) override {
      m_pool_replayer->handle_post_acquire_leader(on_finish);
    }

    void pre_release_handler(Context *on_finish) override {
      m_pool_replayer->handle_pre_release_leader(on_finish);
    }

    void update_leader_handler(
      const std::string &leader_instance_id) override {
      m_pool_replayer->handle_update_leader(leader_instance_id);
    }

    void handle_instances_added(const InstanceIds& instance_ids) override {
      m_pool_replayer->handle_instances_added(instance_ids);
    }

    void handle_instances_removed(const InstanceIds& instance_ids) override {
      m_pool_replayer->handle_instances_removed(instance_ids);
    }

  private:
    PoolReplayer *m_pool_replayer;
  } m_leader_listener;

  std::unique_ptr<LeaderWatcher<ImageCtxT>> m_leader_watcher;
  std::unique_ptr<Throttler<ImageCtxT>> m_image_sync_throttler;
  std::unique_ptr<Throttler<ImageCtxT>> m_image_deletion_throttler;
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::PoolReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_POOL_REPLAYER_H
