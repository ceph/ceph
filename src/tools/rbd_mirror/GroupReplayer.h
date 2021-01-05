// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_GROUP_REPLAYER_H
#define CEPH_RBD_MIRROR_GROUP_REPLAYER_H

#include "common/ceph_mutex.h"
#include "include/rados/librados.hpp"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_replayer/Types.h"
#include <boost/optional.hpp>
#include <string>

class AdminSocketHook;

namespace journal { struct CacheManagerHandler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct InstanceWatcher;
template <typename> struct MirrorStatusUpdater;
struct PoolMetaCache;
template <typename> struct Threads;

/**
 * Replays changes from a remote cluster for a single group.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class GroupReplayer {
public:
  static GroupReplayer *create(
      librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
      const std::string &global_group_id, Threads<ImageCtxT> *threads,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      MirrorStatusUpdater<ImageCtxT>* local_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache* pool_meta_cache) {
    return new GroupReplayer(local_io_ctx, local_mirror_uuid, global_group_id,
                             threads, instance_watcher, local_status_updater,
                             cache_manager_handler, pool_meta_cache);
  }
  void destroy() {
    delete this;
  }

  GroupReplayer(librados::IoCtx &local_io_ctx,
                const std::string &local_mirror_uuid,
                const std::string &global_group_id,
                Threads<ImageCtxT> *threads,
                InstanceWatcher<ImageCtxT> *instance_watcher,
                MirrorStatusUpdater<ImageCtxT>* local_status_updater,
                journal::CacheManagerHandler *cache_manager_handler,
                PoolMetaCache* pool_meta_cache);
  virtual ~GroupReplayer();
  GroupReplayer(const GroupReplayer&) = delete;
  GroupReplayer& operator=(const GroupReplayer&) = delete;

  bool is_stopped() { std::lock_guard l{m_lock}; return is_stopped_(); }
  bool is_running() { std::lock_guard l{m_lock}; return is_running_(); }
  bool is_replaying() { std::lock_guard l{m_lock}; return is_replaying_(); }

  std::string get_name() { std::lock_guard l{m_lock}; return m_group_spec; };
  void set_state_description(int r, const std::string &desc);

  // TODO temporary until policy handles release of group replayers
  inline bool is_finished() const {
    std::lock_guard locker{m_lock};
    return m_finished;
  }
  inline void set_finished(bool finished) {
    std::lock_guard locker{m_lock};
    m_finished = finished;
  }

  inline bool is_blocklisted() const {
    std::lock_guard locker{m_lock};
    return (m_last_r == -EBLOCKLISTED);
  }

  image_replayer::HealthState get_health_state() const;

  void add_peer(const Peer<ImageCtxT>& peer);

  inline int64_t get_local_pool_id() const {
    return m_local_io_ctx.get_id();
  }
  inline std::string get_namespace() const {
    return m_local_io_ctx.get_namespace();
  }
  inline const std::string& get_global_group_id() const {
    return m_global_group_id;
  }

  void start(Context *on_finish = nullptr, bool manual = false,
             bool restart = false);
  void stop(Context *on_finish = nullptr, bool manual = false,
            bool restart = false);
  void restart(Context *on_finish = nullptr);
  void flush();

  void print_status(Formatter *f);

  template <typename>
  friend std::ostream &operator<<(std::ostream &os,
                                  const GroupReplayer &replayer);

  /**
   * @verbatim
   *                   (error)
   * <uninitialized> <------------------------------------ FAIL
   *    |                                                   ^
   *    v                                                   *
   * <starting>                                             *
   *    |                                                   *
   *    v                                           (error) *
   * <stopped>
   *
   * @endverbatim
   */

private:
  typedef std::set<Peer<ImageCtxT>> Peers;

  enum State {
    STATE_UNKNOWN,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  librados::IoCtx &m_local_io_ctx;
  std::string m_local_mirror_uuid;
  std::string m_global_group_id;
  Threads<ImageCtxT> *m_threads;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  MirrorStatusUpdater<ImageCtxT>* m_local_status_updater;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;

  std::string m_local_group_name;
  std::string m_group_spec;
  Peers m_peers;

  mutable ceph::mutex m_lock;
  State m_state = STATE_STOPPED;
  std::string m_state_desc;
  int m_last_r = 0;

  Context *m_on_start_finish = nullptr;
  Context *m_on_stop_finish = nullptr;
  bool m_stop_requested = false;
  bool m_restart_requested = false;
  bool m_manual_stop = false;
  bool m_finished = false;

  AdminSocketHook *m_asok_hook = nullptr;

  bool is_stopped_() const {
    return m_state == STATE_STOPPED;
  }
  bool is_running_() const {
    return !is_stopped_() && m_state != STATE_STOPPING && !m_stop_requested;
  }
  bool is_replaying_() const {
    return (m_state == STATE_REPLAYING);
  }

  void register_admin_socket_hook();
  void unregister_admin_socket_hook();
  void reregister_admin_socket_hook();
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::GroupReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_GROUP_REPLAYER_H
