// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_GROUP_REPLAYER_H
#define CEPH_RBD_MIRROR_GROUP_REPLAYER_H

#include "cls/rbd/cls_rbd_types.h"
#include "common/ceph_mutex.h"
#include "include/rados/librados.hpp"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/group_replayer/Replayer.h"
#include "tools/rbd_mirror/image_replayer/Types.h"
#include <boost/optional.hpp>
#include <string>
#include <list>

class AdminSocketHook;

namespace journal { struct CacheManagerHandler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class ImageReplayer;
template <typename> struct InstanceWatcher;
template <typename> struct MirrorStatusUpdater;
struct PoolMetaCache;
template <typename> struct Threads;

namespace group_replayer {
  template <typename> class BootstrapRequest;
}

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

  bool is_stopped() const {
    std::lock_guard l{m_lock};
    return is_stopped_();
  }
  bool is_running() const {
    std::lock_guard l{m_lock};
    return is_running_();
  }
  bool is_replaying() const {
    std::lock_guard l{m_lock};
    return is_replaying_();
  }

  std::string get_name() const {
    std::lock_guard l{m_lock};
    return m_local_group_name;
  }
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

  bool needs_restart() const;
  void sync_group_names();

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
  inline const std::string& get_local_group_id() const {
    return m_local_group_id;
  }

  void start(Context *on_finish = nullptr, bool manual = false,
             bool restart = false, bool resync = false);
  void stop(Context *on_finish = nullptr, bool manual = false,
            bool restart = false);
  void restart(Context *on_finish = nullptr, bool resync = false);
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
   * BOOTSTRAP_GROUP  * * * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * START_IMAGE_REPLAYERS  * * * * * * * * * * * * * * * * *
   *    |
   *    v
   * REPLAYING
   *    |
   *    v
   * STOP_IMAGE_REPLAYERS
   *    |
   *    v
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

  struct Listener : public GroupCtx::Listener {
    GroupReplayer *group_replayer;

    Listener(GroupReplayer *group_replayer) : group_replayer(group_replayer) {
    }

    void stop() {
      Context *ctx = new LambdaContext(
        [this](int r) {
          return;
        });
      group_replayer->stop(ctx, false);
    }

    void notify_group_snap_image_complete(
        int64_t local_pool_id,
        const std::string &local_image_id,
        const std::string &remote_group_snap_id,
        uint64_t local_snap_id) override {
      group_replayer->m_replayer->notify_group_snap_image_complete(
          local_pool_id, local_image_id, remote_group_snap_id, local_snap_id);
    }
  };

  librados::IoCtx &m_local_io_ctx;
  std::string m_local_mirror_uuid;
  std::string m_global_group_id;
  Threads<ImageCtxT> *m_threads;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  MirrorStatusUpdater<ImageCtxT> *m_local_status_updater;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;

  std::string m_local_group_name;
  std::string m_group_spec;
  GroupCtx m_local_group_ctx;
  Peers m_peers;
  Peer<ImageCtxT> m_remote_group_peer;
  std::string m_local_group_id;
  std::string m_remote_group_id;

  mutable ceph::mutex m_lock;
  State m_state = STATE_STOPPED;
  std::string m_state_desc;
  int m_last_r = 0;

  Context *m_on_start_finish = nullptr;
  Context *m_on_stop_finish = nullptr;
  bool m_stop_requested = false;
  bool m_resync_requested = false;
  bool m_restart_requested = false;
  bool m_manual_stop = false;
  bool m_finished = false;

  AdminSocketHook *m_asok_hook = nullptr;

  group_replayer::BootstrapRequest<ImageCtxT> *m_bootstrap_request = nullptr;
  group_replayer::Replayer<ImageCtxT> *m_replayer = nullptr;
  std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> m_image_replayers;

  Listener m_listener = {this};
  std::map<std::pair<int64_t, std::string>, ImageReplayer<ImageCtxT> *> m_image_replayer_index;
  std::map<std::string, cls::rbd::GroupSnapshot> m_local_group_snaps;
  std::map<std::string, cls::rbd::GroupSnapshot> m_remote_group_snaps;
  std::vector<cls::rbd::GroupSnapshot> remote_group_snaps;
  std::map<std::string, int> m_get_remote_group_snap_ret_vals;
  std::map<std::string, std::map<ImageReplayer<ImageCtxT> *, Context *>> m_create_snap_requests;
  std::set<std::string> m_pending_snap_create;

  static std::string state_to_string(const State &state) {
    switch (state) {
    case STATE_STARTING:
      return "Starting";
    case STATE_REPLAYING:
      return "Replaying";
    case STATE_STOPPING:
      return "Stopping";
    case STATE_STOPPED:
      return "Stopped";
    default:
      return "Unknown (" + stringify(static_cast<uint32_t>(state)) + ")";
    }
  }

  bool is_stopped_() const {
    return m_state == STATE_STOPPED;
  }
  bool is_running_() const {
    return !is_stopped_() && m_state != STATE_STOPPING && !m_stop_requested;
  }
  bool is_replaying_() const {
    return (m_state == STATE_REPLAYING);
  }

  void bootstrap_group();
  void handle_bootstrap_group(int r);

  void create_group_replayer(Context *on_finish);
  void handle_create_group_replayer(int r, Context *on_finish);

  void start_image_replayers();
  void handle_start_image_replayers(int r);

  bool finish_start_if_interrupted();
  bool finish_start_if_interrupted(ceph::mutex &lock);
  void finish_start(int r, const std::string &desc);

  void stop_group_replayer(Context *on_finish);
  void handle_stop_group_replayer(int r, Context *on_finish);

  void stop_image_replayers();
  void handle_stop_image_replayers(int r);

  void register_admin_socket_hook();
  void unregister_admin_socket_hook();
  void reregister_admin_socket_hook();

  void set_mirror_group_status_update(cls::rbd::MirrorGroupStatusState state,
                                      const std::string &desc);
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::GroupReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_GROUP_REPLAYER_H
