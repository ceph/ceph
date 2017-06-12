// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_REPLAYER_H
#define CEPH_RBD_MIRROR_POOL_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"

#include "ClusterWatcher.h"
#include "LeaderWatcher.h"
#include "PoolWatcher.h"
#include "ImageDeleter.h"
#include "types.h"

#include <set>
#include <map>
#include <memory>
#include <atomic>
#include <string>

class AdminSocketHook;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;
template <typename> class InstanceReplayer;
template <typename> class InstanceWatcher;

/**
 * Controls mirroring for a single remote cluster.
 */
class PoolReplayer {
public:
  PoolReplayer(Threads<librbd::ImageCtx> *threads,
	       std::shared_ptr<ImageDeleter> image_deleter,
	       int64_t local_pool_id, const peer_t &peer,
	       const std::vector<const char*> &args);
  ~PoolReplayer();
  PoolReplayer(const PoolReplayer&) = delete;
  PoolReplayer& operator=(const PoolReplayer&) = delete;

  bool is_blacklisted() const;
  bool is_leader() const;

  int init();
  void run();

  void print_status(Formatter *f, stringstream *ss);
  void start();
  void stop(bool manual);
  void restart();
  void flush();
  void release_leader();

private:
  struct PoolWatcherListener : public PoolWatcher<>::Listener {
    PoolReplayer *pool_replayer;
    bool local;

    PoolWatcherListener(PoolReplayer *pool_replayer, bool local)
      : pool_replayer(pool_replayer), local(local) {
    }

    void handle_update(const std::string &mirror_uuid,
                       ImageIds &&added_image_ids,
                       ImageIds &&removed_image_ids) override {
      pool_replayer->handle_update((local ? "" : mirror_uuid),
				   std::move(added_image_ids),
                                   std::move(removed_image_ids));
    }
  };

  void handle_update(const std::string &mirror_uuid,
                     ImageIds &&added_image_ids,
                     ImageIds &&removed_image_ids);

  int init_rados(const std::string &cluster_name,
                 const std::string &client_name,
                 const std::string &description, RadosRef *rados_ref);

  void handle_post_acquire_leader(Context *on_finish);
  void handle_pre_release_leader(Context *on_finish);

  void init_local_pool_watcher(Context *on_finish);
  void handle_init_local_pool_watcher(int r, Context *on_finish);

  void init_remote_pool_watcher(Context *on_finish);

  void shut_down_pool_watchers(Context *on_finish);
  void handle_shut_down_pool_watchers(int r, Context *on_finish);

  void wait_for_update_ops(Context *on_finish);
  void handle_wait_for_update_ops(int r, Context *on_finish);

  void handle_update_leader(const std::string &leader_instance_id);

  Threads<librbd::ImageCtx> *m_threads;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  mutable Mutex m_lock;
  Cond m_cond;
  std::atomic<bool> m_stopping = { false };
  bool m_manual_stop = false;
  bool m_blacklisted = false;

  peer_t m_peer;
  std::vector<const char*> m_args;
  RadosRef m_local_rados;
  RadosRef m_remote_rados;

  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;

  int64_t m_local_pool_id = -1;

  PoolWatcherListener m_local_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<> > m_local_pool_watcher;

  PoolWatcherListener m_remote_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<> > m_remote_pool_watcher;

  std::unique_ptr<InstanceReplayer<librbd::ImageCtx>> m_instance_replayer;

  std::string m_asok_hook_name;
  AdminSocketHook *m_asok_hook;

  std::map<std::string, ImageIds> m_initial_mirror_image_ids;

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

  class LeaderListener : public LeaderWatcher<>::Listener {
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

  private:
    PoolReplayer *m_pool_replayer;
  } m_leader_listener;

  std::unique_ptr<LeaderWatcher<> > m_leader_watcher;
  std::unique_ptr<InstanceWatcher<librbd::ImageCtx> > m_instance_watcher;
  AsyncOpTracker m_update_op_tracker;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_POOL_REPLAYER_H
