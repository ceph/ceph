// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include <map>
#include <string>
#include <vector>

#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include "types.h"

namespace journal {

class Journaler;
class ReplayHandler;
class ReplayEntry;

}

namespace librbd {

class ImageCtx;

namespace journal {

template <typename> class Replay;

}

}

namespace rbd {
namespace mirror {

class ImageReplayerAdminSocketHook;

/**
 * Replays changes from a remote cluster for a single image.
 */
class ImageReplayer {
public:
  enum State {
    STATE_UNINITIALIZED,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_FLUSHING_REPLAY,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  struct BootstrapParams {
    std::string local_pool_name;
    std::string local_image_name;

    BootstrapParams() {}
    BootstrapParams(const std::string &local_pool_name,
		    const std::string local_image_name) :
      local_pool_name(local_pool_name),
      local_image_name(local_image_name) {}

    bool empty() const {
      return local_pool_name.empty() && local_image_name.empty();
    }
  };

public:
  ImageReplayer(RadosRef local, RadosRef remote, const std::string &client_id,
		int64_t remote_pool_id, const std::string &remote_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  State get_state() const { return m_state; }

  int start(const BootstrapParams *bootstrap_params = nullptr);
  void stop();
  int flush();

  void start(Context *on_finish,
	     const BootstrapParams *bootstrap_params = nullptr);
  void stop(Context *on_finish);

  int bootstrap(const BootstrapParams &bootstrap_params);

  virtual void handle_replay_ready();
  virtual void handle_replay_process_ready(int r);
  virtual void handle_replay_complete(int r);

  virtual void handle_replay_committed(::journal::ReplayEntry* replay_entry, int r);

private:
  /**
   * @verbatim
   *                   (error)
   * <uninitialized> <------------------\
   *    |                               |
   *    v                               |
   * <starting>                         |
   *    |                               |
   *    v                               |
   * GET_REGISTERED_CLIENT_STATUS       |
   *    |                               |
   *    v                               |
   * BOOTSTRAP (skip if not needed)     |
   *    |                               |
   *    v                               |
   * REMOTE_JOURNALER_INIT              |
   *    |                               |
   *    v                               |
   * LOCAL_IMAGE_OPEN                   |
   *    |                               |
   *    v                               |
   * LOCAL_IMAGE_LOCK                   |
   *    |                               |
   *    v                               |
   * WAIT_FOR_LOCAL_JOURNAL_READY       |
   *    |                               |
   *    v   (error)                     |
   * FINISH ----------------------------/
   *    |
   *    v
   * <replaying>
   *    |
   *    v
   * <stopping>
   *    |
   *    v
   * JOURNAL_REPLAY_SHUT_DOWN
   *    |
   *    v
   * LOCAL_IMAGE_CLOSE
   *    |
   *    v
   * <stopped>
   *
   * @endverbatim
   */

  void on_start_get_registered_client_status_start(
    const BootstrapParams *bootstrap_params);
  void on_start_get_registered_client_status_finish(int r,
    const std::set<cls::journal::Client> &registered_clients,
    const BootstrapParams &bootstrap_params);
  void on_start_bootstrap_start(const BootstrapParams &bootstrap_params);
  void on_start_bootstrap_finish(int r);
  void on_start_remote_journaler_init_start();
  void on_start_remote_journaler_init_finish(int r);
  void on_start_local_image_open_start();
  void on_start_local_image_open_finish(int r);
  void on_start_local_image_lock_start();
  void on_start_local_image_lock_finish(int r);
  void on_start_wait_for_local_journal_ready_start();
  void on_start_wait_for_local_journal_ready_finish(int r);
  void on_start_finish(int r);

  void on_stop_journal_replay_shut_down_start();
  void on_stop_journal_replay_shut_down_finish(int r);
  void on_stop_local_image_close_start();
  void on_stop_local_image_close_finish(int r);

  int get_bootrstap_params(BootstrapParams *params);
  int register_client();
  int update_client();
  int unregister_client();
  int create_local_image(const BootstrapParams &bootstrap_params);
  int get_image_id(librados::IoCtx &ioctx, const std::string &image_name,
		   std::string *image_id);
  int copy();

  void shut_down_journal_replay();

  friend std::ostream &operator<<(std::ostream &os,
				  const ImageReplayer &replayer);
private:
  RadosRef m_local, m_remote;
  std::string m_client_id;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_remote_image_id, m_local_image_id;
  std::string m_local_cluster_id, m_snap_name;
  Mutex m_lock;
  State m_state;
  std::string m_local_pool_name, m_remote_pool_name;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::Replay<librbd::ImageCtx> *m_local_replay;
  ::journal::Journaler *m_remote_journaler;
  ::journal::ReplayHandler *m_replay_handler;
  Context *m_on_finish;
  ImageReplayerAdminSocketHook *m_asok_hook;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
