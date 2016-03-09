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
#include "types.h"

class ContextWQ;

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
struct Threads;

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
  };

public:
  ImageReplayer(Threads *threads, RadosRef local, RadosRef remote,
		const std::string &client_id, int64_t local_pool_id,
		int64_t remote_pool_id, const std::string &remote_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  State get_state() { return m_state; }

  int start(const BootstrapParams *bootstrap_params = nullptr);
  void stop();

  int flush();

  virtual void handle_replay_ready();
  virtual void handle_replay_process_ready(int r);
  virtual void handle_replay_complete(int r);

  virtual void handle_replay_committed(::journal::ReplayEntry* replay_entry, int r);

private:
  int get_registered_client_status(bool *registered);
  int register_client();
  int get_bootrstap_params(BootstrapParams *params);
  int bootstrap(const BootstrapParams *bootstrap_params);
  int create_local_image(const BootstrapParams &bootstrap_params);
  int get_image_id(librados::IoCtx &ioctx, const std::string &image_name,
		   std::string *image_id);
  int copy();

  void shut_down_journal_replay(bool cancel_ops);

  friend std::ostream &operator<<(std::ostream &os,
				  const ImageReplayer &replayer);
private:
  Threads *m_threads;
  RadosRef m_local, m_remote;
  std::string m_client_id;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_remote_image_id, m_local_image_id;
  std::string m_snap_name;
  Mutex m_lock;
  State m_state;
  std::string m_local_pool_name, m_remote_pool_name;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::Replay<librbd::ImageCtx> *m_local_replay;
  ::journal::Journaler *m_remote_journaler;
  ::journal::ReplayHandler *m_replay_handler;
  ImageReplayerAdminSocketHook *m_asok_hook;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
