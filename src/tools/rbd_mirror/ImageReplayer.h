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
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "types.h"

class AdminSocketHook;

namespace journal {

class Journaler;
class ReplayHandler;

}

namespace librbd {

class ImageCtx;
namespace journal { template <typename> class Replay; }

}

namespace rbd {
namespace mirror {

struct Threads;

/**
 * Replays changes from a remote cluster for a single image.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageReplayer {
public:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::ReplayEntry ReplayEntry;

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

  ImageReplayer(Threads *threads, RadosRef local, RadosRef remote,
		const std::string &client_id, int64_t local_pool_id,
		int64_t remote_pool_id, const std::string &remote_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  State get_state() { Mutex::Locker l(m_lock); return get_state_(); }
  bool is_stopped() { Mutex::Locker l(m_lock); return is_stopped_(); }
  bool is_running() { Mutex::Locker l(m_lock); return is_running_(); }

  std::string get_name() { Mutex::Locker l(m_lock); return m_name; };

  void start(Context *on_finish = nullptr,
	     const BootstrapParams *bootstrap_params = nullptr);
  void stop(Context *on_finish = nullptr);
  void flush(Context *on_finish = nullptr);

  void print_status(Formatter *f, stringstream *ss);

  virtual void handle_replay_ready();
  virtual void handle_replay_process_ready(int r);
  virtual void handle_replay_complete(int r);

  virtual void handle_replay_committed(ReplayEntry* replay_entry, int r);

  inline int64_t get_remote_pool_id() const {
    return m_remote_pool_id;
  }
  inline const std::string get_remote_image_id() const {
    return m_remote_image_id;
  }
protected:
  /**
   * @verbatim
   *                   (error)
   * <uninitialized> <------------------------ FAIL
   *    |                                       ^
   *    v                                       *
   * <starting>                                 *
   *    |                                       *
   *    v                               (error) *
   * GET_REGISTERED_CLIENT_STATUS * * * * * * * *
   *    |                                       *
   *    | (sync required)                       *
   *    |\-----\                                *
   *    |      |                                *
   *    |      v                        (error) *
   *    |   BOOTSTRAP_IMAGE * * * * * * * * * * *
   *    |      |                                *
   *    |      v                                *
   *    |/-----/                                *
   *    |                                       *
   *    v (no sync required)            (error) *
   * REMOTE_JOURNALER_INIT  * * * * * * * * * * *
   *    |                                       *
   *    v                               (error) *
   * LOCAL_IMAGE_OPEN (skip if not  * * * * * * *
   *    |              needed                   *
   *    v                               (error) *
   * WAIT_FOR_LOCAL_JOURNAL_READY * * * * * * * *
   *    |
   *    v-----------------------------------------------\
   * <replaying> --------------> <flushing_replay>      |
   *    |                           |                   |
   *    v                           v                   |
   * <stopping>                  LOCAL_REPLAY_FLUSH     |
   *    |                           |                   |
   *    v                           v                   |
   * JOURNAL_REPLAY_SHUT_DOWN    FLUSH_COMMIT_POSITION  |
   *    |                           |                   |
   *    v                           \-------------------/
   * LOCAL_IMAGE_CLOSE
   *    |
   *    v
   * <stopped>
   *
   * @endverbatim
   */

  virtual void on_start_get_registered_client_status_start(
    const BootstrapParams *bootstrap_params);
  virtual void on_start_get_registered_client_status_finish(int r,
    const std::set<cls::journal::Client> &registered_clients,
    const BootstrapParams &bootstrap_params);

  void bootstrap(const BootstrapParams &params);
  void handle_bootstrap(int r);

  virtual void on_start_remote_journaler_init_start();
  virtual void on_start_remote_journaler_init_finish(int r);
  virtual void on_start_local_image_open_start();
  virtual void on_start_local_image_open_finish(int r);
  virtual void on_start_wait_for_local_journal_ready_start();
  virtual void on_start_wait_for_local_journal_ready_finish(int r);
  virtual void on_start_fail_start(int r);
  virtual void on_start_fail_finish(int r);
  virtual bool on_start_interrupted();

  virtual void on_stop_journal_replay_shut_down_start();
  virtual void on_stop_journal_replay_shut_down_finish(int r);
  virtual void on_stop_local_image_close_start();
  virtual void on_stop_local_image_close_finish(int r);

  virtual void on_flush_local_replay_flush_start();
  virtual void on_flush_local_replay_flush_finish(int r);
  virtual void on_flush_flush_commit_position_start(int last_r);
  virtual void on_flush_flush_commit_position_finish(int last_r, int r);
  virtual bool on_flush_interrupted();

  void close_local_image(Context *on_finish); // for tests

private:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;

  State get_state_() const { return m_state; }
  bool is_stopped_() const { return m_state == STATE_UNINITIALIZED ||
                                    m_state == STATE_STOPPED; }
  bool is_running_() const { return !is_stopped_() && m_state != STATE_STOPPING; }

  int get_bootstrap_params(BootstrapParams *params);

  void shut_down_journal_replay(bool cancel_ops);

  Threads *m_threads;
  RadosRef m_local, m_remote;
  std::string m_client_id;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_remote_image_id, m_local_image_id;
  std::string m_name;
  Mutex m_lock;
  State m_state;
  std::string m_local_pool_name, m_remote_pool_name;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  ImageCtxT *m_local_image_ctx;
  librbd::journal::Replay<ImageCtxT> *m_local_replay;
  Journaler* m_remote_journaler;
  ::journal::ReplayHandler *m_replay_handler;
  Context *m_on_finish;
  AdminSocketHook *m_asok_hook;

  librbd::journal::MirrorPeerClientMeta m_client_meta;
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
