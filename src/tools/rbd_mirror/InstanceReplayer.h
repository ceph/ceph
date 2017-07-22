// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_INSTANCE_REPLAYER_H
#define RBD_MIRROR_INSTANCE_REPLAYER_H

#include <map>
#include <sstream>

#include "common/AsyncOpTracker.h"
#include "common/Formatter.h"
#include "common/Mutex.h"
#include "types.h"

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

class ImageDeleter;

template <typename> class ImageReplayer;
template <typename> class InstanceWatcher;
template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceReplayer {
public:
  static InstanceReplayer* create(
      Threads<ImageCtxT> *threads, std::shared_ptr<ImageDeleter> image_deleter,
      RadosRef local_rados, const std::string &local_mirror_uuid,
      int64_t local_pool_id) {
    return new InstanceReplayer(threads, image_deleter, local_rados,
                                local_mirror_uuid, local_pool_id);
  }
  void destroy() {
    delete this;
  }

  InstanceReplayer(Threads<ImageCtxT> *threads,
		   std::shared_ptr<ImageDeleter> image_deleter,
		   RadosRef local_rados, const std::string &local_mirror_uuid,
		   int64_t local_pool_id);
  ~InstanceReplayer();

  int init();
  void shut_down();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  void add_peer(std::string mirror_uuid, librados::IoCtx io_ctx);
  void remove_peer(std::string mirror_uuid);

  void acquire_image(InstanceWatcher<ImageCtxT> *instance_watcher,
                     const std::string &global_image_id,
                     const std::string &peer_mirror_uuid,
                     const std::string &peer_image_id,
                     Context *on_finish);
  void release_image(const std::string &global_image_id,
                     const std::string &peer_mirror_uuid,
                     const std::string &peer_image_id,
                     bool schedule_delete, Context *on_finish);
  void release_all(Context *on_finish);

  void print_status(Formatter *f, stringstream *ss);
  void start();
  void stop();
  void restart();
  void flush();

private:
  /**
   * @verbatim
   *
   * <uninitialized> <-------------------\
   *    | (init)                         |                    (repeat for each
   *    v                             STOP_IMAGE_REPLAYER ---\ image replayer)
   * SCHEDULE_IMAGE_STATE_CHECK_TASK     ^         ^         |
   *    |                                |         |         |
   *    v          (shut_down)           |         \---------/
   * <initialized> -----------------> WAIT_FOR_OPS
   *
   * @endverbatim
   */

  struct Peer {
    std::string mirror_uuid;
    librados::IoCtx io_ctx;

    Peer() {
    }

    Peer(const std::string &mirror_uuid) : mirror_uuid(mirror_uuid) {
    }

    Peer(const std::string &mirror_uuid, librados::IoCtx &io_ctx)
      : mirror_uuid(mirror_uuid), io_ctx(io_ctx) {
    }

    inline bool operator<(const Peer &rhs) const {
      return mirror_uuid < rhs.mirror_uuid;
    }
    inline bool operator==(const Peer &rhs) const {
      return mirror_uuid == rhs.mirror_uuid;
    }
  };

  typedef std::set<Peer> Peers;

  Threads<ImageCtxT> *m_threads;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  RadosRef m_local_rados;
  std::string m_local_mirror_uuid;
  int64_t m_local_pool_id;

  Mutex m_lock;
  AsyncOpTracker m_async_op_tracker;
  std::map<std::string, ImageReplayer<ImageCtxT> *> m_image_replayers;
  Peers m_peers;
  Context *m_image_state_check_task = nullptr;
  Context *m_on_shut_down = nullptr;
  bool m_manual_stop = false;

  void wait_for_ops();
  void handle_wait_for_ops(int r);

  void start_image_replayer(ImageReplayer<ImageCtxT> *image_replayer);
  void start_image_replayers();

  void stop_image_replayer(ImageReplayer<ImageCtxT> *image_replayer,
                           Context *on_finish);

  void stop_image_replayers();
  void handle_stop_image_replayers(int r);

  void schedule_image_state_check_task();
  void cancel_image_state_check_task();
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::InstanceReplayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_INSTANCE_REPLAYER_H
