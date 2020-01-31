// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_REPLAYER_H
#define RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_REPLAYER_H

#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "common/ceph_mutex.h"
#include <string>
#include <type_traits>

namespace librbd {

struct ImageCtx;
namespace snapshot { template <typename I> class Replay; }

} // namespace librbd

namespace rbd {
namespace mirror {

template <typename> struct Threads;

namespace image_replayer {

struct ReplayerListener;

namespace snapshot {

template <typename> class EventPreprocessor;
template <typename> class ReplayStatusFormatter;
template <typename> class StateBuilder;

template <typename ImageCtxT>
class Replayer : public image_replayer::Replayer {
public:
  static Replayer* create(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      StateBuilder<ImageCtxT>* state_builder,
      ReplayerListener* replayer_listener) {
    return new Replayer(threads, local_mirror_uuid, state_builder,
                        replayer_listener);
  }

  Replayer(
      Threads<ImageCtxT>* threads,
      const std::string& local_mirror_uuid,
      StateBuilder<ImageCtxT>* state_builder,
      ReplayerListener* replayer_listener);
  ~Replayer();

  void destroy() override {
    delete this;
  }

  void init(Context* on_finish) override;
  void shut_down(Context* on_finish) override;

  void flush(Context* on_finish) override;

  bool get_replay_status(std::string* description, Context* on_finish) override;

  bool is_replaying() const override {
    std::unique_lock locker{m_lock};
    return (m_state == STATE_REPLAYING);
  }

  bool is_resync_requested() const override {
    std::unique_lock locker(m_lock);
    // TODO
    return false;
  }

  int get_error_code() const override {
    std::unique_lock locker(m_lock);
    // TODO
    return 0;
  }

  std::string get_error_description() const override {
    std::unique_lock locker(m_lock);
    // TODO
    return "";
  }

private:
  // TODO
  /**
   * @verbatim
   *
   *  <init>
   *    |
   *    v
   * <shutdown>
   *
   * @endverbatim
   */

  enum State {
    STATE_INIT,
    STATE_REPLAYING,
    STATE_COMPLETE
  };

  Threads<ImageCtxT>* m_threads;
  std::string m_local_mirror_uuid;
  StateBuilder<ImageCtxT>* m_state_builder;
  ReplayerListener* m_replayer_listener;

  mutable ceph::mutex m_lock;

  State m_state = STATE_INIT;
};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::snapshot::Replayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_REPLAYER_H
