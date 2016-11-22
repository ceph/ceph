// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_WATCHER_H
#define CEPH_RBD_MIRROR_POOL_WATCHER_H

#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/AsyncOpTracker.h"
#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/rados/librados.hpp"
#include "types.h"
#include <list>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <boost/optional.hpp>
#include "include/assert.h"

namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

/**
 * Keeps track of images that have mirroring enabled within all
 * pools.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class PoolWatcher {
public:
  struct Listener {
    virtual ~Listener() {
    }

    virtual void handle_update(const ImageIds &added_image_ids,
                               const ImageIds &removed_image_ids) = 0;
  };

  PoolWatcher(Threads<ImageCtxT> *threads, librados::IoCtx &remote_io_ctx,
              Listener &listener);
  ~PoolWatcher();
  PoolWatcher(const PoolWatcher&) = delete;
  PoolWatcher& operator=(const PoolWatcher&) = delete;

  bool is_blacklisted() const;

  void init(Context *on_finish = nullptr);
  void shut_down(Context *on_finish);

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
   * REGISTER_WATCHER
   *    |
   *    |/--------------------------------\
   *    |                                 |
   *    v                                 |
   * REFRESH_IMAGES                       |
   *    |                                 |
   *    |/----------------------------\   |
   *    |                             |   |
   *    v                             |   |
   * NOTIFY_LISTENER                  |   |
   *    |                             |   |
   *    v                             |   |
   *  IDLE ---\                       |   |
   *    |     |                       |   |
   *    |     |\---> IMAGE_UPDATED    |   |
   *    |     |         |             |   |
   *    |     |         v             |   |
   *    |     |      GET_IMAGE_NAME --/   |
   *    |     |                           |
   *    |     \----> WATCH_ERROR ---------/
   *    v
   * SHUT_DOWN
   *    |
   *    v
   * UNREGISTER_WATCHER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  class MirroringWatcher;

  struct UpdatedImage {
    std::string global_image_id;
    std::string remote_image_id;
    bool enabled = true;
    bool invalid = false;

    UpdatedImage(const std::string &global_image_id,
                 const std::string &remote_image_id)
      : global_image_id(global_image_id), remote_image_id(remote_image_id) {
    }
  };

  typedef std::pair<std::string, std::string> GlobalRemoteIds;
  typedef std::list<UpdatedImage> UpdatedImages;
  typedef std::unordered_map<GlobalRemoteIds, typename UpdatedImages::iterator,
                             boost::hash<GlobalRemoteIds> > IdToUpdatedImages;

  struct StrictImageIdCompare {
    bool operator()(const ImageId &lhs, const ImageId &rhs) const {
      if (lhs.global_id != rhs.global_id) {
        return lhs.global_id < rhs.global_id;
      }
      return lhs.id < rhs.id;
    }
  };
  Threads<ImageCtxT> *m_threads;
  librados::IoCtx m_remote_io_ctx;
  Listener &m_listener;

  ImageIds m_refresh_image_ids;
  bufferlist m_out_bl;

  mutable Mutex m_lock;

  Context *m_on_init_finish = nullptr;

  ImageIds m_image_ids;

  bool m_pending_updates = false;
  bool m_notify_listener_in_progress = false;
  ImageIds m_pending_image_ids;
  ImageIds m_pending_added_image_ids;
  ImageIds m_pending_removed_image_ids;

  MirroringWatcher *m_mirroring_watcher;

  Context *m_timer_ctx = nullptr;

  AsyncOpTracker m_async_op_tracker;
  bool m_blacklisted = false;
  bool m_shutting_down = false;
  bool m_image_ids_invalid = true;
  bool m_refresh_in_progress = false;

  UpdatedImages m_updated_images;
  IdToUpdatedImages m_id_to_updated_images;
  bool m_get_name_in_progress = false;

  void register_watcher();
  void handle_register_watcher(int r);
  void unregister_watcher();

  void refresh_images();
  void handle_refresh_images(int r);

  void schedule_refresh_images(double interval);
  void processs_refresh_images();

  void handle_rewatch_complete(int r);
  void handle_image_updated(const std::string &remote_image_id,
                            const std::string &global_image_id,
                            bool enabled);

  void schedule_get_image_name();
  void handle_get_image_name(int r);

  void schedule_listener();
  void notify_listener();

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::PoolWatcher<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_POOL_WATCHER_H
