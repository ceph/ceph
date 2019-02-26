// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETE_TRASH_WATCHER_H
#define CEPH_RBD_MIRROR_IMAGE_DELETE_TRASH_WATCHER_H

#include "include/rados/librados.hpp"
#include "common/AsyncOpTracker.h"
#include "common/Mutex.h"
#include "librbd/TrashWatcher.h"
#include <set>
#include <string>

struct Context;
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

namespace image_deleter {

struct TrashListener;

template <typename ImageCtxT = librbd::ImageCtx>
class TrashWatcher : public librbd::TrashWatcher<ImageCtxT> {
public:
  static TrashWatcher* create(librados::IoCtx &io_ctx,
                              Threads<ImageCtxT> *threads,
                              TrashListener& trash_listener) {
    return new TrashWatcher(io_ctx, threads, trash_listener);
  }

  TrashWatcher(librados::IoCtx &io_ctx, Threads<ImageCtxT> *threads,
               TrashListener& trash_listener);
  TrashWatcher(const TrashWatcher&) = delete;
  TrashWatcher& operator=(const TrashWatcher&) = delete;

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

protected:
  void handle_image_added(const std::string &image_id,
                          const cls::rbd::TrashImageSpec& spec) override;

  void handle_image_removed(const std::string &image_id) override;

  void handle_rewatch_complete(int r) override;

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
   * CREATE_TRASH
   *    |
   *    v
   * REGISTER_WATCHER
   *    |
   *    |/--------------------------------\
   *    |                                 |
   *    |/---------\                      |
   *    |          |                      |
   *    v          | (more images)        |
   * TRASH_LIST ---/                      |
   *    |                                 |
   *    |/----------------------------\   |
   *    |                             |   |
   *    v                             |   |
   * <idle> --\                       |   |
   *    |     |                       |   |
   *    |     |\---> IMAGE_ADDED -----/   |
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

  librados::IoCtx m_io_ctx;
  Threads<ImageCtxT> *m_threads;
  TrashListener& m_trash_listener;

  std::string m_last_image_id;
  bufferlist m_out_bl;

  mutable Mutex m_lock;

  Context *m_on_init_finish = nullptr;
  Context *m_timer_ctx = nullptr;

  AsyncOpTracker m_async_op_tracker;
  bool m_trash_list_in_progress = false;
  bool m_deferred_trash_list = false;
  bool m_shutting_down = false;

  void register_watcher();
  void handle_register_watcher(int r);

  void create_trash();
  void handle_create_trash(int r);

  void unregister_watcher(Context* on_finish);
  void handle_unregister_watcher(int r, Context* on_finish);

  void trash_list(bool initial_request);
  void handle_trash_list(int r);

  void schedule_trash_list(double interval);
  void process_trash_list();

  void get_mirror_uuid();
  void handle_get_mirror_uuid(int r);

  void add_image(const std::string& image_id,
                 const cls::rbd::TrashImageSpec& spec);

};

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_deleter::TrashWatcher<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_DELETE_TRASH_WATCHER_H
