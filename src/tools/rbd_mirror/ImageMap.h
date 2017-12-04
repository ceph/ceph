// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_H

#include <vector>

#include "common/Mutex.h"
#include "include/Context.h"
#include "common/AsyncOpTracker.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"

#include "image_map/Policy.h"
#include "image_map/Types.h"

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class ImageMap {
public:
  static ImageMap *create(librados::IoCtx &ioctx, Threads<ImageCtxT> *threads,
                          image_map::Listener &listener) {
    return new ImageMap(ioctx, threads, listener);
  }

  ~ImageMap();

  // init (load) the instance map from disk
  void init(Context *on_finish);

  // shut down map operations
  void shut_down(Context *on_finish);

  // update (add/remove) images
  void update_images(const std::string &peer_uuid,
                     std::set<std::string> &&added_global_image_ids,
                     std::set<std::string> &&removed_global_image_ids);

  // handle notify response from remote peer (r : 0 == success, negative otherwise)
  void handle_peer_ack(const std::string &global_image_id, int r);

  // add/remove instances
  void update_instances_added(const std::vector<std::string> &instances);
  void update_instances_removed(const std::vector<std::string> &instances);

private:
  ImageMap(librados::IoCtx &ioctx, Threads<ImageCtxT> *threads,
           image_map::Listener &listener);

  struct Update {
    std::string global_image_id;
    std::string instance_id;
    utime_t mapped_time;

    Update(const std::string &global_image_id, const std::string &instance_id,
           utime_t mapped_time)
      : global_image_id(global_image_id),
        instance_id(instance_id),
        mapped_time(mapped_time) {
    }
    Update(const std::string &global_image_id, const std::string &instance_id)
      : Update(global_image_id, instance_id, ceph_clock_now()) {
    }
  };
  typedef std::list<Update> Updates;

  // Lock ordering: m_threads->timer_lock, m_lock

  librados::IoCtx &m_ioctx;
  Threads<ImageCtxT> *m_threads;
  image_map::Listener &m_listener;

  std::unique_ptr<image_map::Policy> m_policy; // our mapping policy

  Context *m_timer_task = nullptr;
  Mutex m_lock;
  bool m_shutting_down = false;
  AsyncOpTracker m_async_op_tracker;

  // global_image_id -> registered peers ("" == local, remote otherwise)
  std::map<std::string, std::set<std::string> > m_peer_map;

  Updates m_updates;
  std::set<std::string> m_remove_global_image_ids;
  Updates m_acquire_updates;
  Updates m_release_updates;

  std::set<std::string> m_global_image_ids;

  struct C_LoadMap : Context {
    ImageMap *image_map;
    Context *on_finish;

    std::map<std::string, cls::rbd::MirrorImageMap> image_mapping;

    C_LoadMap(ImageMap *image_map, Context *on_finish)
      : image_map(image_map),
        on_finish(on_finish) {
    }

    void finish(int r) override {
      if (r == 0) {
        image_map->handle_load(image_mapping);
      }

      on_finish->complete(r);
      image_map->finish_async_op();
    }
  };

  // context callbacks which are retry-able get deleted after
  // transiting to the next state.
  struct C_UpdateMap : Context {
    ImageMap *image_map;
    std::string global_image_id;

    C_UpdateMap(ImageMap *image_map, const std::string &global_image_id)
      : image_map(image_map),
        global_image_id(global_image_id) {
    }

    void finish(int r) override {
      image_map->queue_update_map(global_image_id);
    }

    // maybe called more than once
    void complete(int r) override {
      finish(r);
    }
  };

  struct C_RemoveMap : Context {
    ImageMap *image_map;
    std::string global_image_id;

    C_RemoveMap(ImageMap *image_map, const std::string &global_image_id)
      : image_map(image_map),
        global_image_id(global_image_id) {
    }

    void finish(int r) override {
      image_map->queue_remove_map(global_image_id);
    }

    // maybe called more than once
    void complete(int r) override {
      finish(r);
    }
  };

  struct C_AcquireImage : Context {
    ImageMap *image_map;
    std::string global_image_id;

    C_AcquireImage(ImageMap *image_map, const std::string &global_image_id)
      : image_map(image_map),
        global_image_id(global_image_id) {
    }

    void finish(int r) override {
      image_map->queue_acquire_image(global_image_id);
    }

    // maybe called more than once
    void complete(int r) override {
      finish(r);
    }
  };

  // async op-tracker helper routines
  void start_async_op() {
    m_async_op_tracker.start_op();
  }
  void finish_async_op() {
    m_async_op_tracker.finish_op();
  }
  void wait_for_async_ops(Context *on_finish) {
    m_async_op_tracker.wait_for_ops(on_finish);
  }

  bool add_peer(const std::string &global_image_id, const std::string &peer_uuid);
  bool remove_peer(const std::string &global_image_id, const std::string &peer_uuid);

  // queue on-disk,acquire,remove updates in appropriate list
  void queue_update_map(const std::string &global_image_id);
  void queue_remove_map(const std::string &global_image_id);
  void queue_acquire_image(const std::string &global_image_id);
  void queue_release_image(const std::string &global_image_id);

  void handle_load(const std::map<std::string, cls::rbd::MirrorImageMap> &image_mapping);
  void handle_update_request(const Updates &updates,
                             const std::set<std::string> &remove_global_image_ids, int r);
  void handle_add_action(const std::string &global_image_id, int r);
  void handle_remove_action(const std::string &global_image_id, int r);
  void handle_shuffle_action(const std::string &global_image_id, int r);

  // continue (retry or resume depending on state machine) processing
  // current action.
  void continue_action(const std::set<std::string> &global_image_ids, int r);

  // schedule an image for update
  void schedule_action(const std::string &global_image_id);

  void schedule_update_task();
  void process_updates();
  void update_image_mapping();

  void notify_listener_acquire_release_images(const Updates &acquire, const Updates &release);
  void notify_listener_remove_images(const std::string &peer_uuid, const Updates &remove);

  void schedule_add_action(const std::string &global_image_id);
  void schedule_remove_action(const std::string &global_image_id);
  void schedule_shuffle_action(const std::string &global_image_id);

  void update_images_added(const std::string &peer_uuid,
                           const std::set<std::string> &global_image_ids);
  void update_images_removed(const std::string &peer_uuid,
                             const std::set<std::string> &global_image_ids);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_H
