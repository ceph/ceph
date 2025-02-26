// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef _INCLUDED_RBD_REPLAY_REPLAYER_HPP
#define _INCLUDED_RBD_REPLAY_REPLAYER_HPP

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <condition_variable>
#include "rbd_replay/ActionTypes.h"
#include "BoundedBuffer.hpp"
#include "ImageNameMap.hpp"
#include "PendingIO.hpp"

namespace rbd_replay {

class Replayer;

/**
   Performs Actions within a single thread.
 */
class Worker : public ActionCtx {
public:
  explicit Worker(Replayer &replayer);

  void start();

  /// Should only be called by StopThreadAction
  void stop() override;

  void join();

  void send(Action::ptr action);

  void add_pending(PendingIO::ptr io) override;

  void remove_pending(PendingIO::ptr io) override;

  librbd::Image* get_image(imagectx_id_t imagectx_id) override;

  void put_image(imagectx_id_t imagectx_id, librbd::Image* image) override;

  void erase_image(imagectx_id_t imagectx_id) override;

  librbd::RBD* rbd() override;

  librados::IoCtx* ioctx() override;

  void set_action_complete(action_id_t id) override;

  bool readonly() const override;

  rbd_loc map_image_name(std::string image_name, std::string snap_name) const override;

private:
  void run();

  Replayer &m_replayer;
  BoundedBuffer<Action::ptr> m_buffer;
  std::shared_ptr<std::thread> m_thread;
  std::map<action_id_t, PendingIO::ptr> m_pending_ios;
  std::mutex m_pending_ios_mutex;
  std::condition_variable_any m_pending_ios_empty;
  bool m_done;
};


class Replayer {
public:
  explicit Replayer(int num_action_trackers);

  ~Replayer();

  void run(const std::string &replay_file);

  librbd::RBD* get_rbd() {
    return m_rbd;
  }

  librados::IoCtx* get_ioctx() {
    return m_ioctx;
  }

  librbd::Image* get_image(imagectx_id_t imagectx_id);

  void put_image(imagectx_id_t imagectx_id, librbd::Image *image);

  void erase_image(imagectx_id_t imagectx_id);

  void set_action_complete(action_id_t id);

  bool is_action_complete(action_id_t id);

  void wait_for_actions(const action::Dependencies &deps);

  std::string pool_name() const;

  void set_pool_name(std::string pool_name);

  void set_latency_multiplier(float f);

  bool readonly() const;

  void set_readonly(bool readonly);

  void set_image_name_map(const ImageNameMap &map) {
    m_image_name_map = map;
  }

  void set_dump_perf_counters(bool dump_perf_counters) {
    m_dump_perf_counters = dump_perf_counters;
  }

  const ImageNameMap &image_name_map() const {
    return m_image_name_map;
  }

private:
  struct action_tracker_d {
    /// Maps an action ID to the time the action completed
    std::map<action_id_t, std::chrono::system_clock::time_point> actions;
    std::shared_mutex mutex;
    std::condition_variable_any condition;
  };

  void clear_images();

  action_tracker_d &tracker_for(action_id_t id);

  /// Disallow copying
  Replayer(const Replayer& rhs);
  /// Disallow assignment
  const Replayer& operator=(const Replayer& rhs);

  librbd::RBD* m_rbd;
  librados::IoCtx* m_ioctx;
  std::string m_pool_name;
  float m_latency_multiplier;
  bool m_readonly;
  ImageNameMap m_image_name_map;
  bool m_dump_perf_counters;

  std::map<imagectx_id_t, librbd::Image*> m_images;
  std::shared_mutex m_images_mutex;

  /// Actions are hashed across the trackers by ID.
  /// Number of trackers should probably be larger than the number of cores and prime.
  /// Should definitely be odd.
  const int m_num_action_trackers;
  action_tracker_d* m_action_trackers;
};

}

#endif
