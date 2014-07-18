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

#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>
#include "BoundedBuffer.hpp"
#include "PendingIO.hpp"

namespace rbd_replay {

class Replayer;

class Worker : public ActionCtx {
public:
  explicit Worker(Replayer &replayer);

  void start();

  // Should only be called by StopThreadAction
  void stop();

  void join();

  void send(Action::ptr action);

  void add_pending(PendingIO::ptr io);

  void remove_pending(PendingIO::ptr io);

  librbd::Image* get_image(imagectx_id_t imagectx_id);

  void put_image(imagectx_id_t imagectx_id, librbd::Image* image);

  void erase_image(imagectx_id_t imagectx_id);

  librbd::RBD* rbd();

  librados::IoCtx* ioctx();

  void set_action_complete(action_id_t id);

private:
  void run();

  Replayer &m_replayer;
  BoundedBuffer<Action::ptr> m_buffer;
  boost::shared_ptr<boost::thread> m_thread;
  std::map<action_id_t, PendingIO::ptr> m_pending_ios;
  boost::mutex m_pending_ios_mutex;
  boost::condition m_pending_ios_empty;
  bool m_done;
};


class Replayer {
public:
  Replayer();

  void run(const std::string replay_file);

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

  void wait_for_actions(const std::vector<dependency_d> &deps);

  void set_latency_multiplier(float f);

private:
  void clear_images();

  bool _is_action_complete(action_id_t id);

  // Disallow assignment and copying
  Replayer(const Replayer& rhs);
  const Replayer& operator=(const Replayer& rhs);

  librbd::RBD* m_rbd;
  librados::IoCtx* m_ioctx;
  float m_latency_multiplier;

  std::map<imagectx_id_t, librbd::Image*> m_images;
  boost::shared_mutex m_images_mutex;

  // Maps an action ID to the time the action completed
  std::map<action_id_t, boost::system_time> m_actions_complete;
  boost::shared_mutex m_actions_complete_mutex;
  boost::condition m_actions_complete_condition;
};

}

#endif
