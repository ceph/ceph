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

#include "Replayer.hpp"
#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <fstream>
#include "global/global_context.h"
#include "rbd_replay_debug.hpp"


using namespace std;
using namespace rbd_replay;


Worker::Worker(Replayer &replayer)
  : m_replayer(replayer),
    m_buffer(100),
    m_done(false) {
}

void Worker::start() {
  m_thread = boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(&Worker::run, this)));
}

// Should only be called by StopThreadAction
void Worker::stop() {
  m_done = true;
}

void Worker::join() {
  m_thread->join();
}

void Worker::send(Action::ptr action) {
  m_buffer.push_front(action);
}

void Worker::add_pending(PendingIO::ptr io) {
  boost::mutex::scoped_lock lock(m_pending_ios_mutex);
  m_pending_ios.push_back(io);
}

void Worker::run() {
  dout(THREAD_LEVEL) << "Worker thread started" << dendl;
  while (!m_done) {
    Action::ptr action;
    m_buffer.pop_back(&action);
    m_replayer.wait_for_actions(action->predecessors());
    action->perform(*this);
    m_replayer.set_action_complete(action->id());
  }
  {
    boost::mutex::scoped_lock lock(m_pending_ios_mutex);
    while (!m_pending_ios.empty()) {
      m_pending_ios_empty.wait(lock);
    }
  }
  dout(THREAD_LEVEL) << "Worker thread stopped" << dendl;
}


void Worker::remove_pending(PendingIO::ptr io) {
  m_replayer.set_action_complete(io->id());
  boost::mutex::scoped_lock lock(m_pending_ios_mutex);
  for (vector<PendingIO::ptr>::iterator itr = m_pending_ios.begin(); itr != m_pending_ios.end(); itr++) {
    if (*itr == io) {
      m_pending_ios.erase(itr);
      break;
    }
  }
  if (m_pending_ios.empty()) {
    m_pending_ios_empty.notify_all();
  }
}


librbd::Image* Worker::get_image(imagectx_id_t imagectx_id) {
  return m_replayer.get_image(imagectx_id);
}


void Worker::put_image(imagectx_id_t imagectx_id, librbd::Image* image) {
  m_replayer.put_image(imagectx_id, image);
}


void Worker::erase_image(imagectx_id_t imagectx_id) {
  m_replayer.erase_image(imagectx_id);
}


librbd::RBD* Worker::rbd() {
  return m_replayer.get_rbd();
}


librados::IoCtx* Worker::ioctx() {
  return m_replayer.get_ioctx();
}


void Worker::set_action_complete(action_id_t id) {
  m_replayer.set_action_complete(id);
}


Replayer::Replayer() {
}

void Replayer::run(const std::string replay_file) {
  {
    librados::Rados rados;
    rados.init(NULL);
    int r = rados.init_with_context(g_ceph_context);
    if (r) {
      cerr << "Unable to read conf file: " << r << std::endl;
      goto out;
    }
    r = rados.connect();
    if (r) {
      cerr << "Unable to connect to Rados: " << r << std::endl;
      goto out;
    }
    m_ioctx = new librados::IoCtx();
    {
      const char* pool_name = "rbd";
      r = rados.ioctx_create(pool_name, *m_ioctx);
      if (r) {
	cerr << "Unable to create IoCtx: " << r << std::endl;
	goto out2;
      }
      m_rbd = new librbd::RBD();
      map<thread_id_t, Worker*> workers;

      ifstream input(replay_file.c_str(), ios::in | ios::binary);
      if (!input.is_open()) {
	cerr << "Unable to open " << replay_file << std::endl;
	exit(1);
      }

      Deser deser(input);
      while (true) {
	Action::ptr action = Action::read_from(deser);
	if (!action) {
	  break;
	}
	if (action->is_start_thread()) {
	  Worker *worker = new Worker(*this);
	  workers[action->thread_id()] = worker;
	  worker->start();
	} else {
	  workers[action->thread_id()]->send(action);
	}
      }

      dout(THREAD_LEVEL) << "Waiting for workers to die" << dendl;
      pair<thread_id_t, Worker*> w;
      BOOST_FOREACH(w, workers) {
	w.second->join();
	delete w.second;
      }
      clear_images();
      delete m_rbd;
      m_rbd = NULL;
    }
  out2:
    delete m_ioctx;
    m_ioctx = NULL;
  }
 out:
  ;
}


librbd::Image* Replayer::get_image(imagectx_id_t imagectx_id) {
  boost::shared_lock<boost::shared_mutex> lock(m_images_mutex);
  return m_images[imagectx_id];
}

void Replayer::put_image(imagectx_id_t imagectx_id, librbd::Image *image) {
  boost::unique_lock<boost::shared_mutex> lock(m_images_mutex);
  assert(m_images.count(imagectx_id) == 0);
  m_images[imagectx_id] = image;
}

void Replayer::erase_image(imagectx_id_t imagectx_id) {
  boost::unique_lock<boost::shared_mutex> lock(m_images_mutex);
  delete m_images[imagectx_id];
  m_images.erase(imagectx_id);
}

void Replayer::set_action_complete(action_id_t id) {
  dout(DEPGRAPH_LEVEL) << "ActionTracker::set_complete(" << id << ")" << dendl;
  boost::system_time now(boost::get_system_time());
  boost::unique_lock<boost::shared_mutex> lock(m_actions_complete_mutex);
  assert(m_actions_complete.count(id) == 0);
  m_actions_complete[id] = now;
  m_actions_complete_condition.notify_all();
}

bool Replayer::is_action_complete(action_id_t id) {
  boost::shared_lock<boost::shared_mutex> lock(m_actions_complete_mutex);
  return _is_action_complete(id);
}

void Replayer::wait_for_actions(const vector<dependency_d> &deps) {
  boost::posix_time::ptime release_time(boost::posix_time::neg_infin);
  BOOST_FOREACH(const dependency_d &dep, deps) {
    dout(DEPGRAPH_LEVEL) << "Waiting for " << dep.id << dendl;
    boost::system_time start_time(boost::get_system_time());
    boost::shared_lock<boost::shared_mutex> lock(m_actions_complete_mutex);
    while (!_is_action_complete(dep.id)) {
      //m_actions_complete_condition.wait(lock);
      m_actions_complete_condition.timed_wait(lock, boost::posix_time::seconds(1));
      dout(DEPGRAPH_LEVEL) << "Still waiting for " << dep.id << dendl;
    }
    boost::system_time action_completed_time(m_actions_complete[dep.id]);
    lock.unlock();
    boost::system_time end_time(boost::get_system_time());
    long long micros = (end_time - start_time).total_microseconds();
    dout(DEPGRAPH_LEVEL) << "Finished waiting for " << dep.id << " after " << micros << " microseconds" << dendl;
    // Apparently the nanoseconds constructor is optional:
    // http://www.boost.org/doc/libs/1_46_0/doc/html/date_time/details.html#compile_options
    boost::system_time sub_release_time(action_completed_time + boost::posix_time::microseconds(dep.time_delta * m_latency_multiplier / 1000));
    if (sub_release_time > release_time) {
      release_time = sub_release_time;
    }
  }
  if (release_time > boost::get_system_time()) {
    dout(SLEEP_LEVEL) << "Sleeping for " << (release_time - boost::get_system_time()).total_microseconds() << " microseconds" << dendl;
    boost::this_thread::sleep(release_time);
  }
}

void Replayer::clear_images() {
  boost::unique_lock<boost::shared_mutex> lock(m_images_mutex);
  pair<imagectx_id_t, librbd::Image*> p;
  BOOST_FOREACH(p, m_images) {
    delete p.second;
  }
  m_images.clear();
}

bool Replayer::_is_action_complete(action_id_t id) {
  return m_actions_complete.count(id) > 0;
}

void Replayer::set_latency_multiplier(float f) {
  m_latency_multiplier = f;
}
