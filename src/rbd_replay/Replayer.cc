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
#include <chrono>
#include <condition_variable>
#include <thread>
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
  m_thread = std::shared_ptr<std::thread>(new std::thread(&Worker::run, this));
}

// Should only be called by StopThreadAction
void Worker::stop() {
  m_done = true;
}

void Worker::join() {
  m_thread->join();
}

void Worker::send(Action::ptr action) {
  assert(action);
  m_buffer.push_front(action);
}

void Worker::add_pending(PendingIO::ptr io) {
  assert(io);
  std::lock_guard<std::mutex> lock(m_pending_ios_mutex);
  assertf(m_pending_ios.count(io->id()) == 0, "id = %d", io->id());
  m_pending_ios[io->id()] = io;
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
    std::unique_lock<std::mutex> lock(m_pending_ios_mutex);
    bool first_time = true;
    while (!m_pending_ios.empty()) {
      if (!first_time) {
	dout(THREAD_LEVEL) << "Worker thread trying to stop, still waiting for " << m_pending_ios.size() << " pending IOs to complete:" << dendl;
	pair<action_id_t, PendingIO::ptr> p;
	BOOST_FOREACH(p, m_pending_ios) {
	  dout(THREAD_LEVEL) << "> " << p.first << dendl;
	}
      }
      m_pending_ios_empty.wait_for(lock, std::chrono::seconds(1));
      first_time = false;
    }
  }
  dout(THREAD_LEVEL) << "Worker thread stopped" << dendl;
}


void Worker::remove_pending(PendingIO::ptr io) {
  assert(io);
  m_replayer.set_action_complete(io->id());
  std::lock_guard<std::mutex> lock(m_pending_ios_mutex);
  size_t num_erased = m_pending_ios.erase(io->id());
  assertf(num_erased == 1, "id = %d", io->id());
  if (m_pending_ios.empty()) {
    m_pending_ios_empty.notify_all();
  }
}


librbd::Image* Worker::get_image(imagectx_id_t imagectx_id) {
  return m_replayer.get_image(imagectx_id);
}


void Worker::put_image(imagectx_id_t imagectx_id, librbd::Image* image) {
  assert(image);
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

bool Worker::readonly() const {
  return m_replayer.readonly();
}

rbd_loc Worker::map_image_name(string image_name, string snap_name) const {
  return m_replayer.image_name_map().map(rbd_loc("", image_name, snap_name));
}


Replayer::Replayer(int num_action_trackers)
  : m_rbd(NULL), m_ioctx(0),  
    m_pool_name("rbd"), m_latency_multiplier(1.0), 
    m_readonly(false), m_dump_perf_counters(false),
    m_num_action_trackers(num_action_trackers),
    m_action_trackers(new action_tracker_d[m_num_action_trackers]) {
  assertf(num_action_trackers > 0, "num_action_trackers = %d", num_action_trackers);
}

Replayer::~Replayer() {
  delete[] m_action_trackers;
}

Replayer::action_tracker_d &Replayer::tracker_for(action_id_t id) {
  return m_action_trackers[id % m_num_action_trackers];
}

void Replayer::run(const std::string& replay_file) {
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
      r = rados.ioctx_create(m_pool_name.c_str(), *m_ioctx);
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
  std::lock_guard<std::mutex> lock(m_images_mutex);
  return m_images[imagectx_id];
}

void Replayer::put_image(imagectx_id_t imagectx_id, librbd::Image *image) {
  assert(image);
  std::unique_lock<std::mutex> lock(m_images_mutex);
  assert(m_images.count(imagectx_id) == 0);
  m_images[imagectx_id] = image;
}

void Replayer::erase_image(imagectx_id_t imagectx_id) {
  std::unique_lock<std::mutex> lock(m_images_mutex);
  librbd::Image* image = m_images[imagectx_id];
  if (m_dump_perf_counters) {
    string command = "perf dump";
    cmdmap_t cmdmap;
    string format = "json-pretty";
    bufferlist out;
    g_ceph_context->do_command(command, cmdmap, format, &out);
    out.write_stream(cout);
    cout << std::endl;
    cout.flush();
  }
  delete image;
  m_images.erase(imagectx_id);
}

void Replayer::set_action_complete(action_id_t id) {
  dout(DEPGRAPH_LEVEL) << "ActionTracker::set_complete(" << id << ")" << dendl;
  auto now = std::chrono::system_clock::now();
  action_tracker_d &tracker = tracker_for(id);
  std::unique_lock<std::mutex> lock(tracker.mutex);
  assert(tracker.actions.count(id) == 0);
  tracker.actions[id] = now;
  tracker.condition.notify_all();
}

bool Replayer::is_action_complete(action_id_t id) {
  action_tracker_d &tracker = tracker_for(id);
  std::unique_lock<std::mutex> lock(tracker.mutex);
  return tracker.actions.count(id) > 0;
}

void Replayer::wait_for_actions(const vector<dependency_d> &deps) {
  auto release_time = std::chrono::time_point<std::chrono::system_clock>::min();
  BOOST_FOREACH(const dependency_d &dep, deps) {
    dout(DEPGRAPH_LEVEL) << "Waiting for " << dep.id << dendl;
    auto start_time = std::chrono::system_clock::now();
    action_tracker_d &tracker = tracker_for(dep.id);
    std::unique_lock<std::mutex> lock(tracker.mutex);
    bool first_time = true;
    while (tracker.actions.count(dep.id) == 0) {
      if (!first_time) {
	dout(DEPGRAPH_LEVEL) << "Still waiting for " << dep.id << dendl;
      }
      tracker.condition.wait_for(lock, std::chrono::seconds(1));
      first_time = false;
    }
    auto action_completed_time = tracker.actions[dep.id];
    lock.unlock();
    auto end_time = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
    dout(DEPGRAPH_LEVEL) << "Finished waiting for " << dep.id << " after " << micros << " microseconds" << dendl;
    auto sub_release_time = action_completed_time + std::chrono::nanoseconds(long(dep.time_delta * m_latency_multiplier));
    if (sub_release_time > release_time) {
      release_time = sub_release_time;
    }
  }
  if (release_time > std::chrono::system_clock::now()) {
    dout(SLEEP_LEVEL) << "Sleeping for " << std::chrono::duration_cast<std::chrono::microseconds>(release_time - std::chrono::system_clock::now()).count() << " microseconds" << dendl;
    std::this_thread::sleep_until(release_time);
  }
}

void Replayer::clear_images() {
  std::unique_lock<std::mutex> lock(m_images_mutex);
  if (m_dump_perf_counters && !m_images.empty()) {
    string command = "perf dump";
    cmdmap_t cmdmap;
    string format = "json-pretty";
    bufferlist out;
    g_ceph_context->do_command(command, cmdmap, format, &out);
    out.write_stream(cout);
    cout << std::endl;
    cout.flush();
  }
  pair<imagectx_id_t, librbd::Image*> p;
  BOOST_FOREACH(p, m_images) {
    delete p.second;
  }
  m_images.clear();
}

void Replayer::set_latency_multiplier(float f) {
  assertf(f >= 0, "f = %f", f);
  m_latency_multiplier = f;
}

bool Replayer::readonly() const {
  return m_readonly;
}

void Replayer::set_readonly(bool readonly) {
  m_readonly = readonly;
}

string Replayer::pool_name() const {
  return m_pool_name;
}

void Replayer::set_pool_name(string pool_name) {
  m_pool_name = pool_name;
}
