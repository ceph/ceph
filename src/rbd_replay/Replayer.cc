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
#include "common/errno.h"
#include "rbd_replay/ActionTypes.h"
#include "rbd_replay/BufferReader.h"
#include <boost/foreach.hpp>
#include <boost/thread/thread.hpp>
#include <boost/scope_exit.hpp>
#include <fstream>
#include "global/global_context.h"
#include "rbd_replay_debug.hpp"


using namespace std;
using namespace rbd_replay;

namespace {

bool is_versioned_replay(BufferReader &buffer_reader) {
  bufferlist::iterator *it;
  int r = buffer_reader.fetch(&it);
  if (r < 0) {
    return false;
  }

  if (it->get_remaining() < action::BANNER.size()) {
    return false;
  }

  std::string banner;
  it->copy(action::BANNER.size(), banner);
  bool versioned = (banner == action::BANNER);
  if (!versioned) {
    it->seek(0);
  }
  return versioned;
}

} // anonymous namespace

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
  assert(action);
  m_buffer.push_front(action);
}

void Worker::add_pending(PendingIO::ptr io) {
  assert(io);
  boost::mutex::scoped_lock lock(m_pending_ios_mutex);
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
    boost::mutex::scoped_lock lock(m_pending_ios_mutex);
    bool first_time = true;
    while (!m_pending_ios.empty()) {
      if (!first_time) {
	dout(THREAD_LEVEL) << "Worker thread trying to stop, still waiting for " << m_pending_ios.size() << " pending IOs to complete:" << dendl;
	pair<action_id_t, PendingIO::ptr> p;
	BOOST_FOREACH(p, m_pending_ios) {
	  dout(THREAD_LEVEL) << "> " << p.first << dendl;
	}
      }
      m_pending_ios_empty.timed_wait(lock, boost::posix_time::seconds(1));
      first_time = false;
    }
  }
  dout(THREAD_LEVEL) << "Worker thread stopped" << dendl;
}


void Worker::remove_pending(PendingIO::ptr io) {
  assert(io);
  m_replayer.set_action_complete(io->id());
  boost::mutex::scoped_lock lock(m_pending_ios_mutex);
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
      cerr << "Failed to initialize RADOS: " << cpp_strerror(r) << std::endl;
      goto out;
    }
    r = rados.connect();
    if (r) {
      cerr << "Failed to connect to cluster: " << cpp_strerror(r) << std::endl;
      goto out;
    }
    m_ioctx = new librados::IoCtx();
    {
      r = rados.ioctx_create(m_pool_name.c_str(), *m_ioctx);
      if (r) {
        cerr << "Failed to open pool " << m_pool_name << ": "
             << cpp_strerror(r) << std::endl;
	goto out2;
      }
      m_rbd = new librbd::RBD();
      map<thread_id_t, Worker*> workers;

      int fd = open(replay_file.c_str(), O_RDONLY);
      if (fd < 0) {
        std::cerr << "Failed to open " << replay_file << ": "
                  << cpp_strerror(errno) << std::endl;
        exit(1);
      }
      BOOST_SCOPE_EXIT( (fd) ) {
        close(fd);
      } BOOST_SCOPE_EXIT_END;

      BufferReader buffer_reader(fd);
      bool versioned = is_versioned_replay(buffer_reader);
      while (true) {
        action::ActionEntry action_entry;
        try {
          bufferlist::iterator *it;
          int r = buffer_reader.fetch(&it);
          if (r < 0) {
            std::cerr << "Failed to read from trace file: " << cpp_strerror(r)
                      << std::endl;
            exit(-r);
          }
	  if (it->get_remaining() == 0) {
	    break;
	  }

          if (versioned) {
            action_entry.decode(*it);
          } else {
            action_entry.decode_unversioned(*it);
          }
        } catch (const buffer::error &err) {
          std::cerr << "Failed to decode trace action: " << err.what() << std::endl;
          exit(1);
        }

	Action::ptr action = Action::construct(action_entry);
	if (!action) {
          // unknown / unsupported action
	  continue;
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
  assert(image);
  boost::unique_lock<boost::shared_mutex> lock(m_images_mutex);
  assert(m_images.count(imagectx_id) == 0);
  m_images[imagectx_id] = image;
}

void Replayer::erase_image(imagectx_id_t imagectx_id) {
  boost::unique_lock<boost::shared_mutex> lock(m_images_mutex);
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
  boost::system_time now(boost::get_system_time());
  action_tracker_d &tracker = tracker_for(id);
  boost::unique_lock<boost::shared_mutex> lock(tracker.mutex);
  assert(tracker.actions.count(id) == 0);
  tracker.actions[id] = now;
  tracker.condition.notify_all();
}

bool Replayer::is_action_complete(action_id_t id) {
  action_tracker_d &tracker = tracker_for(id);
  boost::shared_lock<boost::shared_mutex> lock(tracker.mutex);
  return tracker.actions.count(id) > 0;
}

void Replayer::wait_for_actions(const action::Dependencies &deps) {
  boost::posix_time::ptime release_time(boost::posix_time::neg_infin);
  BOOST_FOREACH(const action::Dependency &dep, deps) {
    dout(DEPGRAPH_LEVEL) << "Waiting for " << dep.id << dendl;
    boost::system_time start_time(boost::get_system_time());
    action_tracker_d &tracker = tracker_for(dep.id);
    boost::shared_lock<boost::shared_mutex> lock(tracker.mutex);
    bool first_time = true;
    while (tracker.actions.count(dep.id) == 0) {
      if (!first_time) {
	dout(DEPGRAPH_LEVEL) << "Still waiting for " << dep.id << dendl;
      }
      tracker.condition.timed_wait(lock, boost::posix_time::seconds(1));
      first_time = false;
    }
    boost::system_time action_completed_time(tracker.actions[dep.id]);
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
