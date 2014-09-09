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

// This code assumes that IO IDs and timestamps are related monotonically.
// In other words, (a.id < b.id) == (a.timestamp < b.timestamp) for all IOs a and b.

#include "ios.hpp"

using namespace std;
using namespace rbd_replay;

bool rbd_replay::compare_io_ptrs_by_start_time(IO::ptr p1, IO::ptr p2) {
  return p1->start_time() < p2->start_time();
}

static uint64_t min_time(const map<action_id_t, IO::ptr>& s) {
  if (s.empty()) {
    return 0;
  }
  return s.begin()->second->start_time();
}

static uint64_t max_time(const map<action_id_t, IO::ptr>& s) {
  if (s.empty()) {
    return 0;
  }
  map<action_id_t, IO::ptr>::const_iterator itr(s.end());
  --itr;
  return itr->second->start_time();
}

void IO::add_dependencies(const io_set_t& deps) {
  io_set_t base(m_dependencies);
  for (io_set_t::const_iterator itr = deps.begin(); itr != deps.end(); ++itr) {
    ptr dep(*itr);
    for (io_set_t::const_iterator itr2 = dep->m_dependencies.begin(); itr2 != dep->m_dependencies.end(); ++itr2) {
      base.insert(*itr2);
    }
  }
  batch_unreachable_from(deps, base, &m_dependencies);
}

void IO::write_debug_base(ostream& out, string type) const {
  out << m_ionum << ": " << m_start_time / 1000000.0 << ": " << type << ", thread = " << m_thread_id << ", deps = {";
  bool first = true;
  for (io_set_t::iterator itr = m_dependencies.begin(), end = m_dependencies.end(); itr != end; ++itr) {
    if (first) {
      first = false;
    } else {
      out << ", ";
    }
    out << (*itr)->m_ionum << ": " << m_start_time - (*itr)->m_start_time;
  }
  out << "}, num_successors = " << m_num_successors << ", numCompletionSuccessors = " << num_completion_successors();
}


void IO::write_to(Ser& out, io_type iotype) const {
  out.write_uint8_t(iotype);
  out.write_uint32_t(m_ionum);
  out.write_uint64_t(m_thread_id);
  out.write_uint32_t(m_num_successors);
  out.write_uint32_t(num_completion_successors());
  out.write_uint32_t(m_dependencies.size());
  vector<IO::ptr> deps;
  for (io_set_t::const_iterator itr = m_dependencies.begin(), end = m_dependencies.end(); itr != end; ++itr) {
    deps.push_back(*itr);
  }
  sort(deps.begin(), deps.end(), compare_io_ptrs_by_start_time);
  for (vector<IO::ptr>::const_iterator itr = deps.begin(), end = deps.end(); itr != end; ++itr) {
    out.write_uint32_t((*itr)->m_ionum);
    out.write_uint64_t(m_start_time - (*itr)->m_start_time);
  }
}

IO::ptr IO::create_completion(uint64_t start_time, thread_id_t thread_id) {
  assert(!m_completion.lock());
  IO::ptr completion(new CompletionIO(m_ionum + 1, start_time, thread_id));
  m_completion = completion;
  completion->m_dependencies.insert(shared_from_this());
  return completion;
}


// TODO: Add unit tests
// Anything in 'deps' which is not reachable from 'base' is added to 'unreachable'
void rbd_replay::batch_unreachable_from(const io_set_t& deps, const io_set_t& base, io_set_t* unreachable) {
  if (deps.empty()) {
    return;
  }

  map<action_id_t, IO::ptr> searching_for;
  for (io_set_t::const_iterator itr = deps.begin(); itr != deps.end(); ++itr) {
    searching_for[(*itr)->ionum()] = *itr;
  }

  map<action_id_t, IO::ptr> boundary;
  for (io_set_t::const_iterator itr = base.begin(); itr != base.end(); ++itr) {
    boundary[(*itr)->ionum()] = *itr;
  }

  // The boundary horizon is the maximum timestamp of IOs in the boundary.
  // This monotonically decreases, because dependencies (which are added to the set)
  // have earlier timestamp than the dependent IOs (which were just removed from the set).
  uint64_t boundary_horizon = max_time(boundary);

  for (io_map_t::iterator itr = searching_for.begin(); itr != searching_for.end(); ) {
    if (boundary_horizon >= itr->second->start_time()) {
      break;
    }
    unreachable->insert(itr->second);
    searching_for.erase(itr++);
  }
  if (searching_for.empty()) {
    return;
  }

  // The searching horizon is the minimum timestamp of IOs in the searching set.
  // This monotonically increases, because elements are only removed from the set.
  uint64_t searching_horizon = min_time(searching_for);

  while (!boundary.empty()) {
    // Take an IO from the end, which has the highest timestamp.
    // This reduces the boundary horizon as early as possible,
    // which means we can short cut as soon as possible.
    map<action_id_t, boost::shared_ptr<IO> >::iterator b_itr(boundary.end());
    --b_itr;
    boost::shared_ptr<IO> io(b_itr->second);
    boundary.erase(b_itr);

    for (io_set_t::const_iterator itr = io->dependencies().begin(), end = io->dependencies().end(); itr != end; ++itr) {
      IO::ptr dep(*itr);
      assertf(dep->ionum() < io->ionum(), "IO: %d, dependency: %d", io->ionum(), dep->ionum());
      io_map_t::iterator p = searching_for.find(dep->ionum());
      if (p != searching_for.end()) {
	searching_for.erase(p);
	if (dep->start_time() == searching_horizon) {
	  searching_horizon = min_time(searching_for);
	  if (searching_horizon == 0) {
	    return;
	  }
	}
      }
      boundary[dep->ionum()] = dep;
    }

    boundary_horizon = max_time(boundary);
    if (boundary_horizon != 0) {
      // Anything we're searching for that has a timestamp greater than the
      // boundary horizon will never be found, since the boundary horizon
      // falls monotonically.
      for (io_map_t::iterator itr = searching_for.begin(); itr != searching_for.end(); ) {
	if (boundary_horizon >= itr->second->start_time()) {
	  break;
	}
	unreachable->insert(itr->second);
	searching_for.erase(itr++);
      }
      searching_horizon = min_time(searching_for);
      if (searching_horizon == 0) {
	return;
      }
    }
  }

  // Anything we're still searching for has not been found.
  for (io_map_t::iterator itr = searching_for.begin(), end = searching_for.end(); itr != end; ++itr) {
    unreachable->insert(itr->second);
  }
}

ostream& operator<<(ostream& out, IO::ptr io) {
  io->write_debug(out);
  return out;
}

void StartThreadIO::write_to(Ser& out) const {
  IO::write_to(out, IO_START_THREAD);
}

void StartThreadIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "start thread");
}

void StopThreadIO::write_to(Ser& out) const {
  IO::write_to(out, IO_STOP_THREAD);
}

void StopThreadIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "stop thread");
}

void ReadIO::write_to(Ser& out) const {
  IO::write_to(out, IO_READ);
  out.write_uint64_t(m_imagectx);
  out.write_uint64_t(m_offset);
  out.write_uint64_t(m_length);
}

void ReadIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "read");
  out << ", imagectx=" << m_imagectx << ", offset=" << m_offset << ", length=" << m_length << "]";
}

void WriteIO::write_to(Ser& out) const {
  IO::write_to(out, IO_WRITE);
  out.write_uint64_t(m_imagectx);
  out.write_uint64_t(m_offset);
  out.write_uint64_t(m_length);
}

void WriteIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "write");
  out << ", imagectx=" << m_imagectx << ", offset=" << m_offset << ", length=" << m_length << "]";
}

void AioReadIO::write_to(Ser& out) const {
  IO::write_to(out, IO_ASYNC_READ);
  out.write_uint64_t(m_imagectx);
  out.write_uint64_t(m_offset);
  out.write_uint64_t(m_length);
}

void AioReadIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "aio read");
  out << ", imagectx=" << m_imagectx << ", offset=" << m_offset << ", length=" << m_length << "]";
}

void AioWriteIO::write_to(Ser& out) const {
  IO::write_to(out, IO_ASYNC_WRITE);
  out.write_uint64_t(m_imagectx);
  out.write_uint64_t(m_offset);
  out.write_uint64_t(m_length);
}

void AioWriteIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "aio write");
  out << ", imagectx=" << m_imagectx << ", offset=" << m_offset << ", length=" << m_length << "]";
}

void OpenImageIO::write_to(Ser& out) const {
  IO::write_to(out, IO_OPEN_IMAGE);
  out.write_uint64_t(m_imagectx);
  out.write_string(m_name);
  out.write_string(m_snap_name);
  out.write_bool(m_readonly);
}

void OpenImageIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "open image");
  out << ", imagectx=" << m_imagectx << ", name='" << m_name << "', snap_name='" << m_snap_name << "', readonly=" << m_readonly;
}

void CloseImageIO::write_to(Ser& out) const {
  IO::write_to(out, IO_CLOSE_IMAGE);
  out.write_uint64_t(m_imagectx);
}

void CloseImageIO::write_debug(std::ostream& out) const {
  write_debug_base(out, "close image");
  out << ", imagectx=" << m_imagectx;
}
