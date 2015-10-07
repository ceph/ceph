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
  out << "}";
}


void IO::write_to(Ser& out, io_type iotype) const {
  // TODO break compatibility now to add version (and yank unused fields)?
  out.write_uint8_t(iotype);
  out.write_uint32_t(m_ionum);
  out.write_uint64_t(m_thread_id);
  out.write_uint32_t(0);
  out.write_uint32_t(0);
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
