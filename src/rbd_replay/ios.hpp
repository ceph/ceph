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

#ifndef _INCLUDED_RBD_REPLAY_IOS_HPP
#define _INCLUDED_RBD_REPLAY_IOS_HPP

// This code assumes that IO IDs and timestamps are related monotonically.
// In other words, (a.id < b.id) == (a.timestamp < b.timestamp) for all IOs a and b.

#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <map>
#include <set>
#include "actions.hpp"
#include "Ser.hpp"


namespace rbd_replay {

class IO;

typedef std::set<boost::shared_ptr<IO> > io_set_t;

typedef std::map<action_id_t, boost::shared_ptr<IO> > io_map_t;

void batch_unreachable_from(const io_set_t& deps, const io_set_t& base, io_set_t* unreachable);

class IO : public boost::enable_shared_from_this<IO> {
public:
  typedef boost::shared_ptr<IO> ptr;

  typedef boost::weak_ptr<IO> weak_ptr;

  IO(action_id_t ionum,
     uint64_t start_time,
     thread_id_t thread_id,
     ptr prev)
    : m_ionum(ionum),
      m_start_time(start_time),
      m_dependencies(io_set_t()),
      m_completion(weak_ptr()),
      m_num_successors(0),
      m_thread_id(thread_id),
      m_prev(prev) {
  }

  virtual ~IO() {
  }

  uint64_t start_time() const {
    return m_start_time;
  }

  io_set_t& dependencies() {
    return m_dependencies;
  }

  const io_set_t& dependencies() const {
    return m_dependencies;
  }

  void add_dependencies(const io_set_t& deps);

  uint64_t num_completion_successors() const {
    ptr c(m_completion.lock());
    return c ? c->m_num_successors : 0;
  }

  void write_to(Ser& out, io_type iotype) const;

  virtual void write_to(Ser& out) const = 0;

  virtual bool is_completion() const {
    return false;
  }

  void set_ionum(action_id_t ionum) {
    m_ionum = ionum;
  }

  action_id_t ionum() const {
    return m_ionum;
  }

  ptr prev() const {
    return m_prev;
  }

  void set_num_successors(uint32_t n) {
    m_num_successors = n;
  }

  uint32_t num_successors() const {
    return m_num_successors;
  }

  void write_debug_base(std::ostream& out, std::string iotype) const;

  virtual void write_debug(std::ostream& out) const = 0;

  // The result must be stored somewhere, or else m_completion will expire
  ptr create_completion(uint64_t start_time, thread_id_t thread_id);

private:
  action_id_t m_ionum;
  uint64_t m_start_time;
  io_set_t m_dependencies;
  boost::weak_ptr<IO> m_completion;
  uint32_t m_num_successors;
  thread_id_t m_thread_id;
  ptr m_prev;
};

std::ostream& operator<<(std::ostream& out, IO::ptr io);


class StartThreadIO : public IO {
public:
  StartThreadIO(action_id_t ionum,
		uint64_t start_time,
		thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, IO::ptr()) {
  }

  void write_to(Ser& out) const;

  void write_debug(std::ostream& out) const;
};

class StopThreadIO : public IO {
public:
  StopThreadIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, IO::ptr()) {
  }

  void write_to(Ser& out) const;

  void write_debug(std::ostream& out) const;
};

class ReadIO : public IO {
public:
  ReadIO(action_id_t ionum,
	 uint64_t start_time,
	 thread_id_t thread_id,
	 IO::ptr prev,
	 imagectx_id_t imagectx,
	 uint64_t offset,
	 uint64_t length)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  void write_to(Ser& out) const;

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  uint64_t m_offset;
  uint64_t m_length;
};

class WriteIO : public IO {
public:
  WriteIO(action_id_t ionum,
	  uint64_t start_time,
	  thread_id_t thread_id,
	  IO::ptr prev,
	  imagectx_id_t imagectx,
	  uint64_t offset,
	  uint64_t length)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  void write_to(Ser& out) const;

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  uint64_t m_offset;
  uint64_t m_length;
};

class AioReadIO : public IO {
public:
  AioReadIO(action_id_t ionum,
	    uint64_t start_time,
	    thread_id_t thread_id,
	    IO::ptr prev,
	    imagectx_id_t imagectx,
	    uint64_t offset,
	    uint64_t length)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  void write_to(Ser& out) const;

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  uint64_t m_offset;
  uint64_t m_length;
};

class AioWriteIO : public IO {
public:
  AioWriteIO(action_id_t ionum,
	     uint64_t start_time,
	     thread_id_t thread_id,
	     IO::ptr prev,
	     imagectx_id_t imagectx,
	     uint64_t offset,
	     uint64_t length)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  void write_to(Ser& out) const;

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  uint64_t m_offset;
  uint64_t m_length;
};

class OpenImageIO : public IO {
public:
  OpenImageIO(action_id_t ionum,
	      uint64_t start_time,
	      thread_id_t thread_id,
	      IO::ptr prev,
	      imagectx_id_t imagectx,
	      const std::string& name,
	      const std::string& snap_name,
	      bool readonly)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_name(name),
      m_snap_name(snap_name),
      m_readonly(readonly) {
  }

  void write_to(Ser& out) const;

  imagectx_id_t imagectx() const {
    return m_imagectx;
  }

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  std::string m_name;
  std::string m_snap_name;
  bool m_readonly;
};

class CloseImageIO : public IO {
public:
  CloseImageIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id,
	       IO::ptr prev,
	       imagectx_id_t imagectx)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx) {
  }

  void write_to(Ser& out) const;

  imagectx_id_t imagectx() const {
    return m_imagectx;
  }

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
};

class CompletionIO : public IO {
public:
  CompletionIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, IO::ptr()) {
  }

  void write_to(Ser& out) const {
  }

  bool is_completion() const {
    return true;
  }

  void write_debug(std::ostream& out) const {
    write_debug_base(out, "completion");
  }
};

bool compare_io_ptrs_by_start_time(IO::ptr p1, IO::ptr p2);

}

#endif
