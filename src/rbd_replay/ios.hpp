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

#include "include/buffer_fwd.h"
#include <boost/enable_shared_from_this.hpp>
#include <boost/shared_ptr.hpp>
#include <iostream>
#include <map>
#include <set>
#include <vector>
#include "actions.hpp"


namespace rbd_replay {

class IO;

typedef std::set<boost::shared_ptr<IO> > io_set_t;

typedef std::map<action_id_t, boost::shared_ptr<IO> > io_map_t;

/**
   Used by rbd-replay-prep for processing the raw trace.
   Corresponds to the Action class, except that Actions are executed by rbd-replay,
   and IOs are used by rbd-replay-prep for processing the raw trace.
 */
class IO : public boost::enable_shared_from_this<IO> {
public:
  typedef boost::shared_ptr<IO> ptr;
  typedef std::vector<ptr> ptrs;

  /**
     @param ionum ID of this %IO
     @param start_time time the %IO started, in nanoseconds
     @param thread_id ID of the thread that issued the %IO
   */
  IO(action_id_t ionum,
     uint64_t start_time,
     thread_id_t thread_id,
     const io_set_t& deps)
    : m_ionum(ionum),
      m_start_time(start_time),
      m_dependencies(deps),
      m_thread_id(thread_id),
      m_completed(false) {
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

  virtual void encode(bufferlist &bl) const = 0;

  void set_ionum(action_id_t ionum) {
    m_ionum = ionum;
  }

  action_id_t ionum() const {
    return m_ionum;
  }

  thread_id_t thread_id() const {
    return m_thread_id;
  }

  virtual void write_debug(std::ostream& out) const = 0;

protected:
  void write_debug_base(std::ostream& out, std::string iotype) const;

private:
  action_id_t m_ionum;
  uint64_t m_start_time;
  io_set_t m_dependencies;
  thread_id_t m_thread_id;
  bool m_completed;
};

/// Used for dumping debug info.
/// @related IO
std::ostream& operator<<(std::ostream &out, const IO::ptr &io);


class StartThreadIO : public IO {
public:
  StartThreadIO(action_id_t ionum,
		uint64_t start_time,
		thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, io_set_t()) {
  }

  virtual void encode(bufferlist &bl) const;

  void write_debug(std::ostream& out) const;
};

class StopThreadIO : public IO {
public:
  StopThreadIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id,
               const io_set_t& deps)
    : IO(ionum, start_time, thread_id, deps) {
  }

  virtual void encode(bufferlist &bl) const;

  void write_debug(std::ostream& out) const;
};

class ReadIO : public IO {
public:
  ReadIO(action_id_t ionum,
	 uint64_t start_time,
	 thread_id_t thread_id,
         const io_set_t& deps,
	 imagectx_id_t imagectx,
	 uint64_t offset,
	 uint64_t length)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  virtual void encode(bufferlist &bl) const;

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
          const io_set_t& deps,
	  imagectx_id_t imagectx,
	  uint64_t offset,
	  uint64_t length)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  virtual void encode(bufferlist &bl) const;

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  uint64_t m_offset;
  uint64_t m_length;
};

class DiscardIO : public IO {
public:
  DiscardIO(action_id_t ionum,
	    uint64_t start_time,
	    thread_id_t thread_id,
            const io_set_t& deps,
	    imagectx_id_t imagectx,
	    uint64_t offset,
	    uint64_t length)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  virtual void encode(bufferlist &bl) const;

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
            const io_set_t& deps,
	    imagectx_id_t imagectx,
	    uint64_t offset,
	    uint64_t length)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  virtual void encode(bufferlist &bl) const;

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
             const io_set_t& deps,
	     imagectx_id_t imagectx,
	     uint64_t offset,
	     uint64_t length)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  virtual void encode(bufferlist &bl) const;

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
  uint64_t m_offset;
  uint64_t m_length;
};

class AioDiscardIO : public IO {
public:
  AioDiscardIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id,
               const io_set_t& deps,
	       imagectx_id_t imagectx,
	       uint64_t offset,
	       uint64_t length)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_offset(offset),
      m_length(length) {
  }

  virtual void encode(bufferlist &bl) const;

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
              const io_set_t& deps,
	      imagectx_id_t imagectx,
	      const std::string& name,
	      const std::string& snap_name,
	      bool readonly)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_name(name),
      m_snap_name(snap_name),
      m_readonly(readonly) {
  }

  virtual void encode(bufferlist &bl) const;

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
               const io_set_t& deps,
	       imagectx_id_t imagectx)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx) {
  }

  virtual void encode(bufferlist &bl) const;

  imagectx_id_t imagectx() const {
    return m_imagectx;
  }

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
};

class AioOpenImageIO : public IO {
public:
  AioOpenImageIO(action_id_t ionum,
		 uint64_t start_time,
		 thread_id_t thread_id,
		 const io_set_t& deps,
		 imagectx_id_t imagectx,
		 const std::string& name,
		 const std::string& snap_name,
		 bool readonly)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx),
      m_name(name),
      m_snap_name(snap_name),
      m_readonly(readonly) {
  }

  virtual void encode(bufferlist &bl) const;

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

class AioCloseImageIO : public IO {
public:
  AioCloseImageIO(action_id_t ionum,
		  uint64_t start_time,
		  thread_id_t thread_id,
		  const io_set_t& deps,
		  imagectx_id_t imagectx)
    : IO(ionum, start_time, thread_id, deps),
      m_imagectx(imagectx) {
  }

  virtual void encode(bufferlist &bl) const;

  imagectx_id_t imagectx() const {
    return m_imagectx;
  }

  void write_debug(std::ostream& out) const;

private:
  imagectx_id_t m_imagectx;
};

}

#endif
