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

#ifndef _INCLUDED_RBD_REPLAY_ACTIONS_HPP
#define _INCLUDED_RBD_REPLAY_ACTIONS_HPP

#include <boost/shared_ptr.hpp>
#include "include/rbd/librbd.hpp"
#include "Deser.hpp"
#include "rbd_loc.hpp"

// Stupid Doxygen requires this or else the typedef docs don't appear anywhere.
/// @file rbd_replay/actions.hpp

namespace rbd_replay {

typedef uint64_t imagectx_id_t;
typedef uint64_t thread_id_t;

/// Even IDs are normal actions, odd IDs are completions.
typedef uint32_t action_id_t;

/**
   Dependencies link actions to earlier actions or completions.
   If an action has a dependency \c d then it waits until \c d.time_delta nanoseconds after the action or completion with ID \c d.id has fired.
*/
struct dependency_d {
  /// ID of the action or completion to wait for.
  action_id_t id;

  /// Nanoseconds of delay to wait until after the action or completion fires.
  uint64_t time_delta;

  /**
     @param id ID of the action or completion to wait for.
     @param time_delta Nanoseconds of delay to wait after the action or completion fires.
   */
  dependency_d(action_id_t id,
	       uint64_t time_delta)
    : id(id),
      time_delta(time_delta) {
  }
};

// These are written to files, so don't change existing assignments.
enum io_type {
  IO_START_THREAD,
  IO_STOP_THREAD,
  IO_READ,
  IO_WRITE,
  IO_ASYNC_READ,
  IO_ASYNC_WRITE,
  IO_OPEN_IMAGE,
  IO_CLOSE_IMAGE,
};


class PendingIO;


/**
   %Context through which an Action interacts with its environment.
 */
class ActionCtx {
public:
  virtual ~ActionCtx() {
  }

  /**
     Returns the image with the given ID.
     The image must have been previously tracked with put_image(imagectx_id_t,librbd::Image*).
   */
  virtual librbd::Image* get_image(imagectx_id_t imagectx_id) = 0;

  /**
     Tracks an image.
     put_image(imagectx_id_t,librbd::Image*) must not have been called previously with the same ID,
     and the image must not be NULL.
   */
  virtual void put_image(imagectx_id_t imagectx_id, librbd::Image* image) = 0;

  /**
     Stops tracking an Image and release it.
     This deletes the C++ object, not the image itself.
     The image must have been previously tracked with put_image(imagectx_id_t,librbd::Image*).
   */
  virtual void erase_image(imagectx_id_t imagectx_id) = 0;

  virtual librbd::RBD* rbd() = 0;

  virtual librados::IoCtx* ioctx() = 0;

  virtual void add_pending(boost::shared_ptr<PendingIO> io) = 0;

  virtual bool readonly() const = 0;

  virtual void remove_pending(boost::shared_ptr<PendingIO> io) = 0;

  virtual void set_action_complete(action_id_t id) = 0;

  virtual void stop() = 0;

  /**
     Maps an image name from the name in the original trace to the name that should be used when replaying.
     @param image_name name of the image in the original trace
     @param snap_name name of the snap in the orginal trace
     @return image name to replay against
   */
  virtual rbd_loc map_image_name(std::string image_name, std::string snap_name) const = 0;
};


/**
   Performs an %IO or a maintenance action such as starting or stopping a thread.
   Actions are read from a replay file and scheduled by Replayer.
   Corresponds to the IO class, except that Actions are executed by rbd-replay,
   and IOs are used by rbd-replay-prep for processing the raw trace.
 */
class Action {
public:
  typedef boost::shared_ptr<Action> ptr;

  Action(action_id_t id,
	 thread_id_t thread_id,
	 int num_successors,
	 int num_completion_successors,
	 std::vector<dependency_d> &predecessors);

  virtual ~Action();

  virtual void perform(ActionCtx &ctx) = 0;

  /// Returns the ID of the completion corresponding to this action.
  action_id_t pending_io_id() {
    return m_id + 1;
  }

  // There's probably a better way to do this, but oh well.
  virtual bool is_start_thread() {
    return false;
  }

  action_id_t id() const {
    return m_id;
  }

  thread_id_t thread_id() const {
    return m_thread_id;
  }

  const std::vector<dependency_d>& predecessors() const {
    return m_predecessors;
  }

  /// Reads and constructs an action from the replay file.
  static ptr read_from(Deser &d);

protected:
  std::ostream& dump_action_fields(std::ostream& o) const;

private:
  friend std::ostream& operator<<(std::ostream&, const Action&);

  virtual std::ostream& dump(std::ostream& o) const = 0;

  const action_id_t m_id;
  const thread_id_t m_thread_id;
  const int m_num_successors;
  const int m_num_completion_successors;
  const std::vector<dependency_d> m_predecessors;
};

/// Writes human-readable debug information about the action to the stream.
/// @related Action
std::ostream& operator<<(std::ostream& o, const Action& a);


/**
   Placeholder for partially-constructed actions.
   Does nothing, and does not appear in the replay file.
 */
class DummyAction : public Action {
public:
  DummyAction(action_id_t id,
	      thread_id_t thread_id,
	      int num_successors,
	      int num_completion_successors,
	      std::vector<dependency_d> &predecessors)
    : Action(id, thread_id, num_successors, num_completion_successors, predecessors) {
  }

  void perform(ActionCtx &ctx) {
  }

private:
  std::ostream& dump(std::ostream& o) const;
};


class StopThreadAction : public Action {
public:
  explicit StopThreadAction(Action &src);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;
};


class AioReadAction : public Action {
public:
  AioReadAction(const Action &src,
		imagectx_id_t imagectx_id,
		uint64_t offset,
		uint64_t length);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;

  imagectx_id_t m_imagectx_id;
  uint64_t m_offset;
  uint64_t m_length;
};


class ReadAction : public Action {
public:
  ReadAction(const Action &src,
	     imagectx_id_t imagectx_id,
	     uint64_t offset,
	     uint64_t length);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;

  imagectx_id_t m_imagectx_id;
  uint64_t m_offset;
  uint64_t m_length;
};


class AioWriteAction : public Action {
public:
  AioWriteAction(const Action &src,
		 imagectx_id_t imagectx_id,
		 uint64_t offset,
		 uint64_t length);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;

  imagectx_id_t m_imagectx_id;
  uint64_t m_offset;
  uint64_t m_length;
};


class WriteAction : public Action {
public:
  WriteAction(const Action &src,
	      imagectx_id_t imagectx_id,
	      uint64_t offset,
	      uint64_t length);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;

  imagectx_id_t m_imagectx_id;
  uint64_t m_offset;
  uint64_t m_length;
};


class OpenImageAction : public Action {
public:
  OpenImageAction(Action &src,
		  imagectx_id_t imagectx_id,
		  std::string name,
		  std::string snap_name,
		  bool readonly);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;

  imagectx_id_t m_imagectx_id;
  std::string m_name;
  std::string m_snap_name;
  bool m_readonly;
};


class CloseImageAction : public Action {
public:
  CloseImageAction(Action &src,
		   imagectx_id_t imagectx_id);

  void perform(ActionCtx &ctx);

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;

  imagectx_id_t m_imagectx_id;
};


class StartThreadAction : public Action {
public:
  explicit StartThreadAction(Action &src);

  void perform(ActionCtx &ctx);

  bool is_start_thread();

  static Action::ptr read_from(Action &src, Deser &d);

private:
  std::ostream& dump(std::ostream& o) const;
};

}

#endif
