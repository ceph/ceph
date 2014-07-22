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

namespace rbd_replay {

typedef uint64_t imagectx_id_t;
typedef uint64_t thread_id_t;

// Even IDs are normal actions, odd IDs are completions
typedef uint32_t action_id_t;

struct dependency_d {
  action_id_t id;

  uint64_t time_delta;

  dependency_d(action_id_t id,
	       uint64_t time_delta)
    : id(id),
      time_delta(time_delta) {
  }
};


class PendingIO;


class ActionCtx {
public:
  virtual ~ActionCtx() {
  }

  virtual librbd::Image* get_image(imagectx_id_t imagectx_id) = 0;

  virtual void put_image(imagectx_id_t imagectx_id, librbd::Image* image) = 0;

  virtual void erase_image(imagectx_id_t imagectx_id) = 0;

  virtual librbd::RBD* rbd() = 0;

  virtual librados::IoCtx* ioctx() = 0;

  virtual void add_pending(boost::shared_ptr<PendingIO> io) = 0;

  virtual bool readonly() const = 0;

  virtual void remove_pending(boost::shared_ptr<PendingIO> io) = 0;

  virtual void set_action_complete(action_id_t id) = 0;

  virtual void stop() = 0;

  virtual std::pair<std::string, std::string> map_image_name(std::string image_name, std::string snap_name) const = 0;
};


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

std::ostream& operator<<(std::ostream& o, const Action& a);


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
