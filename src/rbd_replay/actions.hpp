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
#include "common/Formatter.h"
#include "rbd_replay/ActionTypes.h"
#include "rbd_loc.hpp"
#include <iostream>

// Stupid Doxygen requires this or else the typedef docs don't appear anywhere.
/// @file rbd_replay/actions.hpp

namespace rbd_replay {

typedef uint64_t imagectx_id_t;
typedef uint64_t thread_id_t;

/// Even IDs are normal actions, odd IDs are completions.
typedef uint32_t action_id_t;

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

  virtual ~Action() {
  }

  virtual void perform(ActionCtx &ctx) = 0;

  /// Returns the ID of the completion corresponding to this action.
  action_id_t pending_io_id() {
    return id() + 1;
  }

  // There's probably a better way to do this, but oh well.
  virtual bool is_start_thread() {
    return false;
  }

  virtual action_id_t id() const = 0;
  virtual thread_id_t thread_id() const = 0;
  virtual const action::Dependencies& predecessors() const = 0;

  virtual std::ostream& dump(std::ostream& o) const = 0;

  static ptr construct(const action::ActionEntry &action_entry);
};

template <typename ActionType>
class TypedAction : public Action {
public:
  explicit TypedAction(const ActionType &action) : m_action(action) {
  }

  virtual action_id_t id() const {
    return m_action.id;
  }

  virtual thread_id_t thread_id() const {
    return m_action.thread_id;
  }

  virtual const action::Dependencies& predecessors() const {
    return m_action.dependencies;
  }

  virtual std::ostream& dump(std::ostream& o) const {
    o << get_action_name() << ": ";
    ceph::JSONFormatter formatter(false);
    formatter.open_object_section("");
    m_action.dump(&formatter);
    formatter.close_section();
    formatter.flush(o);
    return o;
  }

protected:
  const ActionType m_action;

  virtual const char *get_action_name() const = 0;
};

/// Writes human-readable debug information about the action to the stream.
/// @related Action
std::ostream& operator<<(std::ostream& o, const Action& a);

class StartThreadAction : public TypedAction<action::StartThreadAction> {
public:
  explicit StartThreadAction(const action::StartThreadAction &action)
    : TypedAction<action::StartThreadAction>(action) {
  }

  virtual bool is_start_thread() {
    return true;
  }
  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "StartThreadAction";
  }
};

class StopThreadAction : public TypedAction<action::StopThreadAction> {
public:
  explicit StopThreadAction(const action::StopThreadAction &action)
    : TypedAction<action::StopThreadAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "StartThreadAction";
  }
};


class AioReadAction : public TypedAction<action::AioReadAction> {
public:
  explicit AioReadAction(const action::AioReadAction &action)
    : TypedAction<action::AioReadAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "AioReadAction";
  }
};


class ReadAction : public TypedAction<action::ReadAction> {
public:
  explicit ReadAction(const action::ReadAction &action)
    : TypedAction<action::ReadAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "ReadAction";
  }
};


class AioWriteAction : public TypedAction<action::AioWriteAction> {
public:
  explicit AioWriteAction(const action::AioWriteAction &action)
    : TypedAction<action::AioWriteAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "AioWriteAction";
  }
};


class WriteAction : public TypedAction<action::WriteAction> {
public:
  explicit WriteAction(const action::WriteAction &action)
    : TypedAction<action::WriteAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "WriteAction";
  }
};


class AioDiscardAction : public TypedAction<action::AioDiscardAction> {
public:
  explicit AioDiscardAction(const action::AioDiscardAction &action)
    : TypedAction<action::AioDiscardAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "AioDiscardAction";
  }
};


class DiscardAction : public TypedAction<action::DiscardAction> {
public:
  explicit DiscardAction(const action::DiscardAction &action)
    : TypedAction<action::DiscardAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "DiscardAction";
  }
};


class OpenImageAction : public TypedAction<action::OpenImageAction> {
public:
  explicit OpenImageAction(const action::OpenImageAction &action)
    : TypedAction<action::OpenImageAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "OpenImageAction";
  }
};


class CloseImageAction : public TypedAction<action::CloseImageAction> {
public:
  explicit CloseImageAction(const action::CloseImageAction &action)
    : TypedAction<action::CloseImageAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "CloseImageAction";
  }
};

class AioOpenImageAction : public TypedAction<action::AioOpenImageAction> {
public:
  explicit AioOpenImageAction(const action::AioOpenImageAction &action)
    : TypedAction<action::AioOpenImageAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "AioOpenImageAction";
  }
};


class AioCloseImageAction : public TypedAction<action::AioCloseImageAction> {
public:
  explicit AioCloseImageAction(const action::AioCloseImageAction &action)
    : TypedAction<action::AioCloseImageAction>(action) {
  }

  virtual void perform(ActionCtx &ctx);

protected:
  virtual const char *get_action_name() const {
    return "AioCloseImageAction";
  }
};

}

#endif
