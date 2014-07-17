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

#include "actions.hpp"
#include <cstdlib>
#include "PendingIO.hpp"
#include "rbd_replay_debug.hpp"


using namespace rbd_replay;
using namespace std;


Action::Action(action_id_t id,
               thread_id_t thread_id,
               int num_successors,
               int num_completion_successors,
               std::vector<dependency_d> &predecessors)
  : m_id(id),
    m_thread_id(thread_id),
    m_num_successors(num_successors),
    m_num_completion_successors(num_completion_successors),
    m_predecessors(predecessors) {
    }

Action::~Action() {
}

Action::ptr Action::read_from(Deser &d) {
  uint8_t type = d.read_uint8_t();
  if (d.eof()) {
    return Action::ptr();
  }
  uint32_t ionum = d.read_uint32_t();
  uint64_t thread_id = d.read_uint64_t();
  uint32_t num_successors = d.read_uint32_t();
  uint32_t num_completion_successors = d.read_uint32_t();
  uint32_t num_dependencies = d.read_uint32_t();
  vector<dependency_d> deps;
  for (unsigned int i = 0; i < num_dependencies; i++) {
    uint32_t dep_id = d.read_uint32_t();
    uint64_t time_delta = d.read_uint64_t();
    deps.push_back(dependency_d(dep_id, time_delta));
  }
  DummyAction dummy(ionum, thread_id, num_successors, num_completion_successors, deps);
  switch (type) {
  case 0:
    return StartThreadAction::read_from(dummy, d);
  case 1:
    return StopThreadAction::read_from(dummy, d);
  case 2:
    return ReadAction::read_from(dummy, d);
  case 3:
    return WriteAction::read_from(dummy, d);
  case 4:
    return AioReadAction::read_from(dummy, d);
  case 5:
    return AioWriteAction::read_from(dummy, d);
  case 6:
    return OpenImageAction::read_from(dummy, d);
  case 7:
    return CloseImageAction::read_from(dummy, d);
  default:
    cerr << "Invalid action type: " << type << std::endl;
    exit(1);
  }
}


StartThreadAction::StartThreadAction(Action &src)
  : Action(src) {
}

void StartThreadAction::perform(ActionCtx &ctx) {
  cerr << "StartThreadAction should never actually be performed" << std::endl;
  exit(1);
}

bool StartThreadAction::is_start_thread() {
  return true;
}

Action::ptr StartThreadAction::read_from(Action &src, Deser &d) {
  return Action::ptr(new StartThreadAction(src));
}


StopThreadAction::StopThreadAction(Action &src)
  : Action(src) {
}

void StopThreadAction::perform(ActionCtx &ctx) {
  dout(ACTION_LEVEL) << "Performing stop thread action #" << id() << dendl;
  ctx.stop();
}

Action::ptr StopThreadAction::read_from(Action &src, Deser &d) {
  return Action::ptr(new StopThreadAction(src));
}

AioReadAction::AioReadAction(const Action &src,
                             imagectx_id_t imagectx_id,
                             uint64_t offset,
                             uint64_t length)
  : Action(src),
    m_imagectx_id(imagectx_id),
    m_offset(offset),
    m_length(length) {
    }

Action::ptr AioReadAction::read_from(Action &src, Deser &d) {
  imagectx_id_t imagectx_id = d.read_uint64_t();
  uint64_t offset = d.read_uint64_t();
  uint64_t length = d.read_uint64_t();
  return Action::ptr(new AioReadAction(src, imagectx_id, offset, length));
}

void AioReadAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing AIO read action #" << id() << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  assert(image);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  image->aio_read(m_offset, m_length, io->bufferlist(), &io->completion());
  worker.add_pending(io);
}


ReadAction::ReadAction(const Action &src,
                       imagectx_id_t imagectx_id,
                       uint64_t offset,
                       uint64_t length)
  : Action(src),
    m_imagectx_id(imagectx_id),
    m_offset(offset),
    m_length(length) {
    }

Action::ptr ReadAction::read_from(Action &src, Deser &d) {
  imagectx_id_t imagectx_id = d.read_uint64_t();
  uint64_t offset = d.read_uint64_t();
  uint64_t length = d.read_uint64_t();
  return Action::ptr(new ReadAction(src, imagectx_id, offset, length));
}

void ReadAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing read action #" << id() << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  image->read(m_offset, m_length, io->bufferlist());
  worker.remove_pending(io);
}


AioWriteAction::AioWriteAction(const Action &src,
                               imagectx_id_t imagectx_id,
                               uint64_t offset,
                               uint64_t length)
  : Action(src),
    m_imagectx_id(imagectx_id),
    m_offset(offset),
    m_length(length) {
    }

Action::ptr AioWriteAction::read_from(Action &src, Deser &d) {
  imagectx_id_t imagectx_id = d.read_uint64_t();
  uint64_t offset = d.read_uint64_t();
  uint64_t length = d.read_uint64_t();
  return Action::ptr(new AioWriteAction(src, imagectx_id, offset, length));
}

void AioWriteAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing AIO write action #" << id() << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  io->bufferlist().append_zero(m_length);
  image->aio_write(m_offset, m_length, io->bufferlist(), &io->completion());
  worker.add_pending(io);
}


WriteAction::WriteAction(const Action &src,
                         imagectx_id_t imagectx_id,
                         uint64_t offset,
                         uint64_t length)
  : Action(src),
    m_imagectx_id(imagectx_id),
    m_offset(offset),
    m_length(length) {
    }

Action::ptr WriteAction::read_from(Action &src, Deser &d) {
  imagectx_id_t imagectx_id = d.read_uint64_t();
  uint64_t offset = d.read_uint64_t();
  uint64_t length = d.read_uint64_t();
  return Action::ptr(new WriteAction(src, imagectx_id, offset, length));
}

void WriteAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing write action #" << id() << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  io->bufferlist().append_zero(m_length);
  image->write(m_offset, m_length, io->bufferlist());
  worker.remove_pending(io);
}


OpenImageAction::OpenImageAction(Action &src,
                                 imagectx_id_t imagectx_id,
                                 string name,
                                 string snap_name,
                                 bool readonly)
  : Action(src),
    m_imagectx_id(imagectx_id),
    m_name(name),
    m_snap_name(snap_name),
    m_readonly(readonly) {
    }

Action::ptr OpenImageAction::read_from(Action &src, Deser &d) {
  imagectx_id_t imagectx_id = d.read_uint64_t();
  string name = d.read_string();
  string snap_name = d.read_string();
  bool readonly = d.read_bool();
  return Action::ptr(new OpenImageAction(src, imagectx_id, name, snap_name, readonly));
}

void OpenImageAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing open image action #" << id() << dendl;
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  librbd::Image *image = new librbd::Image();
  librbd::RBD *rbd = worker.rbd();
  int r;
  if (m_readonly) {
    r = rbd->open_read_only(*worker.ioctx(), *image, m_name.c_str(), m_snap_name.c_str());
  } else {
    r = rbd->open(*worker.ioctx(), *image, m_name.c_str(), m_snap_name.c_str());
  }
  if (r) {
    cerr << "Unable to open image '" << m_name << "' with snap '" << m_snap_name << "' and readonly " << m_readonly << ": " << strerror(-r) << std::endl;
    exit(1);
  }
  worker.put_image(m_imagectx_id, image);
  worker.remove_pending(io);
}


CloseImageAction::CloseImageAction(Action &src,
                                   imagectx_id_t imagectx_id)
  : Action(src),
    m_imagectx_id(imagectx_id) {
    }

Action::ptr CloseImageAction::read_from(Action &src, Deser &d) {
  imagectx_id_t imagectx_id = d.read_uint64_t();
  return Action::ptr(new CloseImageAction(src, imagectx_id));
}

void CloseImageAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing close image action #" << id() << dendl;
  worker.erase_image(m_imagectx_id);
  worker.set_action_complete(pending_io_id());
}
