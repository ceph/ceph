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
#include <boost/foreach.hpp>
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
  case IO_START_THREAD:
    return StartThreadAction::read_from(dummy, d);
  case IO_STOP_THREAD:
    return StopThreadAction::read_from(dummy, d);
  case IO_READ:
    return ReadAction::read_from(dummy, d);
  case IO_WRITE:
    return WriteAction::read_from(dummy, d);
  case IO_ASYNC_READ:
    return AioReadAction::read_from(dummy, d);
  case IO_ASYNC_WRITE:
    return AioWriteAction::read_from(dummy, d);
  case IO_OPEN_IMAGE:
    return OpenImageAction::read_from(dummy, d);
  case IO_CLOSE_IMAGE:
    return CloseImageAction::read_from(dummy, d);
  default:
    cerr << "Invalid action type: " << type << std::endl;
    exit(1);
  }
}

std::ostream& Action::dump_action_fields(std::ostream& o) const {
  o << "id=" << m_id << ", thread_id=" << m_thread_id << ", predecessors=[";
  bool first = true;
  BOOST_FOREACH(const dependency_d &d, m_predecessors) {
    if (!first) {
      o << ",";
    }
    o << d.id;
    first = false;
  }
  return o << "]";
}

std::ostream& rbd_replay::operator<<(std::ostream& o, const Action& a) {
  return a.dump(o);
}


std::ostream& DummyAction::dump(std::ostream& o) const {
  o << "DummyAction[";
  dump_action_fields(o);
  return o << "]";
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

std::ostream& StartThreadAction::dump(std::ostream& o) const {
  o << "StartThreadAction[";
  dump_action_fields(o);
  return o << "]";
}


StopThreadAction::StopThreadAction(Action &src)
  : Action(src) {
}

void StopThreadAction::perform(ActionCtx &ctx) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  ctx.stop();
}

Action::ptr StopThreadAction::read_from(Action &src, Deser &d) {
  return Action::ptr(new StopThreadAction(src));
}

std::ostream& StopThreadAction::dump(std::ostream& o) const {
  o << "StopThreadAction[";
  dump_action_fields(o);
  return o << "]";
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
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  assert(image);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  int r = image->aio_read(m_offset, m_length, io->bufferlist(), &io->completion());
  assertf(r >= 0, "id = %d, r = %d", id(), r);
}

std::ostream& AioReadAction::dump(std::ostream& o) const {
  o << "AioReadAction[";
  dump_action_fields(o);
  return o << ", imagectx_id=" << m_imagectx_id << ", offset=" << m_offset << ", length=" << m_length << "]";
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
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  ssize_t r = image->read(m_offset, m_length, io->bufferlist());
  assertf(r >= 0, "id = %d, r = %d", id(), r);
  worker.remove_pending(io);
}

std::ostream& ReadAction::dump(std::ostream& o) const {
  o << "ReadAction[";
  dump_action_fields(o);
  return o << ", imagectx_id=" << m_imagectx_id << ", offset=" << m_offset << ", length=" << m_length << "]";
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

static std::string create_fake_data() {
  char data[1 << 20]; // 1 MB
  for (unsigned int i = 0; i < sizeof(data); i++) {
    data[i] = (char) i;
  }
  return std::string(data, sizeof(data));
}

void AioWriteAction::perform(ActionCtx &worker) {
  static const std::string fake_data(create_fake_data());
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  uint64_t remaining = m_length;
  while (remaining > 0) {
    uint64_t n = std::min(remaining, (uint64_t)fake_data.length());
    io->bufferlist().append(fake_data.data(), n);
    remaining -= n;
  }
  worker.add_pending(io);
  if (worker.readonly()) {
    worker.remove_pending(io);
  } else {
    int r = image->aio_write(m_offset, m_length, io->bufferlist(), &io->completion());
    assertf(r >= 0, "id = %d, r = %d", id(), r);
  }
}

std::ostream& AioWriteAction::dump(std::ostream& o) const {
  o << "AioWriteAction[";
  dump_action_fields(o);
  return o << ", imagectx_id=" << m_imagectx_id << ", offset=" << m_offset << ", length=" << m_length << "]";
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
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  io->bufferlist().append_zero(m_length);
  if (!worker.readonly()) {
    ssize_t r = image->write(m_offset, m_length, io->bufferlist());
    assertf(r >= 0, "id = %d, r = %d", id(), r);
  }
  worker.remove_pending(io);
}

std::ostream& WriteAction::dump(std::ostream& o) const {
  o << "WriteAction[";
  dump_action_fields(o);
  return o << ", imagectx_id=" << m_imagectx_id << ", offset=" << m_offset << ", length=" << m_length << "]";
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
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  librbd::Image *image = new librbd::Image();
  librbd::RBD *rbd = worker.rbd();
  rbd_loc name(worker.map_image_name(m_name, m_snap_name));
  int r;
  if (m_readonly || worker.readonly()) {
    r = rbd->open_read_only(*worker.ioctx(), *image, name.image.c_str(), name.snap.c_str());
  } else {
    r = rbd->open(*worker.ioctx(), *image, name.image.c_str(), name.snap.c_str());
  }
  if (r) {
    cerr << "Unable to open image '" << m_name
	 << "' with snap '" << m_snap_name
	 << "' (mapped to '" << name.str()
	 << "') and readonly " << m_readonly
	 << ": (" << -r << ") " << strerror(-r) << std::endl;
    exit(1);
  }
  worker.put_image(m_imagectx_id, image);
  worker.remove_pending(io);
}

std::ostream& OpenImageAction::dump(std::ostream& o) const {
  o << "OpenImageAction[";
  dump_action_fields(o);
  return o << ", imagectx_id=" << m_imagectx_id << ", name='" << m_name << "', snap_name='" << m_snap_name << "', readonly=" << m_readonly << "]";
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
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  worker.erase_image(m_imagectx_id);
  worker.set_action_complete(pending_io_id());
}

std::ostream& CloseImageAction::dump(std::ostream& o) const {
  o << "CloseImageAction[";
  dump_action_fields(o);
  return o << ", imagectx_id=" << m_imagectx_id << "]";
}
