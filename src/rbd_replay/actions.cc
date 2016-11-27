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

namespace {

std::string create_fake_data() {
  char data[1 << 20]; // 1 MB
  for (unsigned int i = 0; i < sizeof(data); i++) {
    data[i] = (char) i;
  }
  return std::string(data, sizeof(data));
}

struct ConstructVisitor : public boost::static_visitor<Action::ptr> {
  inline Action::ptr operator()(const action::StartThreadAction &action) const {
    return Action::ptr(new StartThreadAction(action));
  }

  inline Action::ptr operator()(const action::StopThreadAction &action) const{
    return Action::ptr(new StopThreadAction(action));
  }

  inline Action::ptr operator()(const action::ReadAction &action) const {
    return Action::ptr(new ReadAction(action));
  }

  inline Action::ptr operator()(const action::AioReadAction &action) const {
    return Action::ptr(new AioReadAction(action));
  }

  inline Action::ptr operator()(const action::WriteAction &action) const {
    return Action::ptr(new WriteAction(action));
  }

  inline Action::ptr operator()(const action::AioWriteAction &action) const {
    return Action::ptr(new AioWriteAction(action));
  }

  inline Action::ptr operator()(const action::DiscardAction &action) const {
    return Action::ptr(new DiscardAction(action));
  }

  inline Action::ptr operator()(const action::AioDiscardAction &action) const {
    return Action::ptr(new AioDiscardAction(action));
  }

  inline Action::ptr operator()(const action::OpenImageAction &action) const {
    return Action::ptr(new OpenImageAction(action));
  }

  inline Action::ptr operator()(const action::CloseImageAction &action) const {
    return Action::ptr(new CloseImageAction(action));
  }

  inline Action::ptr operator()(const action::AioOpenImageAction &action) const {
    return Action::ptr(new AioOpenImageAction(action));
  }

  inline Action::ptr operator()(const action::AioCloseImageAction &action) const {
    return Action::ptr(new AioCloseImageAction(action));
  }

  inline Action::ptr operator()(const action::UnknownAction &action) const {
    return Action::ptr();
  }
};

} // anonymous namespace

std::ostream& rbd_replay::operator<<(std::ostream& o, const Action& a) {
  return a.dump(o);
}

Action::ptr Action::construct(const action::ActionEntry &action_entry) {
  return boost::apply_visitor(ConstructVisitor(), action_entry.action);
}

void StartThreadAction::perform(ActionCtx &ctx) {
  cerr << "StartThreadAction should never actually be performed" << std::endl;
  exit(1);
}

void StopThreadAction::perform(ActionCtx &ctx) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  ctx.stop();
}

void AioReadAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_action.imagectx_id);
  assert(image);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  int r = image->aio_read(m_action.offset, m_action.length, io->bufferlist(), &io->completion());
  assertf(r >= 0, "id = %d, r = %d", id(), r);
}

void ReadAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_action.imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  ssize_t r = image->read(m_action.offset, m_action.length, io->bufferlist());
  assertf(r >= 0, "id = %d, r = %d", id(), r);
  worker.remove_pending(io);
}

void AioWriteAction::perform(ActionCtx &worker) {
  static const std::string fake_data(create_fake_data());
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_action.imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  uint64_t remaining = m_action.length;
  while (remaining > 0) {
    uint64_t n = std::min(remaining, (uint64_t)fake_data.length());
    io->bufferlist().append(fake_data.data(), n);
    remaining -= n;
  }
  worker.add_pending(io);
  if (worker.readonly()) {
    worker.remove_pending(io);
  } else {
    int r = image->aio_write(m_action.offset, m_action.length, io->bufferlist(), &io->completion());
    assertf(r >= 0, "id = %d, r = %d", id(), r);
  }
}

void WriteAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_action.imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  io->bufferlist().append_zero(m_action.length);
  if (!worker.readonly()) {
    ssize_t r = image->write(m_action.offset, m_action.length, io->bufferlist());
    assertf(r >= 0, "id = %d, r = %d", id(), r);
  }
  worker.remove_pending(io);
}

void AioDiscardAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_action.imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  if (worker.readonly()) {
    worker.remove_pending(io);
  } else {
    int r = image->aio_discard(m_action.offset, m_action.length, &io->completion());
    assertf(r >= 0, "id = %d, r = %d", id(), r);
  }
}

void DiscardAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  librbd::Image *image = worker.get_image(m_action.imagectx_id);
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  if (!worker.readonly()) {
    ssize_t r = image->discard(m_action.offset, m_action.length);
    assertf(r >= 0, "id = %d, r = %d", id(), r);
  }
  worker.remove_pending(io);
}

void OpenImageAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  librbd::Image *image = new librbd::Image();
  librbd::RBD *rbd = worker.rbd();
  rbd_loc name(worker.map_image_name(m_action.name, m_action.snap_name));
  int r;
  if (m_action.read_only || worker.readonly()) {
    r = rbd->open_read_only(*worker.ioctx(), *image, name.image.c_str(), name.snap.c_str());
  } else {
    r = rbd->open(*worker.ioctx(), *image, name.image.c_str(), name.snap.c_str());
  }
  if (r) {
    cerr << "Unable to open image '" << m_action.name
	 << "' with snap '" << m_action.snap_name
	 << "' (mapped to '" << name.str()
	 << "') and readonly " << m_action.read_only
	 << ": (" << -r << ") " << strerror(-r) << std::endl;
    exit(1);
  }
  worker.put_image(m_action.imagectx_id, image);
  worker.remove_pending(io);
}

void CloseImageAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  worker.erase_image(m_action.imagectx_id);
  worker.set_action_complete(pending_io_id());
}

void AioOpenImageAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  // TODO: Make it async
  PendingIO::ptr io(new PendingIO(pending_io_id(), worker));
  worker.add_pending(io);
  librbd::Image *image = new librbd::Image();
  librbd::RBD *rbd = worker.rbd();
  rbd_loc name(worker.map_image_name(m_action.name, m_action.snap_name));
  int r;
  if (m_action.read_only || worker.readonly()) {
    r = rbd->open_read_only(*worker.ioctx(), *image, name.image.c_str(), name.snap.c_str());
  } else {
    r = rbd->open(*worker.ioctx(), *image, name.image.c_str(), name.snap.c_str());
  }
  if (r) {
    cerr << "Unable to open image '" << m_action.name
	 << "' with snap '" << m_action.snap_name
	 << "' (mapped to '" << name.str()
	 << "') and readonly " << m_action.read_only
	 << ": (" << -r << ") " << strerror(-r) << std::endl;
    exit(1);
  }
  worker.put_image(m_action.imagectx_id, image);
  worker.remove_pending(io);
}

void AioCloseImageAction::perform(ActionCtx &worker) {
  dout(ACTION_LEVEL) << "Performing " << *this << dendl;
  // TODO: Make it async
  worker.erase_image(m_action.imagectx_id);
  worker.set_action_complete(pending_io_id());
}
