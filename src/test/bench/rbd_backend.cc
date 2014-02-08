// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include "rbd_backend.h"
#include <boost/tuple/tuple.hpp>

typedef boost::tuple<Context*, Context*> arg_type;

void on_complete(void *completion, void *_arg) {
  arg_type *arg = static_cast<arg_type*>(_arg);
  librbd::RBD::AioCompletion *comp =
    static_cast<librbd::RBD::AioCompletion *>(completion);
  ssize_t r = comp->get_return_value();
  assert(r >= 0);
  arg->get<0>()->complete(0);
  if (arg->get<1>())
    arg->get<1>()->complete(0);
  comp->release();
  delete arg;
}

void RBDBackend::write(
  const string &oid,
  uint64_t offset,
  const bufferlist &bl,
  Context *on_write_applied,
  Context *on_commit)
{
  bufferlist &bl_non_const = const_cast<bufferlist&>(bl);
  ceph::shared_ptr<librbd::Image> image = (*m_images)[oid];
  void *arg = static_cast<void *>(new arg_type(on_commit, on_write_applied));
  librbd::RBD::AioCompletion *completion =
    new librbd::RBD::AioCompletion(arg, on_complete);
  int r = image->aio_write(offset, (size_t) bl_non_const.length(),
			   bl_non_const, completion);
  assert(r >= 0);
}

void RBDBackend::read(
  const string &oid,
  uint64_t offset,
  uint64_t length,
  bufferlist *bl,
  Context *on_read_complete)
{
  ceph::shared_ptr<librbd::Image> image = (*m_images)[oid];
  void *arg = static_cast<void *>(new arg_type(on_read_complete, NULL));
  librbd::RBD::AioCompletion *completion =
    new librbd::RBD::AioCompletion(arg, on_complete);
  int r = image->aio_read(offset, (size_t) length, *bl, completion);
  assert(r >= 0);
}
