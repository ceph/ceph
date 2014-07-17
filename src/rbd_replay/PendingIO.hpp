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

#ifndef _INCLUDED_RBD_REPLAY_PENDINGIO_HPP
#define _INCLUDED_RBD_REPLAY_PENDINGIO_HPP

#include <boost/enable_shared_from_this.hpp>
#include "actions.hpp"

namespace rbd_replay {

class PendingIO : public boost::enable_shared_from_this<PendingIO> {
public:
  typedef boost::shared_ptr<PendingIO> ptr;

  PendingIO(action_id_t id,
            ActionCtx &worker);

  void completed(librbd::completion_t cb);

  action_id_t id() const {
    return m_id;
  }

  ceph::bufferlist &bufferlist() {
    return m_bl;
  }

  librbd::RBD::AioCompletion &completion() {
    return m_completion;
  }

private:
  const action_id_t m_id;
  ceph::bufferlist m_bl;
  librbd::RBD::AioCompletion m_completion;
  ActionCtx &m_worker;
};

}

#endif
