// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_MINTERMDS_H
#define CEPH_MINTERMDS_H

#include "include/types.h"
#include "include/fs_types.h"

#include "msg/Message.h"

class MInterMDS : public Message {
public:
  typedef boost::intrusive_ptr<MInterMDS> ref;
  typedef boost::intrusive_ptr<MInterMDS const> const_ref;

  template <typename... T>
  MInterMDS(T&&... args) : Message(std::forward<T>(args)...) {}

  virtual bool is_forwardable() const { return false; }

  // N.B. only some types of messages we should be 'forwarding'; they
  // explicitly encode their source mds, which gets clobbered when resent
  virtual MInterMDS::ref forwardable() const {ceph_abort();}

protected:
  virtual ~MInterMDS() {}
};

#endif
