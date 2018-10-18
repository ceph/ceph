// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc. <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_MESSAGEREF_H
#define CEPH_MESSAGEREF_H
 
#include <boost/intrusive_ptr.hpp>

class Message;

typedef boost::intrusive_ptr<Message> MessageRef;
typedef boost::intrusive_ptr<Message const> MessageConstRef;

#endif
