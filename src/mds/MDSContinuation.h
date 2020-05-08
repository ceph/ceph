// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_MDSCONTINUATION_H
#define CEPH_MDSCONTINUATION_H

#include "common/Continuation.h"
#include "mds/Mutation.h"
#include "MDSContext.h"
class Server;
class MDSContinuation : public Continuation {
protected:
  Server *server;
  MDSContext *get_internal_callback(int stage);
  MDSIOContextBase *get_io_callback(int stage);
  
public:
  MDSContinuation(Server *s) :
    Continuation(NULL), server(s) {}
};

#endif // CEPH_CONTINUATION_H
