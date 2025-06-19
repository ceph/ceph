// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#pragma once

#include "MDSContext.h"
#include "MDSRank.h"
#include "msg/Message.h"

class C_MDS_RetryMessage : public MDSInternalContext {
public:
  C_MDS_RetryMessage(MDSRank *mds, const cref_t<Message> &m)
    : MDSInternalContext(mds), m(m) {}
  void finish(int r) override {
    get_mds()->retry_dispatch(m);
  }
protected:
  cref_t<Message> m;
};

class CF_MDS_RetryMessageFactory : public MDSContextFactory {
public:
  CF_MDS_RetryMessageFactory(MDSRank *mds, const cref_t<Message> &m)
    : mds(mds), m(m) {}

  MDSContext *build() {
    return new C_MDS_RetryMessage(mds, m);
  }
private:
  MDSRank *mds;
  cref_t<Message> m;
};
