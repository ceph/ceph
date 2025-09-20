// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#pragma once

#include "MDCache.h"
#include "MDSContext.h"

class C_MDS_RetryRequest : public MDSInternalContext {
  MDCache *cache;
  MDRequestRef mdr;
 public:
  C_MDS_RetryRequest(MDCache *c, const MDRequestRef& r) :
    MDSInternalContext(c->mds), cache(c), mdr(r) {}
  void finish(int r) override;
};

class CF_MDS_RetryRequestFactory : public MDSContextFactory {
public:
  CF_MDS_RetryRequestFactory(MDCache *cache, const MDRequestRef& mdr, bool dl) :
    mdcache(cache), mdr(mdr), drop_locks(dl) {}
  MDSContext *build() override;
private:
  MDCache *mdcache;
  MDRequestRef mdr;
  bool drop_locks;
};
