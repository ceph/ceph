// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef MDS_CONTEXT_H
#define MDS_CONTEXT_H

#include "include/Context.h"

class MDSRank;


/**
 * Completion which has access to a reference to the global MDS instance.
 *
 * This class exists so that Context subclasses can provide the MDS pointer
 * from a pointer they already had, e.g. MDCache or Locker, rather than
 * necessarily having to carry around an extra MDS* pointer. 
 */
class MDSContext : public Context
{
protected:
  virtual MDSRank *get_mds() = 0;
};

class MDSInternalContextBase : public MDSContext
{
public:
    void complete(int r);
};

/**
 * General purpose, lets you pass in an MDS pointer.
 */
class MDSInternalContext : public MDSInternalContextBase
{
protected:
  MDSRank *mds;
  virtual MDSRank* get_mds();

public:
  explicit MDSInternalContext(MDSRank *mds_) : mds(mds_) {
    assert(mds != NULL);
  }
};

class MDSIOContextBase : public MDSContext
{
  void complete(int r);
};

class MDSIOContext : public MDSIOContextBase
{
protected:
  MDSRank *mds;
  virtual MDSRank* get_mds();

public:
  explicit MDSIOContext(MDSRank *mds_) : mds(mds_) {
    assert(mds != NULL);
  }
};

class C_MDSInternalNoop : public MDSInternalContextBase
{
  virtual MDSRank* get_mds() {assert(0);}
public:
  void finish(int r) {}
  void complete(int r) {}
};
#endif  // MDS_CONTEXT_H
