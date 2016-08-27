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


#ifndef MDS2_CONTEXT_H
#define MDS2_CONTEXT_H

#include "include/Context.h"

class MDSRank;
class MDLog;


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

class C_MDSInternalNoop : public MDSInternalContextBase
{
  virtual MDSRank* get_mds() {assert(0);}
public:
  void finish(int r) {}
  void complete(int r) {}
};

/*
 * Gather needs a default-constructable class
 */
class MDSInternalContextGather : public MDSInternalContextBase
{
protected:
  MDSRank *get_mds();
};


class MDSGather : public C_GatherBase<MDSInternalContextBase, MDSInternalContextGather>
{
public:
  MDSGather(CephContext *cct, MDSInternalContextBase *onfinish) : C_GatherBase<MDSInternalContextBase, MDSInternalContextGather>(cct, onfinish) {}
protected:
  virtual MDSRank *get_mds() {return NULL;}
};
typedef C_GatherBuilderBase<MDSInternalContextBase, MDSGather> MDSGatherBuilder;

class MDSLogContextBase : public MDSInternalContextBase
{
private:
  uint64_t write_pos;
public:
  void set_write_pos(uint64_t wp) { write_pos = wp; }
  void complete(int r);
  MDSLogContextBase(uint64_t wp=0) : write_pos(wp) {}
};
#endif  // MDS_CONTEXT_H
