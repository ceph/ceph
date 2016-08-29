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
class Finisher;


class MDSContextBase : public Context
{
protected:
  virtual MDSRank *get_mds() = 0;
};

/**
 * General purpose, lets you pass in an MDS pointer.
 */
class MDSContext : public MDSContextBase
{
protected:
  MDSRank *mds;
  virtual MDSRank* get_mds() { return mds; }
public:
  explicit MDSContext(MDSRank *mds_) : mds(mds_) {
    assert(mds != NULL);
  }
};

class C_MDSContextNoop : public MDSContextBase
{
  virtual MDSRank* get_mds() { assert(0); }
public:
  void finish(int r) {}
};

/*
 * Gather needs a default-constructable class
 */
class MDSContextGather : public MDSContextBase
{
protected:
  MDSRank *get_mds();
};


class MDSGather : public C_GatherBase<MDSContextBase, MDSContextGather>
{
public:
  MDSGather(CephContext *cct, MDSContextBase *onfinish) :
    C_GatherBase<MDSContextBase, MDSContextGather>(cct, onfinish) {}
protected:
  virtual MDSRank *get_mds() {return NULL;}
};
typedef C_GatherBuilderBase<MDSContextBase, MDSGather> MDSGatherBuilder;

class MDSAsyncContextBase : public MDSContextBase
{
private:
  Finisher *finisher;
public:
  MDSAsyncContextBase() : finisher(NULL) {}
  void set_finisher(Finisher *f) { finisher = f; }
  void complete(int r) final;
};

class MDSLogContextBase : public MDSContextBase
{
private:
  Finisher *finisher;
  uint64_t write_pos;
public:
  MDSLogContextBase() : finisher(NULL), write_pos(0) {}
  void set_finisher(Finisher *f) { finisher = f; }
  void set_write_pos(uint64_t wp) { write_pos = wp; }
  void complete(int r) final;
};

#endif  // MDS_CONTEXT_H
