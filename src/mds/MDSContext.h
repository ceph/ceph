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
#include "include/elist.h"
#include "common/ceph_time.h"
#include "common/simple_spin.h"

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


/**
 * A context which must be called with the big MDS lock held.  Subclass
 * this with a get_mds implementation.
 */
class MDSInternalContextBase : public MDSContext
{
public:
    void complete(int r) override;
};

/**
 * General purpose, lets you pass in an MDS pointer.
 */
class MDSInternalContext : public MDSInternalContextBase
{
protected:
  MDSRank *mds;
  MDSRank* get_mds() override;

public:
  explicit MDSInternalContext(MDSRank *mds_) : mds(mds_) {
    assert(mds != NULL);
  }
};

/**
 * Wrap a regular Context up as an Internal context. Useful
 * if you're trying to work with one of our more generic frameworks.
 */
class MDSInternalContextWrapper : public MDSInternalContextBase
{
protected:
  MDSRank *mds;
  Context *fin;
  MDSRank *get_mds() override;
  void finish(int r) override;
public:
  MDSInternalContextWrapper(MDSRank *m, Context *c) : mds(m), fin(c) {}
};

class MDSIOContextBase : public MDSContext
{
public:
  MDSIOContextBase(bool track=true);
  virtual ~MDSIOContextBase();
  MDSIOContextBase(const MDSIOContextBase&) = delete;
  MDSIOContextBase& operator=(const MDSIOContextBase&) = delete;

  void complete(int r) override;

  virtual void print(ostream& out) const = 0;

  static bool check_ios_in_flight(ceph::coarse_mono_time cutoff,
				  std::string& slow_count,
				  ceph::coarse_mono_time& oldest);
private:
  ceph::coarse_mono_time created_at;
  elist<MDSIOContextBase*>::item list_item;

  static elist<MDSIOContextBase*> ctx_list;
  static std::atomic_flag ctx_list_lock;
};

/**
 * Completion for an log operation, takes big MDSRank lock
 * before executing finish function. Update log's safe pos
 * after finish functuon return.
 */
class MDSLogContextBase : public MDSIOContextBase
{
protected:
  uint64_t write_pos;
public:
  MDSLogContextBase() : write_pos(0) {}
  void complete(int r) final;
  void set_write_pos(uint64_t wp) { write_pos = wp; }
  virtual void pre_finish(int r) {}
  void print(ostream& out) const override {
    out << "log_event(" << write_pos << ")";
  }
};

/**
 * Completion for an I/O operation, takes big MDSRank lock
 * before executing finish function.
 */
class MDSIOContext : public MDSIOContextBase
{
protected:
  MDSRank *mds;
  MDSRank* get_mds() override;

public:
  explicit MDSIOContext(MDSRank *mds_) : mds(mds_) {
    assert(mds != NULL);
  }
};

/**
 * Wrap a regular Context up as an IO Context. Useful
 * if you're trying to work with one of our more generic frameworks.
 */
class MDSIOContextWrapper : public MDSIOContextBase
{
protected:
  MDSRank *mds;
  Context *fin;
  MDSRank *get_mds() override;
public:
  MDSIOContextWrapper(MDSRank *m, Context *c) : mds(m), fin(c) {}
  void finish(int r) override;
  void print(ostream& out) const override {
    out << "io_context_wrapper(" << fin << ")";
  }
};

/**
 * No-op for callers expecting MDSInternalContextBase
 */
class C_MDSInternalNoop final : public MDSInternalContextBase
{
  MDSRank* get_mds() override {ceph_abort();}
public:
  void finish(int r) override {}
  void complete(int r) override { delete this; }
};


/**
 * This class is used where you have an MDSInternalContextBase but
 * you sometimes want to call it back from an I/O completion.
 */
class C_IO_Wrapper : public MDSIOContext
{
protected:
  bool async;
  MDSInternalContextBase *wrapped;
  void finish(int r) override {
    wrapped->complete(r);
    wrapped = nullptr;
  }
public:
  C_IO_Wrapper(MDSRank *mds_, MDSInternalContextBase *wrapped_) :
    MDSIOContext(mds_), async(true), wrapped(wrapped_) {
    assert(wrapped != NULL);
  }

  ~C_IO_Wrapper() override {
    if (wrapped != nullptr) {
      delete wrapped;
      wrapped = nullptr;
    }
  }
  void complete(int r) final;
  void print(ostream& out) const override {
    out << "io_wrapper(" << wrapped << ")";
  }
};


/**
 * Gather needs a default-constructable class
 */
class MDSInternalContextGather : public MDSInternalContextBase
{
protected:
  MDSRank *get_mds() override;
};


class MDSGather : public C_GatherBase<MDSInternalContextBase, MDSInternalContextGather>
{
public:
  MDSGather(CephContext *cct, MDSInternalContextBase *onfinish) : C_GatherBase<MDSInternalContextBase, MDSInternalContextGather>(cct, onfinish) {}
protected:
  MDSRank *get_mds() override {return NULL;}
};


typedef C_GatherBuilderBase<MDSInternalContextBase, MDSGather> MDSGatherBuilder;

#endif  // MDS_CONTEXT_H
