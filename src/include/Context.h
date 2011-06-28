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


#ifndef CEPH_CONTEXT_H
#define CEPH_CONTEXT_H

#include "common/dout.h"

#include "assert.h"
#include <list>
#include <set>

#include <iostream>

#define DOUT_SUBSYS context

/*
 * Context - abstract callback class
 */
class Context {
 public:
  Context(const Context& other);
  const Context& operator=(const Context& other);

  Context() {}
  virtual ~Context() {}       // we want a virtual destructor!!!
  virtual void finish(int r) = 0;
};


/*
 * finish and destroy a list of Contexts
 */
inline void finish_contexts(CephContext *cct, std::list<Context*>& finished, 
                            int result = 0)
{
  if (finished.empty())
    return;

  list<Context*> ls;
  ls.swap(finished); // swap out of place to avoid weird loops

  ldout(cct,10) << ls.size() << " contexts to finish with " << result << dendl;
  for (std::list<Context*>::iterator it = ls.begin(); 
       it != ls.end(); 
       it++) {
    Context *c = *it;
    ldout(cct,10) << "---- " << c << dendl;
    c->finish(result);
    delete c;
  }
}

inline void finish_contexts(CephContext *cct, std::vector<Context*>& finished, 
                            int result = 0)
{
  if (finished.empty())
    return;

  vector<Context*> ls;
  ls.swap(finished); // swap out of place to avoid weird loops

  ldout(cct,10) << ls.size() << " contexts to finish with " << result << dendl;
  for (std::vector<Context*>::iterator it = ls.begin(); 
       it != ls.end(); 
       it++) {
    Context *c = *it;
    ldout(cct,10) << "---- " << c << dendl;
    c->finish(result);
    delete c;
  }
}

class C_NoopContext : public Context {
public:
  void finish(int r) { }
};


/*
 * C_Contexts - set of Contexts
 */
class C_Contexts : public Context {
public:
  CephContext *cct;
  std::list<Context*> contexts;

  C_Contexts(CephContext *cct_)
    : cct(cct_)
  {
  }

  void add(Context* c) {
    contexts.push_back(c);
  }
  void take(std::list<Context*>& ls) {
    contexts.splice(contexts.end(), ls);
  }
  void finish(int r) {
    finish_contexts(cct, contexts, r);
  }
};




/*
 * C_Gather
 *
 * BUG: does not report errors.
 */
class C_Gather : public Context {
private:
  CephContext *cct;
  int result;
  Context *onfinish;
#ifdef DEBUG_GATHER
  std::set<Context*> waitfor;
#endif
  int sub_created_count;
  int sub_existing_count;
  Mutex lock;
  bool activated;

  void sub_finish(Context* sub, int r) {
    lock.Lock();
#ifdef DEBUG_GATHER
    assert(waitfor.count(sub));
    waitfor.erase(sub);
#endif
    --sub_existing_count;
    ldout(cct,10) << "C_Gather " << this << ".sub_finish(r=" << r << ") " << sub
#ifdef DEBUG_GATHER
		    << " (remaining " << waitfor << ")"
#endif
		    << dendl;
    if (r < 0 && result == 0)
      result = r;
    if ((activated == false) || (sub_existing_count != 0)) {
      lock.Unlock();
      return;
    }
    lock.Unlock();
    delete_me();
  }

  void delete_me() {
    if (onfinish) {
      onfinish->finish(result);
      delete onfinish;
      onfinish = 0;
    }
    delete this;
  }

  class C_GatherSub : public Context {
    C_Gather *gather;
  public:
    C_GatherSub(C_Gather *g) : gather(g) {}
    void finish(int r) {
      gather->sub_finish(this, r);
      gather = 0;
    }
    ~C_GatherSub() {
      if (gather)
	gather->sub_finish(this, 0);
    }
  };

  C_Gather(CephContext *cct_, Context *onfinish_)
    : cct(cct_), result(0), onfinish(onfinish_),
      sub_created_count(0), sub_existing_count(0),
      lock("C_Gather::lock", true, false), //disable lockdep
      activated(false)
  {
    ldout(cct,10) << "C_Gather " << this << ".new" << dendl;
  }
public:
  ~C_Gather() {
    ldout(cct,10) << "C_Gather " << this << ".delete" << dendl;
  }
  void set_finisher(Context *onfinish_) {
    Mutex::Locker l(lock);
    assert(!onfinish);
    onfinish = onfinish_;
  }
  void activate() {
    lock.Lock();
    assert(activated == false);
    activated = true;
    if (sub_existing_count != 0) {
      lock.Unlock();
      return;
    }
    lock.Unlock();
    delete_me();
  }
  Context *new_sub() {
    Mutex::Locker l(lock);
    assert(activated == false);
    sub_created_count++;
    sub_existing_count++;
    Context *s = new C_GatherSub(this);
#ifdef DEBUG_GATHER
    waitfor.insert(s);
#endif
    ldout(cct,10) << "C_Gather " << this << ".new_sub is " << sub_created_count << " " << s << dendl;
    return s;
  }
  void finish(int r) {
    assert(0);    // nobody should ever call me.
  }
  friend class C_GatherBuilder;
};

/*
 * This is a class designed to help you construct CGather objects.
 *
 * You construct a C_GatherBuilder on the stack, and call new_sub as many or as
 * few times as you need. If you call new_sub 0 times, there is no need for a
 * CGather object, so none is created.
 *
 * C_Gather objects must have a finisher context. You can provide the finisher
 * in the constructor for C_GatherBuilder, or set it later with set_finisher.
 * If you forget, you will get an assert.
 *
 * If a C_Gather object is created, it will be destroyed when the last sub
 * finishes.
 */
class C_GatherBuilder
{
public:
  C_GatherBuilder(CephContext *cct_)
    : cct(cct_), c_gather(NULL), finisher(NULL), activated(false)
  {
  }
  C_GatherBuilder(CephContext *cct_, Context *finisher_)
    : cct(cct_), c_gather(NULL), finisher(finisher_), activated(false)
  {
  }
  ~C_GatherBuilder() {
    if (c_gather) {
      assert(activated); // Don't forget to activate your C_Gather!
    }
    else {
      delete finisher;
    }
  }
  Context *new_sub() {
    if (!c_gather) {
      c_gather = new C_Gather(cct, finisher);
    }
    return c_gather->new_sub();
  }
  void activate() {
    if (!c_gather)
      return;
    assert(finisher != NULL);
    activated = true;
    c_gather->activate();
  }
  void set_finisher(Context *finisher_) {
    finisher = finisher_;
    if (c_gather)
      c_gather->set_finisher(finisher);
  }
  C_Gather *get() const {
    return c_gather;
  }
  bool has_subs() const {
    return (c_gather != NULL);
  }
  int num_subs_created() {
    assert(!activated);
    if (c_gather == NULL)
      return 0;
    Mutex::Locker l(c_gather->lock); 
    return c_gather->sub_created_count;
  }
  int num_subs_remaining() {
    assert(!activated);
    if (c_gather == NULL)
      return 0;
    Mutex::Locker l(c_gather->lock);
    return c_gather->sub_existing_count;
  }

private:
  CephContext *cct;
  C_Gather *c_gather;
  Context *finisher;
  bool activated;
};

#undef DOUT_SUBSYS

#endif
