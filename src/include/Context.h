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
#include "include/assert.h"

#include <list>
#include <set>

#include <iostream>
#include "include/memory.h"

#define mydout(cct, v) lgeneric_subdout(cct, context, v)

/*
 * GenContext - abstract callback class
 */
template <typename T>
class GenContext {
  GenContext(const GenContext& other);
  const GenContext& operator=(const GenContext& other);

 protected:
  virtual void finish(T t) = 0;

 public:
  GenContext() {}
  virtual ~GenContext() {}       // we want a virtual destructor!!!
  virtual void complete(T t) {
    finish(t);
    delete this;
  }
};

/*
 * Context - abstract callback class
 */
class Context {
  Context(const Context& other);
  const Context& operator=(const Context& other);

 protected:
  virtual void finish(int r) = 0;

 public:
  Context() {}
  virtual ~Context() {}       // we want a virtual destructor!!!
  virtual void complete(int r) {
    finish(r);
    delete this;
  }
};

/**
 * Simple context holding a single object
 */
template<class T>
class ContainerContext : public Context {
  T obj;
public:
  ContainerContext(T &obj) : obj(obj) {}
  void finish(int r) {}
};

template <class T>
struct Wrapper : public Context {
  Context *to_run;
  T val;
  Wrapper(Context *to_run, T val) : to_run(to_run), val(val) {}
  void finish(int r) {
    if (to_run)
      to_run->complete(r);
  }
};
struct RunOnDelete {
  Context *to_run;
  RunOnDelete(Context *to_run) : to_run(to_run) {}
  ~RunOnDelete() {
    if (to_run)
      to_run->complete(0);
  }
};
typedef ceph::shared_ptr<RunOnDelete> RunOnDeleteRef;

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

  if (cct)
    mydout(cct, 10) << ls.size() << " contexts to finish with " << result << dendl;
  for (std::list<Context*>::iterator it = ls.begin(); 
       it != ls.end(); 
       it++) {
    Context *c = *it;
    if (cct)
      mydout(cct,10) << "---- " << c << dendl;
    c->complete(result);
  }
}

inline void finish_contexts(CephContext *cct, std::vector<Context*>& finished, 
                            int result = 0)
{
  if (finished.empty())
    return;

  vector<Context*> ls;
  ls.swap(finished); // swap out of place to avoid weird loops

  if (cct)
    mydout(cct,10) << ls.size() << " contexts to finish with " << result << dendl;
  for (std::vector<Context*>::iterator it = ls.begin(); 
       it != ls.end(); 
       it++) {
    Context *c = *it;
    if (cct)
      mydout(cct,10) << "---- " << c << dendl;
    c->complete(result);
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
  bool empty() { return contexts.empty(); }

  static Context *list_to_context(list<Context *> &cs) {
    if (cs.size() == 0) {
      return 0;
    } else if (cs.size() == 1) {
      Context *c = cs.front();
      cs.clear();
      return c;
    } else {
      C_Contexts *c(new C_Contexts(0));
      c->take(cs);
      return c;
    }
  }
};


/*
 * C_Gather
 *
 * BUG:? only reports error from last sub to have an error return
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
    mydout(cct,10) << "C_Gather " << this << ".sub_finish(r=" << r << ") " << sub
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
      onfinish->complete(result);
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
    mydout(cct,10) << "C_Gather " << this << ".new" << dendl;
  }
public:
  ~C_Gather() {
    mydout(cct,10) << "C_Gather " << this << ".delete" << dendl;
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
    mydout(cct,10) << "C_Gather " << this << ".new_sub is " << sub_created_count << " " << s << dendl;
    return s;
  }
  void finish(int r) {
    assert(0);    // nobody should ever call me.
  }
  friend class C_GatherBuilder;
};

/*
 * How to use C_GatherBuilder:
 *
 * 1. Create a C_GatherBuilder on the stack
 * 2. Call gather_bld.new_sub() as many times as you want to create new subs
 *    It is safe to call this 0 times, or 100, or anything in between.
 * 3. If you didn't supply a finisher in the C_GatherBuilder constructor,
 *    set one with gather_bld.set_finisher(my_finisher)
 * 4. Call gather_bld.activate()
 *
 * The finisher may be called at any point after step 4, including immediately
 * from the activate() function.
 * The finisher will never be called before activate().
 *
 * Note: Currently, subs must be manually freed by the caller (for some reason.)
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

#undef mydout

#endif
