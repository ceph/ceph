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

#include "config.h"

#include "assert.h"
#include <list>
#include <set>

#include <iostream>


/*
 * Context - abstract callback class
 */
class Context {
 public:
  virtual ~Context() {}       // we want a virtual destructor!!!
  virtual void finish(int r) = 0;
};


/*
 * finish and destroy a list of Contexts
 */
inline void finish_contexts(std::list<Context*>& finished, 
                            int result = 0)
{
  if (finished.empty())
    return;

  list<Context*> ls;
  ls.swap(finished); // swap out of place to avoid weird loops

  generic_dout(10) << ls.size() << " contexts to finish with " << result << dendl;
  for (std::list<Context*>::iterator it = ls.begin(); 
       it != ls.end(); 
       it++) {
    Context *c = *it;
    generic_dout(10) << "---- " << c << dendl;
    c->finish(result);
    delete c;
  }
}

inline void finish_contexts(std::vector<Context*>& finished, 
                            int result = 0)
{
  if (finished.empty())
    return;

  vector<Context*> ls;
  ls.swap(finished); // swap out of place to avoid weird loops

  generic_dout(10) << ls.size() << " contexts to finish with " << result << dendl;
  for (std::vector<Context*>::iterator it = ls.begin(); 
       it != ls.end(); 
       it++) {
    Context *c = *it;
    generic_dout(10) << "---- " << c << dendl;
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
  std::list<Context*> contexts;

  void add(Context* c) {
    contexts.push_back(c);
  }
  void take(std::list<Context*>& ls) {
    contexts.splice(contexts.end(), ls);
  }
  void finish(int r) {
    finish_contexts(contexts, r);
  }
};




/*
 * C_Gather
 *
 * BUG: does not report errors.
 */
class C_Gather : public Context {
private:
  int result;
  Context *onfinish;
#ifdef DEBUG_GATHER
  std::set<Context*> waitfor;
#endif
  int sub_created_count;
  int sub_existing_count;
  Mutex lock;
  bool any;  /* if true, OR, otherwise, AND */
  bool activated;

  bool sub_finish(Context* sub, int r) {
    Mutex::Locker l(lock);
#ifdef DEBUG_GATHER
    assert(waitfor.count(sub));
    waitfor.erase(sub);
#endif
    --sub_existing_count;

    //generic_dout(0) << "C_Gather " << this << ".sub_finish(r=" << r << ") " << sub << " " << dendl;

    if (r < 0 && result == 0)
      result = r;

    if (!activated)
      return false;  // no finisher set yet, ignore.

    if (any && onfinish) {
      lock.Unlock();
      onfinish->finish(result);
      lock.Lock();
      delete onfinish;
      onfinish = 0;
    }

    if (sub_existing_count)
      return false;  // more subs left

    // last one
    if (!any && onfinish) {
      lock.Unlock();
      onfinish->finish(result);
      lock.Lock();
      delete onfinish;
      onfinish = 0;
    }
    return true;
  }

  class C_GatherSub : public Context {
    C_Gather *gather;
  public:
    C_GatherSub(C_Gather *g) : gather(g) {}
    void finish(int r) {
      if (gather->sub_finish(this, r))
	delete gather;   // last one!
      gather = 0;
    }
    ~C_GatherSub() {
      if (gather)
	gather->rm_sub(this);
    }
  };

public:
  C_Gather(Context *f=0, bool an=false) : result(0), onfinish(f), sub_created_count(0),
                                          sub_existing_count(0),
                                          lock("C_Gather::lock", true, false), //disable lockdep
                                          any(an),
					  activated(onfinish ? true : false) {
    //generic_dout(0) << "C_Gather " << this << ".new" << dendl;
  }
  ~C_Gather() {
    //generic_dout(0) << "C_Gather " << this << ".delete" << dendl;
    assert(sub_existing_count == 0);
#ifdef DEBUG_GATHER
    assert(waitfor.empty());
#endif
    assert(!onfinish);
  }

  void set_finisher(Context *c) {
    Mutex::Locker l(lock);
    assert(!onfinish);
    onfinish = c;
    activated = true;
  }
  Context *new_sub() {
    Mutex::Locker l(lock);
    sub_created_count++;
    sub_existing_count++;
    Context *s = new C_GatherSub(this);
#ifdef DEBUG_GATHER
    waitfor.insert(s);
#endif
    //generic_dout(0) << "C_Gather " << this << ".new_sub is " << sub_created_count << " " << s << dendl;
    return s;
  }
  void rm_sub(Context *s) {
    Mutex::Locker l(lock);
#ifdef DEBUG_GATHER
    assert(waitfor.count(s));
    waitfor.erase(s);
#endif
    sub_existing_count--;
  }

  bool empty() { Mutex::Locker l(lock); return sub_existing_count == 0; }
  int get_num() { Mutex::Locker l(lock); return sub_created_count; }

  void finish(int r) {
    assert(0);    // nobody should ever call me.
  }

};

#endif
