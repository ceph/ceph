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
  std::set<int> waitfor;
  int num;
  bool any;  /* if true, OR, otherwise, AND */
  bool activated;

  bool sub_finish(void *sub, int num, int r) {
    assert(waitfor.count(num));
    waitfor.erase(num);

    //generic_dout(0) << "C_Gather " << this << ".sub_finish(r=" << r << ") " << sub << " " << num << " of " << waitfor << dendl;

    if (r < 0 && result == 0)
      result = r;

    if (!activated)
      return false;  // no finisher set yet, ignore.

    if (any && onfinish) {
      onfinish->finish(result);
      delete onfinish;
      onfinish = 0;
    }

    if (!waitfor.empty()) 
      return false;  // more subs left

    // last one
    if (!any && onfinish) {
      onfinish->finish(result);
      delete onfinish;
      onfinish = 0;
    }
    return true;
  }

  class C_GatherSub : public Context {
    C_Gather *gather;
    int num;
  public:
    C_GatherSub(C_Gather *g, int n) : gather(g), num(n) {}
    void finish(int r) {
      if (gather->sub_finish((void *)this, num, r)) {
	delete gather;   // last one!
	gather = 0;
      }
    }
    ~C_GatherSub() {
      if (gather)
	gather->rm_sub(num);
    }
  };

public:
  C_Gather(Context *f=0, bool an=false) : result(0), onfinish(f), num(0), any(an),
					  activated(onfinish ? true : false) {
    //generic_dout(0) << "C_Gather " << this << ".new" << dendl;
  }
  ~C_Gather() {
    //generic_dout(0) << "C_Gather " << this << ".delete" << dendl;
    assert(!onfinish);
  }

  void set_finisher(Context *c) {
    assert(!onfinish);
    onfinish = c;
    activated = true;
  }
  Context *new_sub() {
    num++;
    waitfor.insert(num);
    Context *s = new C_GatherSub(this, num);
    //generic_dout(0) << "C_Gather " << this << ".new_sub " << num << " " << s << dendl;
    return s;
  }
  void rm_sub(int n) {
    num--;
    waitfor.erase(n);
  }

  bool empty() { return num == 0; }
  int get_num() { return num; }

  void finish(int r) {
    assert(0);    // nobody should ever call me.
  }

};

#endif
