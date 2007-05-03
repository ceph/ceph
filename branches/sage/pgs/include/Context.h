// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __CONTEXT_H
#define __CONTEXT_H

#include "config.h"

#include <assert.h>
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
  using std::cout;
  using std::endl;
  
  if (finished.empty()) return;

  dout(10) << finished.size() << " contexts to finish with " << result << endl;
  for (std::list<Context*>::iterator it = finished.begin(); 
       it != finished.end(); 
       it++) {
    Context *c = *it;
    dout(10) << "---- " << c << endl;
    c->finish(result);
    delete c;
  }
}

/*
 * C_Contexts - set of Contexts
 */
class C_Contexts : public Context {
  std::list<Context*> clist;
  
public:
  void add(Context* c) {
    clist.push_back(c);
  }
  void take(std::list<Context*>& ls) {
    clist.splice(clist.end(), ls);
  }
  void finish(int r) {
    finish_contexts(clist, r);
  }
};


/*
 * C_Gather
 *
 * BUG: does not report errors.
 */
class C_Gather : public Context {
public:
  bool sub_finish(int r) {
    //cout << "C_Gather sub_finish " << this << endl;
    assert(waitfor.count(r));
    waitfor.erase(r);
    if (!waitfor.empty()) 
      return false;  // more subs left

    // last one
    onfinish->finish(0);
    delete onfinish;
    onfinish = 0;
    return true;
  }

  class C_GatherSub : public Context {
    C_Gather *gather;
    int num;
  public:
    C_GatherSub(C_Gather *g, int n) : gather(g), num(n) {}
    void finish(int r) {
      if (gather->sub_finish(num))
	delete gather;   // last one!
    }
  };

  Context *new_sub() {
    num++;
    waitfor.insert(num);
    return new C_GatherSub(this, num);
  }

private:
  Context *onfinish;
  std::set<int> waitfor;
  int num;

public:
  C_Gather(Context *f) : onfinish(f), num(0) {
    //cout << "C_Gather new " << this << endl;
  }
  ~C_Gather() {
    //cout << "C_Gather delete " << this << endl;
    assert(!onfinish);
  }
  void finish(int r) {
    // nobody should ever call me.
    assert(0);
  }

};

#endif
