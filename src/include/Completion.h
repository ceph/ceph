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


#ifndef __COMPLETION_H
#define __COMPLETION_H


struct Completion : public Context {
public:
  virtual ~Completion() {}
  virtual void complete(int r) = 0;
};


inline void complete(Completion *c, int result = 0)
{
  c->complete(result);
}

template<class T>
inline void complete(T& c, int result = 0)
{
  if (c.empty())
    return;

  // swap out of place to avoid weird loops
  T ls;
  ls.swap(c); 

  generic_dout(10) << ls.size() << " completions to complete with " << result << dendl;
  for (typename T::iterator it = ls.begin(); it != ls.end(); it++) {
    Completion *c = *it;
    generic_dout(10) << "---- " << c << dendl;
    c->complete(result);
  }
}

struct C_Noop : public Completion {
  void complete(int r) {
    delete this;
  }
};


struct C_ScatterList : public Completion {
  std::list<Completion*> completions;

  void add(Completion* c) {
    completions.push_back(c);
  }
  void take(std::list<Completion*>& ls) {
    completions.splice(completions.end(), ls);
  }
  void finish(int r) {
    ::complete(completions, r);
    delete this;
  }
};


class C_GatherSet : public Completion {
private:
  int result;
  Completion *onfinish;
  Mutex lock;
  int num;
  bool all;

public:
  C_GatherSet(Completion *f=0, bool a=true) : result(0), onfinish(f),
					      lock("C_GatherSet::lock"), num(0), all(a) {
    //cout << "C_GatherSet new " << this << endl;
  }
  ~C_GatherSet() {
    //cout << "C_GatherSet delete " << this << endl;
    assert(!onfinish);
  }

  void set_finisher(Completion *c) {
    assert(!onfinish);
    onfinish = c;
  }

  Completion *new_sub() {
    Mutex::Locker l(lock);
    num++;
    return this;
  }

  void rm_sub() {
    Mutex::Locker l(lock);
    num--;
  }

  bool empty() { return num == 0; }
  int get_num() { return num; }

  void finish(int r) {
    lock.Lock();
    assert(num > 0);
    num--;

    if (r < 0 && result == 0)
      result = r;

    Context *f = 0;
    int final = 0;
    if (onfinish && (!all || (all && num == 0))) {
      final = result;
      f = onfinish;
      onfinish = 0;
    }

    bool last = !num;
    lock.Unlock();

    if (f)
      f->complete(final);

    if (last)
      delete this;
  }

};

#endif
