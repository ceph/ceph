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



#ifndef CEPH_LRU_H
#define CEPH_LRU_H

#include <math.h>
#include <stdint.h>

#include "common/config.h"
#include "xlist.h"

class LRUObject {
public:
  LRUObject() : lru_link(this) {}
  virtual ~LRUObject();

  // pin/unpin item in cache
  void lru_pin();
  void lru_unpin();
  bool lru_is_expireable() const { return !lru_pinned; }

  friend class LRU;
private:
  class LRU *lru{};
  xlist<LRUObject *>::item lru_link;
  bool lru_pinned = false;
};

class LRU {
public:
  uint64_t lru_get_size() const { return lru_get_top()+lru_get_bot()+lru_get_pintail(); }
  uint64_t lru_get_top() const { return top.size(); }
  uint64_t lru_get_bot() const{ return bottom.size(); }
  uint64_t lru_get_pintail() const { return pintail.size(); }
  uint64_t lru_get_num_pinned() const { return num_pinned; }

  void lru_set_midpoint(double f) { midpoint = fmin(1.0, fmax(0.0, f)); }
  
  void lru_clear() {
    while (!top.empty()) {
      lru_remove(top.front());
    }
    while (!bottom.empty()) {
      lru_remove(bottom.front());
    }
    while (!pintail.empty()) {
      lru_remove(pintail.front());
    }
    ceph_assert(num_pinned == 0);
  }

  // insert at top of lru
  void lru_insert_top(LRUObject *o) {
    ceph_assert(!o->lru);
    o->lru = this;
    top.push_front(&o->lru_link);
    if (o->lru_pinned) num_pinned++;
    adjust();
  }

  // insert at mid point in lru
  void lru_insert_mid(LRUObject *o) {
    ceph_assert(!o->lru);
    o->lru = this;
    bottom.push_front(&o->lru_link);
    if (o->lru_pinned) num_pinned++;
    adjust();
  }

  // insert at bottom of lru
  void lru_insert_bot(LRUObject *o) {
    ceph_assert(!o->lru);
    o->lru = this;
    bottom.push_back(&o->lru_link);
    if (o->lru_pinned) num_pinned++;
    adjust();
  }

  // remove an item
  LRUObject *lru_remove(LRUObject *o) {
    if (!o->lru) return o;
    auto list = o->lru_link.get_list();
    ceph_assert(list == &top || list == &bottom || list == &pintail);
    o->lru_link.remove_myself();
    if (o->lru_pinned) num_pinned--;
    o->lru = nullptr;
    adjust();
    return o;
  }

  // touch item -- move to head of lru
  bool lru_touch(LRUObject *o) {
    if (!o->lru) {
      lru_insert_top(o);
    } else {
      ceph_assert(o->lru == this);
      auto list = o->lru_link.get_list();
      ceph_assert(list == &top || list == &bottom || list == &pintail);
      top.push_front(&o->lru_link);
      adjust();
    }
    return true;
  }

  // touch item -- move to midpoint (unless already higher)
  bool lru_midtouch(LRUObject *o) {
    if (!o->lru) {
      lru_insert_mid(o);
    } else {
      ceph_assert(o->lru == this);
      auto list = o->lru_link.get_list();
      ceph_assert(list == &top || list == &bottom || list == &pintail);
      if (list == &top) return false;
      bottom.push_front(&o->lru_link);
      adjust();
    }
    return true;
  }

  // touch item -- move to bottom
  bool lru_bottouch(LRUObject *o) {
    if (!o->lru) {
      lru_insert_bot(o);
    } else {
      ceph_assert(o->lru == this);
      auto list = o->lru_link.get_list();
      ceph_assert(list == &top || list == &bottom || list == &pintail);
      bottom.push_back(&o->lru_link);
      adjust();
    }
    return true;
  }

  void lru_touch_entire_pintail() {
    // promote entire pintail to the top lru
    while (pintail.size() > 0) {
      top.push_back(&pintail.front()->lru_link);
      adjust();
    }
  }

  // expire -- expire a single item
  LRUObject *lru_get_next_expire() {
    adjust();
    // look through tail of bot
    while (bottom.size()) {
      LRUObject *p = bottom.back();
      if (!p->lru_pinned) return p;

      // move to pintail
      pintail.push_front(&p->lru_link);
    }

    // ok, try head then
    while (top.size()) {
      LRUObject *p = top.back();
      if (!p->lru_pinned) return p;

      // move to pintail
      pintail.push_front(&p->lru_link);
    }
    
    // no luck!
    return NULL;
  }
  
  LRUObject *lru_expire() {
    LRUObject *p = lru_get_next_expire();
    if (p) 
      return lru_remove(p);
    return NULL;
  }

  void lru_status() {
    //generic_dout(10) << "lru: " << lru_get_size() << " items, " << top.size() << " top, " << bottom.size() << " bot, " << pintail.size() << " pintail" << dendl;
  }

protected:
  // adjust top/bot balance, as necessary
  void adjust() {
    uint64_t toplen = top.size();
    uint64_t topwant = (midpoint * (double)(lru_get_size() - num_pinned));
    /* move items from below midpoint (bottom) to top: move midpoint forward */
    for (uint64_t i = toplen; i < topwant; i++) {
      top.push_back(&bottom.front()->lru_link);
    }
    /* or: move items from above midpoint (top) to bottom: move midpoint backwards */
    for (uint64_t i = toplen; i > topwant; i--) {
      bottom.push_front(&top.back()->lru_link);
    }
  }

  uint64_t num_pinned = 0;
  double midpoint = 0.6;

  friend class LRUObject;
private:
  using LRUList = xlist<LRUObject*>;
  LRUList top, bottom, pintail;
};

inline LRUObject::~LRUObject() {
  if (lru) {
    lru->lru_remove(this);
  }
}

inline void LRUObject::lru_pin() {
  if (lru && !lru_pinned) {
    lru->num_pinned++;
  }
  lru_pinned = true;
}

inline void LRUObject::lru_unpin() {
  if (lru && lru_pinned) {
    lru->num_pinned--;

    // move from pintail -> bot
    if (lru_link.get_list() == &lru->pintail) {
      lru->lru_bottouch(this);
    }
  }
  lru_pinned = false;
}

#endif
