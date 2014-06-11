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

#include <stdint.h>

#include "common/config.h"



class LRUObject {
 private:
  LRUObject *lru_next, *lru_prev;
  bool lru_pinned;
  class LRU *lru;
  class LRUList *lru_list;

 public:
  LRUObject() {
    lru_next = lru_prev = NULL;
    lru_list = 0;
    lru_pinned = false;
    lru = 0;
  }

  // pin/unpin item in cache
  void lru_pin(); 
  void lru_unpin();
  bool lru_is_expireable() { return !lru_pinned; }

  friend class LRU;
  friend class LRUList;
};


class LRUList {
 private:
  LRUObject *head, *tail;
  uint32_t len;

 public:
  LRUList() {
    head = tail = 0;
    len = 0;
  }
  
  uint32_t  get_length() { return len; }

  LRUObject *get_head() {
    return head;
  }
  LRUObject *get_tail() {
    return tail;
  }

  void clear() {
    while (len > 0) {
      remove(get_head());
    }
  }

  void insert_head(LRUObject *o) {
    o->lru_next = head;
    o->lru_prev = NULL;
    if (head) {
      head->lru_prev = o;
    } else {
      tail = o;
    }
    head = o;
    o->lru_list = this;
    len++;
  }
  void insert_tail(LRUObject *o) {
    o->lru_next = NULL;
    o->lru_prev = tail;
    if (tail) {
      tail->lru_next = o;
    } else {
      head = o;
    }
    tail = o;
    o->lru_list = this;
    len++;
  }

  void remove(LRUObject *o) {
    assert(o->lru_list == this);
    if (o->lru_next)
      o->lru_next->lru_prev = o->lru_prev;
    else
      tail = o->lru_prev;
    if (o->lru_prev)
      o->lru_prev->lru_next = o->lru_next;
    else
      head = o->lru_next;
    o->lru_next = o->lru_prev = NULL;
    o->lru_list = 0;
    assert(len>0);
    len--;
  }
  
};


class LRU {
 protected:
  LRUList lru_top, lru_bot, lru_pintail;
  uint32_t lru_num, lru_num_pinned;
  uint32_t lru_max;   // max items
  double lru_midpoint;

  friend class LRUObject;
  //friend class MDCache; // hack
  
 public:
  LRU(int max = 0) {
    lru_num = 0;
    lru_num_pinned = 0;
    lru_midpoint = .6;
    lru_max = max;
  }

  uint32_t lru_get_size() { return lru_num; }
  uint32_t lru_get_top() { return lru_top.get_length(); }
  uint32_t lru_get_bot() { return lru_bot.get_length(); }
  uint32_t lru_get_pintail() { return lru_pintail.get_length(); }
  uint32_t lru_get_max() { return lru_max; }
  uint32_t lru_get_num_pinned() { return lru_num_pinned; }

  void lru_set_max(uint32_t m) { lru_max = m; }
  void lru_set_midpoint(float f) { lru_midpoint = f; }
  
  void lru_clear() {
    lru_top.clear();
    lru_bot.clear();
    lru_pintail.clear();
    lru_num = 0;
  }

  // insert at top of lru
  void lru_insert_top(LRUObject *o) {
    //assert(!o->lru_in_lru);
    //o->lru_in_lru = true;
    assert(!o->lru);
    o->lru = this;
    lru_top.insert_head( o );
    lru_num++;
    if (o->lru_pinned) lru_num_pinned++;
    lru_adjust();
  }

  // insert at mid point in lru
  void lru_insert_mid(LRUObject *o) {
    //assert(!o->lru_in_lru);
    //o->lru_in_lru = true;
    assert(!o->lru);
    o->lru = this;
    lru_bot.insert_head(o);
    lru_num++;
    if (o->lru_pinned) lru_num_pinned++;
  }

  // insert at bottom of lru
  void lru_insert_bot(LRUObject *o) {
    assert(!o->lru);
    o->lru = this;
    lru_bot.insert_tail(o);
    lru_num++;
    if (o->lru_pinned) lru_num_pinned++;
  }

  /*
  // insert at bottom of lru
  void lru_insert_pintail(LRUObject *o) {
    assert(!o->lru);
    o->lru = this;
    
    assert(o->lru_pinned);

    lru_pintail.insert_head(o);
    lru_num++;
    lru_num_pinned += o->lru_pinned;
  }
  */

  


  // adjust top/bot balance, as necessary
  void lru_adjust() {
    if (!lru_max) return;

    unsigned toplen = lru_top.get_length();
    unsigned topwant = (unsigned)(lru_midpoint * ((double)lru_max - lru_num_pinned));
    while (toplen > 0 && 
           toplen > topwant) {
      // remove from tail of top, stick at head of bot
      // FIXME: this could be way more efficient by moving a whole chain of items.

      LRUObject *o = lru_top.get_tail();
      lru_top.remove(o);
      lru_bot.insert_head(o);
      toplen--;
    }
  }


  // remove an item
  LRUObject *lru_remove(LRUObject *o) {
    // not in list
    //assert(o->lru_in_lru);
    //if (!o->lru_in_lru) return o;  // might have expired and been removed that way.
    if (!o->lru) return o;

    assert((o->lru_list == &lru_pintail) ||
           (o->lru_list == &lru_top) ||
           (o->lru_list == &lru_bot));
    o->lru_list->remove(o);

    lru_num--;
    if (o->lru_pinned) lru_num_pinned--;
    o->lru = 0;
    return o;
  }

  // touch item -- move to head of lru
  bool lru_touch(LRUObject *o) {
    lru_remove(o);
    lru_insert_top(o);
    return true;
  }

  // touch item -- move to midpoint (unless already higher)
  bool lru_midtouch(LRUObject *o) {
    if (o->lru_list == &lru_top) return false;
    
    lru_remove(o);
    lru_insert_mid(o);
    return true;
  }

  // touch item -- move to bottom
  bool lru_bottouch(LRUObject *o) {
    lru_remove(o);
    lru_insert_bot(o);
    return true;
  }

  void lru_touch_entire_pintail() {
    // promote entire pintail to the top lru
    while (lru_pintail.get_length() > 0) {
      LRUObject *o = lru_pintail.get_head();
      lru_pintail.remove(o);
      lru_top.insert_tail(o);
    }
  }


  // expire -- expire a single item
  LRUObject *lru_get_next_expire() {
    LRUObject *p;
    
    // look through tail of bot
    while (lru_bot.get_length()) {
      p = lru_bot.get_tail();
      if (!p->lru_pinned) return p;

      // move to pintail
      lru_bot.remove(p);
      lru_pintail.insert_head(p);
    }

    // ok, try head then
    while (lru_top.get_length()) {
      p = lru_top.get_tail();
      if (!p->lru_pinned) return p;

      // move to pintail
      lru_top.remove(p);
      lru_pintail.insert_head(p);
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
    //generic_dout(10) << "lru: " << lru_num << " items, " << lru_top.get_length() << " top, " << lru_bot.get_length() << " bot, " << lru_pintail.get_length() << " pintail" << dendl;
  }

};


inline void LRUObject::lru_pin() {
  if (lru && !lru_pinned) {
    lru->lru_num_pinned++;
    lru->lru_adjust();
  }
  lru_pinned = true;
}

inline void LRUObject::lru_unpin() {
  if (lru && lru_pinned) {
    lru->lru_num_pinned--;

    // move from pintail -> bot
    if (lru_list == &lru->lru_pintail) {
      lru->lru_pintail.remove(this);
      lru->lru_bot.insert_tail(this);
    }
    lru->lru_adjust();
  }
  lru_pinned = false;
}

#endif
