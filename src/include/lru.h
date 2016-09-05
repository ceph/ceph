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
#include "include/Spinlock.h"

#include <atomic>

class LRUObject {
 private:
  const static unsigned STATE_LOCKED = 1;
  const static unsigned STATE_PINNED = 2;
  std::atomic<unsigned> lru_state;

  void lru_set_pinned() {
    std::atomic_fetch_or(&lru_state, STATE_PINNED);
  }
  void lru_clear_pinned() {
    std::atomic_fetch_and(&lru_state, ~STATE_PINNED);
  }
  bool lru_is_pinned() const {
    return std::atomic_load(&lru_state) & STATE_PINNED;
  }

  // synchronize LRUObject::{lru_pin,lru_unpin} with LRU::{lru_insert_xxx,lru_remove}
  bool lru_trylock() {
    return !(std::atomic_fetch_or(&lru_state, STATE_LOCKED) & STATE_LOCKED);
  }
  void lru_lock() {
    while(!lru_trylock());
  }
  void lru_unlock() {
    unsigned old_state = std::atomic_fetch_and(&lru_state, ~STATE_LOCKED);
    assert(old_state & STATE_LOCKED);
  }

  LRUObject *lru_next, *lru_prev;
  class LRU *lru;
  class LRUList *lru_list;
 public:
  LRUObject() : lru_state(ATOMIC_VAR_INIT(0)) {
    lru_next = lru_prev = NULL;
    lru_list = 0;
    lru = 0;
  }

  // pin/unpin item in cache
  void lru_pin(); 
  void lru_unpin();
  bool lru_is_expireable() const { return !lru_is_pinned(); }

  friend class LRU;
  friend class LRUList;
};


class LRUList {
 private:
  LRUObject *head, *tail;
  std::atomic<uint32_t> len;

 public:
  LRUList() : len(ATOMIC_VAR_INIT(0)) {
    head = tail = 0;
  }
  
  uint32_t get_length() { return std::atomic_load(&len); }

  LRUObject *get_head() {
    return head;
  }
  LRUObject *get_tail() {
    return tail;
  }

  void clear() {
    while (get_length() > 0) {
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
    std::atomic_fetch_add(&len, 1U);
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
    std::atomic_fetch_add(&len, 1U);
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
    uint32_t old_len = std::atomic_fetch_sub(&len, 1U);
    assert(old_len > 0);
  }
};


class LRU {
 private:
  static const unsigned MAX_OPS_LOCKED = 64;
  Spinlock lock;

  LRU(const LRU& other);
 protected:
  LRUList lru_top, lru_bot, lru_pintail;
  std::atomic<uint32_t> lru_num, lru_num_pinned;
  uint32_t lru_max;   // max items
  double lru_midpoint;

  friend class LRUObject;
  //friend class MDCache; // hack

  // insert at top of lru
  void _lru_insert_top(LRUObject *o) {
    Spinlock::Locker l(lock);
    //assert(!o->lru_in_lru);
    //o->lru_in_lru = true;
    if (o->lru == this) {
      o->lru_unlock();
      return;
    }
    assert(!o->lru);
    o->lru = this;
    lru_top.insert_head(o);
    std::atomic_fetch_add(&lru_num, 1U);
    if (o->lru_is_pinned())
      std::atomic_fetch_add(&lru_num_pinned, 1U);
    o->lru_unlock();
    _lru_adjust();
  }

  // insert at mid point in lru
  void _lru_insert_mid(LRUObject *o) {
    Spinlock::Locker l(lock);
    //assert(!o->lru_in_lru);
    //o->lru_in_lru = true;
    assert(!o->lru);
    o->lru = this;
    lru_bot.insert_head(o);
    std::atomic_fetch_add(&lru_num, 1U);
    if (o->lru_is_pinned())
      std::atomic_fetch_add(&lru_num_pinned, 1U);
    o->lru_unlock();
  }

  // insert at bottom of lru
  void _lru_insert_bot(LRUObject *o) {
    Spinlock::Locker l(lock);
    assert(!o->lru);
    o->lru = this;
    lru_bot.insert_tail(o);
    std::atomic_fetch_add(&lru_num, 1U);
    if (o->lru_is_pinned())
      std::atomic_fetch_add(&lru_num_pinned, 1U);
    o->lru_unlock();
  }

  // remove an item
  LRUObject *_lru_remove(LRUObject *o) {
    Spinlock::Locker l(lock);
    assert(o->lru);
    assert((o->lru_list == &lru_pintail) ||
	   (o->lru_list == &lru_top) ||
	   (o->lru_list == &lru_bot));
    o->lru_list->remove(o);

    uint32_t old_num;
    old_num = std::atomic_fetch_sub(&lru_num, 1U);
    assert(old_num > 0);
    if (o->lru_is_pinned()) {
      old_num = std::atomic_fetch_sub(&lru_num_pinned, 1U);
      assert(old_num > 0);
    }
    o->lru = 0;
    o->lru_unlock();
    return o;
  }

  // adjust top/bot balance, as necessary
  void _lru_adjust() {
    unsigned nr = 0;
    if (!lru_max)
      return;

    unsigned top_want;
    unsigned num_pinned = std::atomic_load(&lru_num_pinned);
    if (lru_max > num_pinned)
      top_want = (unsigned)(lru_midpoint * (lru_max - num_pinned));
    else
      top_want = 0;
    while (lru_top.get_length() > top_want) {
      // remove from tail of top, stick at head of bot
      // FIXME: this could be way more efficient by moving a whole chain of items.

      LRUObject *o = lru_top.get_tail();
      lru_top.remove(o);
      lru_bot.insert_head(o);
      if ((++nr % MAX_OPS_LOCKED) == 0) {
	lock.unlock();
	lock.lock();
	unsigned num_pinned = std::atomic_load(&lru_num_pinned);
	if (lru_max > num_pinned)
	  top_want = (unsigned)(lru_midpoint * (lru_max - num_pinned));
	else
	  top_want = 0;
      }
    }
  }

  LRUObject *_lru_get_next_expire(bool remove) {
    LRUObject *p;

  retry:
    unsigned nr = 0;
    // look through tail of bot
    while (lru_bot.get_length()) {
      p = lru_bot.get_tail();
      if (!p->lru_is_pinned()) goto out;

      // move to pintail
      lru_bot.remove(p);
      lru_pintail.insert_head(p);

      if ((++nr % MAX_OPS_LOCKED) == 0) {
	lock.unlock();
	lock.lock();
      }
    }

    // ok, try head then
    while (lru_top.get_length()) {
      p = lru_top.get_tail();
      if (!p->lru_is_pinned()) goto out;

      // move to pintail
      lru_top.remove(p);
      lru_pintail.insert_head(p);

      if ((++nr % MAX_OPS_LOCKED) == 0) {
	lock.unlock();
	lock.lock();
      }
    }
    // no luck!
    p = NULL;
  out:
    if (p && remove) { 
      if (p->lru_trylock()) {
	p->lru_list->remove(p);
	uint32_t old_num = std::atomic_fetch_sub(&lru_num, 1U);
	assert(old_num > 0);
	p->lru = 0;
	p->lru_unlock();
      } else {
	lock.unlock();
	lock.lock();
	goto retry;
      }
    }
    return p;
  }

 public:
  LRU(int max = 0) : lru_num(ATOMIC_VAR_INIT(0)), lru_num_pinned(ATOMIC_VAR_INIT(0)) {
    lru_midpoint = .6;
    lru_max = max;
  }

  uint32_t lru_get_size() { return std::atomic_load(&lru_num); }
  uint32_t lru_get_top() { return lru_top.get_length(); }
  uint32_t lru_get_bot() { return lru_bot.get_length(); }
  uint32_t lru_get_pintail() { return lru_pintail.get_length(); }
  uint32_t lru_get_max() { return lru_max; }
  uint32_t lru_get_num_pinned() { return std::atomic_load(&lru_num_pinned); }

  void lru_set_max(uint32_t m) {
    Spinlock::Locker l(lock);
    lru_max = m;
  }
  void lru_set_midpoint(float f) {
    Spinlock::Locker l(lock);
    lru_midpoint = f;
  }
  
  void lru_clear() {
    Spinlock::Locker l(lock);
    lru_top.clear();
    lru_bot.clear();
    lru_pintail.clear();
    std::atomic_store(&lru_num, 0U);
    std::atomic_store(&lru_num_pinned, 0U);
  }

  void lru_insert_top(LRUObject *o) {
    o->lru_lock();
    if (!o->lru)
      _lru_insert_top(o);
    else
      o->lru_unlock();
  }

  void lru_insert_mid(LRUObject *o) {
    o->lru_lock();
    if (!o->lru)
      _lru_insert_mid(o);
    else
      o->lru_unlock();
  }

  void lru_insert_bot(LRUObject *o) {
    o->lru_lock();
    if (!o->lru)
      _lru_insert_bot(o);
    else
      o->lru_unlock();
  }

  LRUObject *lru_remove(LRUObject *o) {
    o->lru_lock();
    if (o->lru) {
      return _lru_remove(o);;
    } else {
      o->lru_unlock();
      return o;
    }
  }

  // touch item -- move to head of lru
  bool lru_touch(LRUObject *o) {
    o->lru_lock();
    if (o->lru) {
      Spinlock::Locker l(lock);
      o->lru_list->remove(o);
      lru_top.insert_head(o);
      o->lru_unlock();
      _lru_adjust();
    } else {
      _lru_insert_top(o);
    }
    return true;
  }

  // touch item -- move to midpoint (unless already higher)
  bool lru_midtouch(LRUObject *o) {
    o->lru_lock();
    if (o->lru) {
      Spinlock::Locker l(lock);
      if (o->lru_list == &lru_top) {
	o->lru_unlock();
	return false;
      }
      o->lru_list->remove(o);
      lru_bot.insert_head(o);
      o->lru_unlock();
    } else {
      _lru_insert_mid(o);
    }
    return true;
  }

  // touch item -- move to bottom
  bool lru_bottouch(LRUObject *o) {
    o->lru_lock();
    if (o->lru) {
      Spinlock::Locker l(lock);
      o->lru_list->remove(o);
      lru_bot.insert_tail(o);
      o->lru_unlock();
    } else {
      _lru_insert_bot(o);
    }
    return true;
  }

  void lru_touch_entire_pintail() {
    unsigned nr = 0;
    lock.lock();
    // promote entire pintail to the top lru
    while (lru_pintail.get_length() > 0) {
      LRUObject *o = lru_pintail.get_head();
      lru_pintail.remove(o);
      lru_top.insert_tail(o);
      if ((++nr % MAX_OPS_LOCKED) == 0) {
	lock.unlock();
	lock.lock();
      }
    }
    lock.unlock();
  }

  LRUObject *lru_get_next_expire() {
    Spinlock::Locker l(lock);
    return _lru_get_next_expire(false);
  }

  // expire -- expire a single item
  LRUObject *lru_expire() {
    Spinlock::Locker l(lock);
    return _lru_get_next_expire(true);
  }


  void lru_status() {
    //generic_dout(10) << "lru: " << lru_num << " items, " << lru_top.get_length() << " top, " << lru_bot.get_length() << " bot, " << lru_pintail.get_length() << " pintail" << dendl;
  }

};


inline void LRUObject::lru_pin() {
  lru_lock();
  if (lru) {
    if (!lru_is_pinned()) {
      Spinlock::Locker l(lru->lock);
      lru_set_pinned();
      std::atomic_fetch_add(&lru->lru_num_pinned, 1U);
      lru_unlock();
      lru->_lru_adjust();
      return;
    }
  } else {
    lru_set_pinned();
  }
  lru_unlock();
}

inline void LRUObject::lru_unpin() {
  lru_lock();
  if (lru) {
    if (lru_is_pinned()) {
      Spinlock::Locker l(lru->lock);
      lru_clear_pinned();
      uint32_t old_num = std::atomic_fetch_sub(&lru->lru_num_pinned, 1U);
      assert(old_num > 0);

      // move from pintail -> bot
      if (lru_list == &lru->lru_pintail) {
	lru->lru_pintail.remove(this);
	lru->lru_bot.insert_tail(this);
      }
      lru_unlock();
      lru->_lru_adjust();
      return;
    }
  } else {
    lru_clear_pinned();
  }
  lru_unlock();
}

#endif
