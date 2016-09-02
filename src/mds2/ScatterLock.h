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


#ifndef CEPH_SCATTERLOCK_H
#define CEPH_SCATTERLOCK_H

#include "SimpleLock.h"
class LogSegment;

class ScatterLock : public SimpleLock {

  struct more_bits_t {
    int state_flags;
    utime_t last_scatter;
    xlist<ScatterLock*>::item item_updated;
    utime_t update_stamp;

    explicit more_bits_t(ScatterLock *lock) :
      state_flags(0),
      item_updated(lock)
    {}

    bool empty() const {
      return
	!state_flags &&
	!item_updated.is_on_list();
    }
  };
  more_bits_t *_more;

  bool have_more() const { return _more ? true : false; }
  void try_clear_more() {
    if (_more && _more->empty()) {
      delete _more;
      _more = NULL;
    }
  }
  more_bits_t *more() {
    if (!_more)
      _more = new more_bits_t(this);
    return _more;
  }

  enum flag_values {  // flag values for more_bits_t state
    SCATTER_WANTED   = 1 << 0,
    UNSCATTER_WANTED = 1 << 1,
    DIRTY            = 1 << 2,
    FLUSHING         = 1 << 3,
    FLUSHED          = 1 << 4,
  };

public:
  ScatterLock(CObject *o, LockType *lt) : 
    SimpleLock(o, lt), _more(NULL)
  {}
  ~ScatterLock() {
    if (_more) {
      assert(!_more->item_updated.is_on_list());
      delete _more;
    }
  }

  bool is_scatterlock() const {
    return true;
  }

  bool is_sync_and_unlocked() const {
    return
      SimpleLock::is_sync_and_unlocked() && 
      !is_dirty() &&
      !is_flushing();
  }

  bool can_scatter_pin(client_t loner) {
    /*
      LOCK : NOT okay because it can MIX and force replicas to journal something
      TSYN : also not okay for same reason
      EXCL : also not okay

      MIX  : okay, replica can stall before sending AC_SYNCACK
      SYNC : okay, replica can stall before sending AC_MIXACK or AC_LOCKACK
    */   
    return
      get_state() == LOCK_SYNC ||
      get_state() == LOCK_MIX;
  }

  xlist<ScatterLock*>::item *get_updated_item() { return &more()->item_updated; }

  utime_t get_update_stamp() {
    return more()->update_stamp;
  }

  void set_update_stamp(utime_t t) { more()->update_stamp = t; }

  void set_scatter_wanted() {
    more()->state_flags |= SCATTER_WANTED;
  }
  void set_unscatter_wanted() {
    more()->state_flags |= UNSCATTER_WANTED;
  }
  void clear_scatter_wanted() {
    if (have_more())
      _more->state_flags &= ~SCATTER_WANTED;
    try_clear_more();
  }
  void clear_unscatter_wanted() {
    if (have_more())
      _more->state_flags &= ~UNSCATTER_WANTED;
    try_clear_more();
  }
  bool get_scatter_wanted() const {
    return have_more() ? _more->state_flags & SCATTER_WANTED : false;
  }
  bool get_unscatter_wanted() const {
    return have_more() ? _more->state_flags & UNSCATTER_WANTED : false;
  }

  bool is_dirty() const {
    return have_more() ? _more->state_flags & DIRTY : false;
  }
  bool is_flushing() const {
    return have_more() ? _more->state_flags & FLUSHING: false;
  }
  bool is_flushed() const {
    return have_more() ? _more->state_flags & FLUSHED: false;
  }
  bool is_dirty_or_flushing() const {
    return have_more() ? (is_dirty() || is_flushing()) : false;
  }

  void mark_dirty(LogSegment *ls) { 
    if (!is_dirty()) {
      if (!is_flushing())
	parent->get(CObject::PIN_DIRTYSCATTERED);
      set_dirty();
    }
    if (ls)
      parent->mark_dirty_scattered(get_type(), ls);
  }
  void start_flush() {
    if (is_dirty()) {
      set_flushing();
      clear_dirty();
    }
  }
  void finish_flush() {
    if (is_flushing()) {
      clear_flushing();
      set_flushed();
      if (!is_dirty()) {
	parent->clear_dirty_scattered(get_type());
	parent->put(CObject::PIN_DIRTYSCATTERED);
      }
    }
  }
  void remove_dirty() {
    start_flush();
    finish_flush();
  }
  void clear_flushed() {
    if (have_more()) {
      _more->state_flags &= ~FLUSHED;
      try_clear_more();
    }
  }

  void set_last_scatter(utime_t t) { more()->last_scatter = t; }
  utime_t get_last_scatter() {
    return more()->last_scatter;
  }

  void infer_state_from_strong_rejoin(int rstate, bool locktoo) {
    if (rstate == LOCK_MIX || 
	rstate == LOCK_MIX_LOCK || // replica still has wrlocks?
	rstate == LOCK_MIX_SYNC || // "
	rstate == LOCK_MIX_TSYN)  // "
      state = LOCK_MIX;
    else if (locktoo && rstate == LOCK_LOCK)
      state = LOCK_LOCK;
  }

  virtual void print(ostream& out) const {
    out << "(";
    _print(out);
    if (is_dirty())
      out << " dirty";
    if (is_flushing())
      out << " flushing";
    if (is_flushed())
      out << " flushed";
    if (get_scatter_wanted())
      out << " scatter_wanted";
    out << ")";
  }

private:
  void set_flushing() {
    more()->state_flags |= FLUSHING;
  }
  void clear_flushing() {
    if (have_more()) {
      _more->state_flags &= ~FLUSHING;
    }
  }
  void set_flushed() {
    more()->state_flags |= FLUSHED;
  }
  void set_dirty() {
    more()->state_flags |= DIRTY;
  }
  void clear_dirty() {
    if (have_more()) {
      _more->state_flags &= ~DIRTY;
    }
  }
};

#endif
