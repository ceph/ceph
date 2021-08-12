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

#include "MDSContext.h"

class ScatterLock : public SimpleLock {
public:
  ScatterLock(MDSCacheObject *o, LockType *lt) :
    SimpleLock(o, lt) {}
  ~ScatterLock() override {
    ceph_assert(!_more);
  }

  bool is_scatterlock() const override {
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

  void set_xlock_snap_sync(MDSContext *c)
  {
    ceph_assert(get_type() == CEPH_LOCK_IFILE);
    ceph_assert(state == LOCK_XLOCK || state == LOCK_XLOCKDONE);
    state = LOCK_XLOCKSNAP;
    add_waiter(WAIT_STABLE, c);
  }

  xlist<ScatterLock*>::item *get_updated_item() { return &more()->item_updated; }

  utime_t get_update_stamp() {
    return _more ? _more->update_stamp : utime_t();
  }

  void set_update_stamp(utime_t t) { more()->update_stamp = t; }

  void set_scatter_wanted() {
    state_flags |= SCATTER_WANTED;
  }
  void set_unscatter_wanted() {
    state_flags |= UNSCATTER_WANTED;
  }
  void clear_scatter_wanted() {
    state_flags &= ~SCATTER_WANTED;
  }
  void clear_unscatter_wanted() {
    state_flags &= ~UNSCATTER_WANTED;
  }
  bool get_scatter_wanted() const {
    return state_flags & SCATTER_WANTED;
  }
  bool get_unscatter_wanted() const {
    return state_flags & UNSCATTER_WANTED;
  }

  bool is_dirty() const override {
    return state_flags & DIRTY;
  }
  bool is_flushing() const override {
    return state_flags & FLUSHING;
  }
  bool is_flushed() const override {
    return state_flags & FLUSHED;
  }
  bool is_dirty_or_flushing() const {
    return is_dirty() || is_flushing();
  }

  void mark_dirty() { 
    if (!is_dirty()) {
      if (!is_flushing())
	parent->get(MDSCacheObject::PIN_DIRTYSCATTERED);
      set_dirty();
    }
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
	parent->put(MDSCacheObject::PIN_DIRTYSCATTERED);
	parent->clear_dirty_scattered(get_type());
      }
    }
  }
  void clear_flushed() override {
    state_flags &= ~FLUSHED;
  }
  void remove_dirty() {
    start_flush();
    finish_flush();
    clear_flushed();
  }

  void infer_state_from_strong_rejoin(int rstate, bool locktoo) {
    if (rstate == LOCK_MIX || 
	rstate == LOCK_MIX_LOCK || // replica still has wrlocks?
	rstate == LOCK_MIX_SYNC)
      state = LOCK_MIX;
    else if (locktoo && rstate == LOCK_LOCK)
      state = LOCK_LOCK;
  }

  void encode_state_for_rejoin(ceph::buffer::list& bl, int rep) {
    __s16 s = get_replica_state();
    if (is_gathering(rep)) {
      // the recovering mds may hold rejoined wrlocks
      if (state == LOCK_MIX_SYNC)
	s = LOCK_MIX_SYNC;
      else
	s = LOCK_MIX_LOCK;
    } else if (s == LOCK_MIX) {
      remove_lazy_scatter(rep);
    }

    // If there is a recovering mds who replcated an object when it failed
    // and scatterlock in the object was in MIX state, It's possible that
    // the recovering mds needs to take wrlock on the scatterlock when it
    // replays unsafe requests. So this mds should delay taking rdlock on
    // the scatterlock until the recovering mds finishes replaying unsafe.
    // Otherwise unsafe requests may get replayed after current request.
    //
    // For example:
    // The recovering mds is auth mds of a dirfrag, this mds is auth mds
    // of corresponding inode. when 'rm -rf' the direcotry, this mds should
    // delay the rmdir request until the recovering mds has replayed unlink
    // requests.
    if (s == LOCK_MIX || s == LOCK_MIX_LOCK || s == LOCK_MIX_SYNC)
      mark_need_recover();

    ceph::encode(s, bl);
  }

  void decode_state_rejoin(ceph::buffer::list::const_iterator& p, MDSContext::vec& waiters, bool survivor) {
    SimpleLock::decode_state_rejoin(p, waiters, survivor);
    if (is_flushing()) {
      set_dirty();
      clear_flushing();
    }
  }

  void add_lazy_scatter(int i) {
    more()->lazy_set.insert(i);
  }
  bool remove_lazy_scatter(int i) {
    if (_more && _more->lazy_set.erase(i)) {
      if (_more->empty())
	_more.reset();
      return true;
    }
    return false;
  }
  void clear_lazy_scatter() {
    if (_more) {
      _more->lazy_set.clear();
      if (_more->empty())
	_more.reset();
    }
  }

  void encode(ceph::buffer::list& bl) const {
    SimpleLock::encode(bl);
    if (_more)
      ceph::encode(_more->lazy_set, bl);
    else
      ceph::encode(std::set<int32_t>{}, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    SimpleLock::decode(p);
    std::set<int32_t> l;
    ceph::decode(l, p);
    if (l.empty())
      clear_lazy_scatter();
    else
      more()->lazy_set.swap(l);
  }

  void export_twiddle() {
    clear_lazy_scatter();
    SimpleLock::export_twiddle();
  }

  bool remove_replica(int from, bool rejoin) {
    if (rejoin &&
	(state == LOCK_MIX ||
	 state == LOCK_MIX_SYNC ||
	 state == LOCK_MIX_LOCK2 ||
	 state == LOCK_MIX_TSYN ||
	 state == LOCK_MIX_EXCL))
      return false;

    remove_lazy_scatter(from);

    return SimpleLock::remove_replica(from);
  }

  void print(std::ostream& out) const override {
    out << "(";
    _print(out);
    if (_more && !_more->lazy_set.empty())
      out << " l=" << _more->lazy_set;
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
  struct more_bits_t {
    xlist<ScatterLock*>::item item_updated;
    utime_t update_stamp;

    std::set<__s32> lazy_set;

    bool empty() const {
      return !item_updated.is_on_list() && lazy_set.empty();
    }

    explicit more_bits_t(ScatterLock *lock) :
      item_updated(lock)
    {}
  };

  more_bits_t *more() {
    if (!_more)
      _more.reset(new more_bits_t(this));
    return _more.get();
  }

  enum {
    SCATTER_WANTED   = 1 << 8,
    UNSCATTER_WANTED = 1 << 9,
    DIRTY            = 1 << 10,
    FLUSHING         = 1 << 11,
    FLUSHED          = 1 << 12,
  };

  void set_flushing() {
    state_flags |= FLUSHING;
  }
  void clear_flushing() {
    state_flags &= ~FLUSHING;
  }
  void set_flushed() {
    state_flags |= FLUSHED;
  }
  void set_dirty() {
    state_flags |= DIRTY;
  }
  void clear_dirty() {
    state_flags &= ~DIRTY;
    if (_more) {
      _more->item_updated.remove_myself();
      if (_more->empty())
	_more.reset();
    }
  }

  mutable std::unique_ptr<more_bits_t> _more;
};
WRITE_CLASS_ENCODER(ScatterLock)

#endif
