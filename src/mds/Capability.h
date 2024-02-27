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


#ifndef CEPH_CAPABILITY_H
#define CEPH_CAPABILITY_H

#include "include/buffer_fwd.h"
#include "include/counter.h"
#include "include/mempool.h"
#include "include/xlist.h"
#include "include/elist.h"

#include "common/config.h"

#include "mdstypes.h"


/*

  Capability protocol notes.

- two types of cap events from mds -> client:
  - cap "issue" in a MClientReply, or an MClientCaps IMPORT op.
  - cap "update" (revocation or grant) .. an MClientCaps message.
- if client has cap, the mds should have it too.

- if client has no dirty data, it can release it without waiting for an mds ack.
  - client may thus get a cap _update_ and not have the cap.  ignore it.

- mds should track seq of last issue.  any release
  attempt will only succeed if the client has seen the latest.

- a UPDATE updates the clients issued caps, wanted, etc.  it may also flush dirty metadata.
  - 'caps' are which caps the client retains.
    - if 0, client wishes to release the cap
  - 'wanted' is which caps the client wants.
  - 'dirty' is which metadata is to be written.
    - client gets a FLUSH_ACK with matching dirty flags indicating which caps were written.

- a FLUSH_ACK acks a FLUSH.
  - 'dirty' is the _original_ FLUSH's dirty (i.e., which metadata was written back)
  - 'seq' is the _original_ FLUSH's seq.
  - 'caps' is the _original_ FLUSH's caps (not actually important)
  - client can conclude that (dirty & ~caps) bits were successfully cleaned.

- a FLUSHSNAP flushes snapshot metadata.
  - 'dirty' indicates which caps, were dirty, if any.
  - mds writes metadata.  if dirty!=0, replies with FLUSHSNAP_ACK.

 */

class CInode;
class Session;
class MDLockCache;

namespace ceph {
  class Formatter;
}

class Capability : public Counter<Capability> {
public:
  MEMPOOL_CLASS_HELPERS();

  struct Export {
    Export() {}
    Export(int64_t id, int w, int i, int p, snapid_t cf,
	   ceph_seq_t s, ceph_seq_t m, utime_t lis, unsigned st) :
      cap_id(id), wanted(w), issued(i), pending(p), client_follows(cf),
      seq(s), mseq(m), last_issue_stamp(lis), state(st) {}
    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &p);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<Export*>& ls);

    int64_t cap_id = 0;
    int32_t wanted = 0;
    int32_t issued = 0;
    int32_t pending = 0;
    snapid_t client_follows;
    ceph_seq_t seq = 0;
    ceph_seq_t mseq = 0;
    utime_t last_issue_stamp;
    uint32_t state = 0;
  };
  struct Import {
    Import() {}
    Import(int64_t i, ceph_seq_t s, ceph_seq_t m) : cap_id(i), issue_seq(s), mseq(m) {}
    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &p);
    void dump(ceph::Formatter *f) const;

    int64_t cap_id = 0;
    ceph_seq_t issue_seq = 0;
    ceph_seq_t mseq = 0;
  };
  struct revoke_info {
    revoke_info() {}
    revoke_info(__u32 b, ceph_seq_t s, ceph_seq_t li) : before(b), seq(s), last_issue(li) {}
    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<revoke_info*>& ls);

    __u32 before = 0;
    ceph_seq_t seq = 0;
    ceph_seq_t last_issue = 0;
  };

  const static unsigned STATE_NOTABLE		= (1<<0);
  const static unsigned STATE_NEW		= (1<<1);
  const static unsigned STATE_IMPORTING		= (1<<2);
  const static unsigned STATE_NEEDSNAPFLUSH	= (1<<3);
  const static unsigned STATE_CLIENTWRITEABLE	= (1<<4);
  const static unsigned STATE_NOINLINE		= (1<<5);
  const static unsigned STATE_NOPOOLNS		= (1<<6);
  const static unsigned STATE_NOQUOTA		= (1<<7);

  const static unsigned MASK_STATE_EXPORTED =
    (STATE_CLIENTWRITEABLE | STATE_NOINLINE | STATE_NOPOOLNS | STATE_NOQUOTA);

  Capability(CInode *i=nullptr, Session *s=nullptr, uint64_t id=0);
  Capability(const Capability& other) = delete;

  const Capability& operator=(const Capability& other) = delete;

  int pending() const {
    return _pending;
  }
  int issued() const {
    return _issued;
  }
  int revoking() const {
    return _issued & ~_pending;
  }
  ceph_seq_t issue(unsigned c, bool reval=false) {
    if (reval)
      revalidate();

    if (_pending & ~c) {
      // revoking (and maybe adding) bits.  note caps prior to this revocation
      _revokes.emplace_back(_pending, last_sent, last_issue);
      _pending = c;
      _issued |= c;
      if (!is_notable())
	mark_notable();
    } else if (~_pending & c) {
      // adding bits only.  remove obsolete revocations?
      _pending |= c;
      _issued |= c;
      // drop old _revokes with no bits we don't have
      while (!_revokes.empty() &&
	     (_revokes.back().before & ~_pending) == 0)
	_revokes.pop_back();
    } else {
      // no change.
      ceph_assert(_pending == c);
    }
    //last_issue = 
    inc_last_seq();
    return last_sent;
  }
  ceph_seq_t issue_norevoke(unsigned c, bool reval=false) {
    if (reval)
      revalidate();

    _pending |= c;
    _issued |= c;
    clear_new();

    inc_last_seq();
    return last_sent;
  }
  int confirm_receipt(ceph_seq_t seq, unsigned caps);
  // we may get a release racing with revocations, which means our revokes will be ignored
  // by the client.  clean them out of our _revokes history so we don't wait on them.
  void clean_revoke_from(ceph_seq_t li) {
    bool changed = false;
    while (!_revokes.empty() && _revokes.front().last_issue <= li) {
      _revokes.pop_front();
      changed = true;
    }
    if (changed) {
      bool was_revoking = (_issued & ~_pending);
      calc_issued();
      if (was_revoking && _issued == _pending) {
	item_revoking_caps.remove_myself();
	item_client_revoking_caps.remove_myself();
	maybe_clear_notable();
      }
    }
  }
  ceph_seq_t get_mseq() const { return mseq; }
  void inc_mseq() { mseq++; }

  utime_t get_last_issue_stamp() const { return last_issue_stamp; }
  utime_t get_last_revoke_stamp() const { return last_revoke_stamp; }

  void set_last_issue() { last_issue = last_sent; }
  void set_last_issue_stamp(utime_t t) { last_issue_stamp = t; }
  void set_last_revoke_stamp(utime_t t) { last_revoke_stamp = t; }
  void reset_num_revoke_warnings() { num_revoke_warnings = 0; }
  void inc_num_revoke_warnings() { ++num_revoke_warnings; }
  unsigned get_num_revoke_warnings() const { return num_revoke_warnings; }

  void set_cap_id(uint64_t i) { cap_id = i; }
  uint64_t get_cap_id() const { return cap_id; }

  //ceph_seq_t get_last_issue() { return last_issue; }

  bool is_suppress() const { return suppress > 0; }
  void inc_suppress() { suppress++; }
  void dec_suppress() { suppress--; }

  static bool is_wanted_notable(int wanted) {
    return wanted & (CEPH_CAP_ANY_WR|CEPH_CAP_FILE_WR|CEPH_CAP_FILE_RD);
  }
  bool is_wanted_notable() const {
    return is_wanted_notable(wanted());
  }
  bool is_notable() const { return state & STATE_NOTABLE; }

  bool is_stale() const;
  bool is_valid() const;
  bool is_new() const { return state & STATE_NEW; }
  void mark_new() { state |= STATE_NEW; }
  void clear_new() { state &= ~STATE_NEW; }
  bool is_importing() const { return state & STATE_IMPORTING; }
  void mark_importing() { state |= STATE_IMPORTING; }
  void clear_importing() { state &= ~STATE_IMPORTING; }
  bool need_snapflush() const { return state & STATE_NEEDSNAPFLUSH; }
  void mark_needsnapflush() { state |= STATE_NEEDSNAPFLUSH; }
  void clear_needsnapflush() { state &= ~STATE_NEEDSNAPFLUSH; }

  bool is_clientwriteable() const { return state & STATE_CLIENTWRITEABLE; }
  void mark_clientwriteable() {
    if (!is_clientwriteable()) {
      state |= STATE_CLIENTWRITEABLE;
      if (!is_notable())
	mark_notable();
    }
  }
  void clear_clientwriteable() {
    if (is_clientwriteable()) {
      state &= ~STATE_CLIENTWRITEABLE;
      maybe_clear_notable();
    }
  }

  bool is_noinline() const { return state & STATE_NOINLINE; }
  bool is_nopoolns() const { return state & STATE_NOPOOLNS; }
  bool is_noquota() const { return state & STATE_NOQUOTA; }

  CInode *get_inode() const { return inode; }
  Session *get_session() const { return session; }
  client_t get_client() const;

  // caps this client wants to hold
  int wanted() const { return _wanted; }
  void set_wanted(int w);

  void inc_last_seq() { last_sent++; }
  ceph_seq_t get_last_seq() const {
    return last_sent;
  }
  ceph_seq_t get_last_issue() const { return last_issue; }

  void reset_seq() {
    last_sent = 0;
    last_issue = 0;
  }
  
  // -- exports --
  Export make_export() const {
    return Export(cap_id, wanted(), issued(), pending(), client_follows, get_last_seq(), mseq+1, last_issue_stamp, state);
  }
  void merge(const Export& other, bool auth_cap) {
    // issued + pending
    int newpending = other.pending | pending();
    if (other.issued & ~newpending)
      issue(other.issued | newpending);
    else
      issue(newpending);
    last_issue_stamp = other.last_issue_stamp;

    client_follows = other.client_follows;

    state |= other.state & MASK_STATE_EXPORTED;
    if ((other.state & STATE_CLIENTWRITEABLE) && !is_notable())
      mark_notable();

    // wanted
    set_wanted(wanted() | other.wanted);
    if (auth_cap)
      mseq = other.mseq;
  }
  void merge(int otherwanted, int otherissued) {
    // issued + pending
    int newpending = pending();
    if (otherissued & ~newpending)
      issue(otherissued | newpending);
    else
      issue(newpending);

    // wanted
    set_wanted(wanted() | otherwanted);
  }

  int revoke() {
    if (revoking())
      return confirm_receipt(last_sent, pending());
    return 0;
  }

  // serializers
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<Capability*>& ls);

  snapid_t client_follows = 0;
  version_t client_xattr_version = 0;
  version_t client_inline_version = 0;
  int64_t last_rbytes = 0;
  int64_t last_rsize = 0;

  xlist<Capability*>::item item_session_caps;
  xlist<Capability*>::item item_snaprealm_caps;
  xlist<Capability*>::item item_revoking_caps;
  xlist<Capability*>::item item_client_revoking_caps;

  elist<MDLockCache*> lock_caches;
  int get_lock_cache_allowed() const { return lock_cache_allowed; }
  void set_lock_cache_allowed(int c) { lock_cache_allowed |= c; }
  void clear_lock_cache_allowed(int c) { lock_cache_allowed &= ~c; }

private:
  void calc_issued() {
    _issued = _pending;
    for (const auto &r : _revokes) {
      _issued |= r.before;
    }
  }

  void revalidate();

  void mark_notable();
  void maybe_clear_notable();

  CInode *inode;
  Session *session;

  uint64_t cap_id;
  uint32_t cap_gen;

  __u32 _wanted = 0;     // what the client wants (ideally)

  utime_t last_issue_stamp;
  utime_t last_revoke_stamp;
  unsigned num_revoke_warnings = 0;

  // track in-flight caps --------------
  //  - add new caps to _pending
  //  - track revocations in _revokes list
  __u32 _pending = 0, _issued = 0;
  mempool::mds_co::list<revoke_info> _revokes;

  ceph_seq_t last_sent = 0;
  ceph_seq_t last_issue = 0;
  ceph_seq_t mseq = 0;

  int suppress = 0;
  uint32_t state = 0;

  int lock_cache_allowed = 0;
};

WRITE_CLASS_ENCODER(Capability::Export)
WRITE_CLASS_ENCODER(Capability::Import)
WRITE_CLASS_ENCODER(Capability::revoke_info)
WRITE_CLASS_ENCODER(Capability)



#endif
