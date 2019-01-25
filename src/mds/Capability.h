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

namespace ceph {
  class Formatter;
}

class Capability : public Counter<Capability> {
public:
  MEMPOOL_CLASS_HELPERS();

  struct Export {
    int64_t cap_id;
    int32_t wanted;
    int32_t issued;
    int32_t pending;
    snapid_t client_follows;
    ceph_seq_t seq;
    ceph_seq_t mseq;
    utime_t last_issue_stamp;
    Export() : cap_id(0), wanted(0), issued(0), pending(0), seq(0), mseq(0) {}
    Export(int64_t id, int w, int i, int p, snapid_t cf, ceph_seq_t s, ceph_seq_t m, utime_t lis) :
      cap_id(id), wanted(w), issued(i), pending(p), client_follows(cf),
      seq(s), mseq(m), last_issue_stamp(lis) {}
    void encode(bufferlist &bl) const;
    void decode(bufferlist::const_iterator &p);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Export*>& ls);
  };
  struct Import {
    int64_t cap_id;
    ceph_seq_t issue_seq;
    ceph_seq_t mseq;
    Import() : cap_id(0), issue_seq(0), mseq(0) {}
    Import(int64_t i, ceph_seq_t s, ceph_seq_t m) : cap_id(i), issue_seq(s), mseq(m) {}
    void encode(bufferlist &bl) const;
    void decode(bufferlist::const_iterator &p);
    void dump(Formatter *f) const;
  };
  struct revoke_info {
    __u32 before;
    ceph_seq_t seq, last_issue;
    revoke_info() : before(0), seq(0), last_issue(0) {}
    revoke_info(__u32 b, ceph_seq_t s, ceph_seq_t li) : before(b), seq(s), last_issue(li) {}
    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<revoke_info*>& ls);
  };

  const static unsigned STATE_NOTABLE		= (1<<0);
  const static unsigned STATE_NEW		= (1<<1);
  const static unsigned STATE_IMPORTING		= (1<<2);
  const static unsigned STATE_NEEDSNAPFLUSH	= (1<<3);
  const static unsigned STATE_CLIENTWRITEABLE	= (1<<4);

  Capability(CInode *i=nullptr, Session *s=nullptr, uint64_t id=0);
  Capability(const Capability& other) = delete;

  const Capability& operator=(const Capability& other) = delete;

  int pending() const {
    return is_valid() ? _pending : (_pending & CEPH_CAP_PIN);
  }
  int issued() const {
    return is_valid() ? _issued : (_issued & CEPH_CAP_PIN);
  }

  ceph_seq_t issue(unsigned c) {
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
  ceph_seq_t issue_norevoke(unsigned c) {
    revalidate();

    _pending |= c;
    _issued |= c;
    //check_rdcaps_list();
    inc_last_seq();
    return last_sent;
  }
  void confirm_receipt(ceph_seq_t seq, unsigned caps) {
    bool was_revoking = (_issued & ~_pending);
    if (seq == last_sent) {
      _revokes.clear();
      _issued = caps;
      // don't add bits
      _pending &= caps;
    } else {
      // can i forget any revocations?
      while (!_revokes.empty() && _revokes.front().seq < seq)
	_revokes.pop_front();
      if (!_revokes.empty()) {
	if (_revokes.front().seq == seq)
	  _revokes.begin()->before = caps;
	calc_issued();
      } else {
	// seq < last_sent
	_issued = caps | _pending;
      }
    }

    if (was_revoking && _issued == _pending) {
      item_revoking_caps.remove_myself();
      item_client_revoking_caps.remove_myself();
      maybe_clear_notable();
    }
    //check_rdcaps_list();
  }
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
  bool is_notable() const { return state & STATE_NOTABLE; }

  bool is_stale() const;
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

  CInode *get_inode() const { return inode; }
  Session *get_session() const { return session; }
  client_t get_client() const;

  // caps this client wants to hold
  int wanted() const { return _wanted; }
  void set_wanted(int w);

  void inc_last_seq() { last_sent++; }
  ceph_seq_t get_last_seq() const {
    if (!is_valid() && (_pending & ~CEPH_CAP_PIN))
      return last_sent + 1;
    return last_sent;
  }
  ceph_seq_t get_last_issue() const { return last_issue; }

  void reset_seq() {
    last_sent = 0;
    last_issue = 0;
  }
  
  // -- exports --
  Export make_export() const {
    return Export(cap_id, wanted(), issued(), pending(), client_follows, get_last_seq(), mseq+1, last_issue_stamp);
  }
  void merge(const Export& other, bool auth_cap) {
    if (!is_stale()) {
      // issued + pending
      int newpending = other.pending | pending();
      if (other.issued & ~newpending)
	issue(other.issued | newpending);
      else
	issue(newpending);
      last_issue_stamp = other.last_issue_stamp;
    } else {
      issue(CEPH_CAP_PIN);
    }

    client_follows = other.client_follows;

    // wanted
    set_wanted(wanted() | other.wanted);
    if (auth_cap)
      mseq = other.mseq;
  }
  void merge(int otherwanted, int otherissued) {
    if (!is_stale()) {
      // issued + pending
      int newpending = pending();
      if (otherissued & ~newpending)
	issue(otherissued | newpending);
      else
	issue(newpending);
    } else {
      issue(CEPH_CAP_PIN);
    }

    // wanted
    set_wanted(wanted() | otherwanted);
  }

  void revoke() {
    if (pending() & ~CEPH_CAP_PIN)
      issue(CEPH_CAP_PIN);
    confirm_receipt(last_sent, CEPH_CAP_PIN);
  }

  // serializers
  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<Capability*>& ls);
  
  snapid_t client_follows;
  version_t client_xattr_version;
  version_t client_inline_version;
  int64_t last_rbytes;
  int64_t last_rsize;

  xlist<Capability*>::item item_session_caps;
  xlist<Capability*>::item item_snaprealm_caps;
  xlist<Capability*>::item item_revoking_caps;
  xlist<Capability*>::item item_client_revoking_caps;

private:
  CInode *inode;
  Session *session;

  uint64_t cap_id;
  uint32_t cap_gen;

  __u32 _wanted;     // what the client wants (ideally)

  utime_t last_issue_stamp;
  utime_t last_revoke_stamp;
  unsigned num_revoke_warnings;

  // track in-flight caps --------------
  //  - add new caps to _pending
  //  - track revocations in _revokes list
  __u32 _pending, _issued;
  mempool::mds_co::list<revoke_info> _revokes;

  ceph_seq_t last_sent;
  ceph_seq_t last_issue;
  ceph_seq_t mseq;

  int suppress;
  unsigned state;

  void calc_issued() {
    _issued = _pending;
    for (const auto &r : _revokes) {
      _issued |= r.before;
    }
  }

  bool is_valid() const;
  void revalidate();

  void mark_notable();
  void maybe_clear_notable();
};

WRITE_CLASS_ENCODER(Capability::Export)
WRITE_CLASS_ENCODER(Capability::Import)
WRITE_CLASS_ENCODER(Capability::revoke_info)
WRITE_CLASS_ENCODER(Capability)



#endif
