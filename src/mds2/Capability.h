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
#include "include/xlist.h"

#include "common/config.h"

#include "mds/mdstypes.h"

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

namespace ceph {
  class Formatter;
}

class Capability {
private:
  static boost::pool<> pool;
public:
  static void *operator new(size_t num_bytes) { 
    void *n = pool.malloc();
    if (!n)
      throw std::bad_alloc();
    return n;
  }
  void operator delete(void *p) {
    pool.free(p);
  }
public:
  struct Export {
    int64_t cap_id;
    int32_t wanted;
    int32_t issued;
    int32_t pending;
    snapid_t client_follows;
    ceph_seq_t seq;
    ceph_seq_t mseq;
    utime_t last_issue_stamp;
    Export() {}
    Export(int64_t id, int w, int i, int p, snapid_t cf, ceph_seq_t s, ceph_seq_t m, utime_t lis) :
      cap_id(id), wanted(w), issued(i), pending(p), client_follows(cf),
      seq(s), mseq(m), last_issue_stamp(lis) {}
    void encode(bufferlist &bl) const;
    void decode(bufferlist::iterator &p);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<Export*>& ls);
  };
  struct Import {
    int64_t cap_id;
    ceph_seq_t issue_seq;
    ceph_seq_t mseq;
    Import() {}
    Import(int64_t i, ceph_seq_t s, ceph_seq_t m) : cap_id(i), issue_seq(s), mseq(m) {}
    void encode(bufferlist &bl) const;
    void decode(bufferlist::iterator &p);
    void dump(Formatter *f) const;
  };

private:
  CInode *inode;
  client_t client;

  uint64_t cap_id;

  __u32 _wanted;     // what the client wants (ideally)

  utime_t last_issue_stamp;
  utime_t last_revoke_stamp;
  unsigned num_revoke_warnings;

  // track in-flight caps --------------
  //  - add new caps to _pending
  //  - track revocations in _revokes list
public:
  struct revoke_info {
    __u32 before;
    ceph_seq_t seq, last_issue;
    revoke_info() {}
    revoke_info(__u32 b, ceph_seq_t s, ceph_seq_t li) : before(b), seq(s), last_issue(li) {}
    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& bl);
    void dump(Formatter *f) const;
    static void generate_test_instances(list<revoke_info*>& ls);
  };
private:
  __u32 _pending, _issued;
  list<revoke_info> _revokes;

public:
  int pending() { return _pending; }
  int issued() { return _issued; }
  bool is_null() { return !_pending && _revokes.empty(); }

  ceph_seq_t issue(unsigned c) {
    if (_pending & ~c) {
      // revoking (and maybe adding) bits.  note caps prior to this revocation
      _revokes.push_back(revoke_info(_pending, last_sent, last_issue));
      _pending = c;
      _issued |= c;
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
      assert(_pending == c);
    }
    //last_issue = 
    ++last_sent;
    return last_sent;
  }
  ceph_seq_t issue_norevoke(unsigned c) {
    _pending |= c;
    _issued |= c;
    //check_rdcaps_list();
    ++last_sent;
    return last_sent;
  }
  void _calc_issued() {
    _issued = _pending;
    for (list<revoke_info>::iterator p = _revokes.begin(); p != _revokes.end(); ++p)
      _issued |= p->before;
  }
  void confirm_receipt(ceph_seq_t seq, unsigned caps) {
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
	_calc_issued();
      } else {
	// seq < last_sent
	_issued = caps | _pending;
      }
    }

    if (_issued == _pending) {
      item_revoking_caps.remove_myself();
      item_client_revoking_caps.remove_myself();
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
      _calc_issued();
      if (_issued == _pending) {
	item_revoking_caps.remove_myself();
	item_client_revoking_caps.remove_myself();
      }
    }
  }


private:
  ceph_seq_t last_sent;
  ceph_seq_t last_issue;
  ceph_seq_t mseq;

  int suppress;
  unsigned state;

  const static unsigned STATE_STALE		= (1<<0);
  const static unsigned STATE_NEW		= (1<<1);

public:
  snapid_t client_follows;
  version_t client_xattr_version;
  version_t client_inline_version;
  int64_t last_rbytes;
  int64_t last_rsize;

  xlist<Capability*>::item item_session_caps;
  xlist<Capability*>::item item_snaprealm_caps;
  xlist<Capability*>::item item_revoking_caps;
  xlist<Capability*>::item item_client_revoking_caps;

  Capability(CInode *i = NULL, uint64_t id = 0, client_t c = 0) : 
    inode(i), client(c),
    cap_id(id),
    _wanted(0), num_revoke_warnings(0),
    _pending(0), _issued(0),
    last_sent(0),
    last_issue(0),
    mseq(0),
    suppress(0), state(0),
    client_follows(0), client_xattr_version(0),
    client_inline_version(0),
    last_rbytes(0), last_rsize(0),
    item_session_caps(this), item_snaprealm_caps(this),
    item_revoking_caps(this), item_client_revoking_caps(this) {
    g_num_cap++;
    g_num_capa++;
  }
  ~Capability() {
    g_num_cap--;
    g_num_caps++;
  }

  Capability(const Capability& other);  // no copying
  const Capability& operator=(const Capability& other);  // no copying
  
  ceph_seq_t get_mseq() { return mseq; }
  void inc_mseq() { mseq++; }

  ceph_seq_t get_last_sent() { return last_sent; }
  utime_t get_last_issue_stamp() { return last_issue_stamp; }
  utime_t get_last_revoke_stamp() { return last_revoke_stamp; }

  void set_last_issue() { last_issue = last_sent; }
  void set_last_issue_stamp(utime_t t) { last_issue_stamp = t; }
  void set_last_revoke_stamp(utime_t t) { last_revoke_stamp = t; }
  void reset_num_revoke_warnings() { num_revoke_warnings = 0; }
  void inc_num_revoke_warnings() { ++num_revoke_warnings; }
  unsigned get_num_revoke_warnings() { return num_revoke_warnings; }

  void set_cap_id(uint64_t i) { cap_id = i; }
  uint64_t get_cap_id() { return cap_id; }

  //ceph_seq_t get_last_issue() { return last_issue; }

  bool is_suppress() { return suppress > 0; }
  void inc_suppress() { suppress++; }
  void dec_suppress() { suppress--; }

  bool is_stale() { return state & STATE_STALE; }
  void mark_stale() { state |= STATE_STALE; }
  void clear_stale() { state &= ~STATE_STALE; }
  bool is_new() { return state & STATE_NEW; }
  void mark_new() { state |= STATE_NEW; }
  void clear_new() { state &= ~STATE_NEW; }

  CInode *get_inode() { return inode; }
  client_t get_client() const { return client; }

  // caps this client wants to hold
  int wanted() { return _wanted; }
  void set_wanted(int w) {
    _wanted = w;
    //check_rdcaps_list();
  }

  void inc_last_seq() { last_sent++; }
  ceph_seq_t get_last_seq() { return last_sent; }
  ceph_seq_t get_last_issue() { return last_issue; }

  void reset_seq() {
    last_sent = 0;
    last_issue = 0;
  }
  
  // -- exports --
  Export make_export() {
    return Export(cap_id, _wanted, issued(), pending(), client_follows, last_sent, mseq+1, last_issue_stamp);
  }
  void merge(Export& other, bool auth_cap) {
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
    _wanted = _wanted | other.wanted;
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
    _wanted = _wanted | otherwanted;
  }

  void revoke() {
    if (pending() & ~CEPH_CAP_PIN)
      issue(CEPH_CAP_PIN);
    confirm_receipt(last_sent, CEPH_CAP_PIN);
  }

  // serializers
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<Capability*>& ls);
  
};

WRITE_CLASS_ENCODER(Capability::Export)
WRITE_CLASS_ENCODER(Capability::Import)
WRITE_CLASS_ENCODER(Capability::revoke_info)
WRITE_CLASS_ENCODER(Capability)



#endif
