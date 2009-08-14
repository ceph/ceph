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


#ifndef __CAPABILITY_H
#define __CAPABILITY_H

#include "include/buffer.h"
#include "include/xlist.h"

#include "config.h"

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

class Capability {
private:
  static boost::pool<> pool;
public:
  static void *operator new(size_t num_bytes) { 
    return pool.malloc();
  }
  void operator delete(void *p) {
    pool.free(p);
  }
public:
  struct Export {
    int32_t wanted;
    int32_t issued;
    int32_t pending;
    snapid_t client_follows;
    ceph_seq_t mseq;
    utime_t last_issue_stamp;
    Export() {}
    Export(int w, int i, int p, snapid_t cf, ceph_seq_t s, utime_t lis) : 
      wanted(w), issued(i), pending(p), client_follows(cf), mseq(s), last_issue_stamp(lis) {}
    void encode(bufferlist &bl) const {
      ::encode(wanted, bl);
      ::encode(issued, bl);
      ::encode(pending, bl);
      ::encode(client_follows, bl);
      ::encode(mseq, bl);
      ::encode(last_issue_stamp, bl);
    }
    void decode(bufferlist::iterator &p) {
      ::decode(wanted, p);
      ::decode(issued, p);
      ::decode(pending, p);
      ::decode(client_follows, p);
      ::decode(mseq, p);
      ::decode(last_issue_stamp, p);
    }
  };

private:
  CInode *inode;
  int client;

  __u64 cap_id;

  __u32 _wanted;     // what the client wants (ideally)

  utime_t last_issue_stamp;


  // track in-flight caps --------------
  //  - add new caps to _pending
  //  - track revocations in _revokes list
public:
  struct revoke_info {
    __u32 before;
    ceph_seq_t seq;
    revoke_info() {}
    revoke_info(__u32 b, ceph_seq_t s) : before(b), seq(s) {}
    void encode(bufferlist& bl) const {
      ::encode(before, bl);
      ::encode(seq, bl);
    }
    void decode(bufferlist::iterator& bl) {
      ::decode(before, bl);
      ::decode(seq, bl);
    }
  };
private:
  __u32 _pending;
  list<revoke_info> _revokes;

public:
  int pending() { return _pending; }
  int issued() {
    int c = _pending;
    for (list<revoke_info>::iterator p = _revokes.begin(); p != _revokes.end(); p++)
      c |= p->before;
    return c;
  }
  ceph_seq_t issue(unsigned c) {
    if (_pending & ~c) {
      // revoking (and maybe adding) bits.  note caps prior to this revocation
      _revokes.push_back(revoke_info(_pending, last_sent));
      _pending = c;
    } else if (~_pending & c) {
      // adding bits only.  remove obsolete revocations?
      _pending |= c;
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
    //check_rdcaps_list();
    ++last_sent;
    return last_sent;
  }
  void confirm_receipt(ceph_seq_t seq, unsigned caps) {
    if (seq == last_sent) {
      _pending = caps;
      _revokes.clear();
    } else {
      // can i forget any revocations?
      while (!_revokes.empty() &&
	     _revokes.front().seq <= seq)
	_revokes.pop_front();
    }
    //check_rdcaps_list();
  }
  bool is_null() {
    return !_pending && _revokes.empty();
  }

#if 0
  // track up to N revocations ---------
  static const int _max_revoke = 3;
  __u32 _pending, _issued;
  __u32 _revoke_before[_max_revoke];  // caps before this issue
  ceph_seq_t _revoke_seq[_max_revoke];
  int _num_revoke;

public:
  int pending() { return _pending; }
  int issued() {
    int c = _pending | _issued;
    for (int i=0; i<_num_revoke; i++)
      c |= _revoke_before[i];
    return c;
  }
  ceph_seq_t issue(int c) {
    if (_pending & ~c) {
      // note _revoked_ caps prior to this revocation
      if (_num_revoke < _max_revoke) {
	_num_revoke++;
	_revoke_before[_num_revoke-1] = 0;
      }
      _revoke_before[_num_revoke-1] |= _pending|_issued;
      _revoke_seq[_num_revoke-1] = last_sent;
    }

    _pending = c;
    //check_rdcaps_list();
    //last_issue = 
    ++last_sent;
    return last_sent;
  }
  ceph_seq_t issue_norevoke(int c) {
    _pending |= c;
    //check_rdcaps_list();
    ++last_sent;
    return last_sent;
  }
  void confirm_receipt(ceph_seq_t seq, int caps) {
    _issued = caps;
    if (seq == last_sent) {
      _pending = caps;
      _num_revoke = 0;
    } else {
      // can i forget any revocations?
      int i = 0, o = 0;
      while (i < _num_revoke) {
	if (_revoke_seq[i] > seq) {
	  // keep this one
	  if (o < i) {
	    _revoke_before[o] = _revoke_before[i];
	    _revoke_seq[o] = _revoke_before[i];
	  }
	  o++;
	}
	i++;
      }
      _num_revoke = o;
    }
    //check_rdcaps_list();
  }
  bool is_null() {
    return !_pending && !_issued && !_num_revoke;
  }
#endif

private:
  ceph_seq_t last_sent;
  ceph_seq_t last_issue;
  ceph_seq_t mseq;

  int suppress;
  bool stale;

public:
  snapid_t client_follows;
  version_t client_xattr_version;
  
  xlist<Capability*>::item session_caps_item;

  xlist<Capability*>::item snaprealm_caps_item;

  Capability(CInode *i, __u64 id, int c) : 
    inode(i), client(c),
    cap_id(id),
    _wanted(0),
    _pending(0),
    last_sent(0),
    mseq(0),
    suppress(0), stale(false),
    client_follows(0), client_xattr_version(0),
    session_caps_item(this), snaprealm_caps_item(this) {
    g_num_cap++;
    g_num_capa++;
  }
  ~Capability() {
    g_num_cap--;
    g_num_caps++;
  }
  
  ceph_seq_t get_mseq() { return mseq; }

  ceph_seq_t get_last_sent() { return last_sent; }
  utime_t get_last_issue_stamp() { return last_issue_stamp; }

  void set_last_issue() { last_issue = last_sent; }
  void set_last_issue_stamp(utime_t t) { last_issue_stamp = t; }

  void set_cap_id(__u64 i) { cap_id = i; }
  __u64 get_cap_id() { return cap_id; }

  //ceph_seq_t get_last_issue() { return last_issue; }

  bool is_suppress() { return suppress > 0; }
  void inc_suppress() { suppress++; }
  void dec_suppress() { suppress--; }

  bool is_stale() { return stale; }
  void set_stale(bool b) { stale = b; }

  CInode *get_inode() { return inode; }
  int get_client() { return client; }

  // caps this client wants to hold
  int wanted() { return _wanted; }
  void set_wanted(int w) {
    _wanted = w;
    //check_rdcaps_list();
  }

  ceph_seq_t get_last_seq() { return last_sent; }
  ceph_seq_t get_last_issue() { return last_issue; }

  void reset_seq() {
    last_sent = 0;
    last_issue = 0;
  }
  
  // -- exports --
  Export make_export() {
    return Export(_wanted, issued(), pending(), client_follows, mseq+1, last_issue_stamp);
  }
  void merge(Export& other) {
    // issued + pending
    int newpending = other.pending | pending();
    if (other.issued & ~newpending)
      issue(other.issued | newpending);
    issue(newpending);
    last_issue_stamp = other.last_issue_stamp;

    client_follows = other.client_follows;

    // wanted
    _wanted = _wanted | other.wanted;
    mseq = other.mseq;
  }
  void merge(int otherwanted, int otherissued) {
    // issued + pending
    int newpending = pending();
    if (otherissued & ~newpending)
      issue(otherissued | newpending);
    issue(newpending);

    // wanted
    _wanted = _wanted | otherwanted;
  }

  void revoke() {
    if (pending())
      issue(0);
    confirm_receipt(last_sent, 0);
  }

  // serializers
  void encode(bufferlist &bl) const {
    ::encode(last_sent, bl);
    ::encode(last_issue_stamp, bl);

    ::encode(_wanted, bl);
    ::encode(_pending, bl);
    ::encode(_revokes, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(last_sent, bl);
    ::decode(last_issue_stamp, bl);

    ::decode(_wanted, bl);
    ::decode(_pending, bl);
    ::decode(_revokes, bl);
  }
  
};

WRITE_CLASS_ENCODER(Capability::Export)
WRITE_CLASS_ENCODER(Capability::revoke_info)
WRITE_CLASS_ENCODER(Capability)



#endif
