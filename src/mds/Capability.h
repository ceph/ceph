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

#include <map>
using namespace std;

#include "config.h"


/*

  Capability protocol notes.

- two types of cap events from mds -> client:
  - cap "issue" in a MClientReply, or an MClientCaps IMPORT op.
  - cap "update" (revocation, etc.) .. an MClientCaps message.
- if client has cap, the mds should have it too.

- if client has no dirty data, it can release it without waiting for an mds ack.
  - client may thus get a cap _update_ and not have the cap.  ignore it.

- mds should track seq of last _issue_ (not update).  any release
  attempt will only succeed if the client has seen the latest issue.
  - if client gets an IMPORT issue and doesn't have the inode, immediately send a release.

- a UPDATE updates the clients issued caps, wanted, etc.  it may also flush dirty metadata.
  - 'caps' are which caps the client retains.
    - if 0, client wishes to release the cap
  - 'wanted' is which caps the client wants.
  - 'dirty' is which metadata is to be written.
    - client gets a FLUSH_ACK with matching dirty flags indicating which caps were written.

- a FLUSH_ACK acks a FLUSH.
  - 'dirty' is the _original_ FLUSH's dirty (i.e., which metadata was written back)
  - 'seq' is the _original_ FLUSH's seq.
  - 'caps' is the _original_ FLUSH's caps.
  - client can conclude that (dirty & ~caps) bits were successfully cleaned.

- a FLUSHSNAP flushes snapshot metadata.
  - 'dirty' indicates which caps, were dirty, if any.
  - mds writes metadata.  if dirty!=0, replies with FLUSHSNAP_ACK.

- a RELEASE releases one or more (clean) caps.
  - 'caps' is which caps are retained by the client.
  - 'wanted' is which caps the client wants.
  - dirty==0
  - if caps==0, mds can close out the cap (provided there are no racing cap issues)

 */

class CInode;

class Capability {
public:
  struct Export {
    int32_t wanted;
    int32_t issued;
    int32_t pending;
    snapid_t client_follows;
    capseq_t mseq;
    Export() {}
    Export(int w, int i, int p, snapid_t cf, capseq_t s) : 
      wanted(w), issued(i), pending(p), client_follows(cf), mseq(s) {}
    void encode(bufferlist &bl) const {
      ::encode(wanted, bl);
      ::encode(issued, bl);
      ::encode(pending, bl);
      ::encode(client_follows, bl);
      ::encode(mseq, bl);
    }
    void decode(bufferlist::iterator &p) {
      ::decode(wanted, p);
      ::decode(issued, p);
      ::decode(pending, p);
      ::decode(client_follows, p);
      ::decode(mseq, p);
    }
  };

private:
  CInode *inode;
  __u32 wanted_caps;     // what the client wants (ideally)
    //::decode(cap_history, bl);


  // simplest --------------------------
#if 0
  __u32 _pending;
  __u32 _issued;

public:
  int pending() {
    return _pending;
  }
  int issued() {
    return _pending | _issued;
  }
  capseq_t issue(int c) {
    _pending = c;
    _issued |= c;
    last_issue = ++last_sent;
    return last_sent;
  }
  void confirm_receipt(capseq_t seq, int caps) {
    if (seq == last_sent)
      _pending = _issued = caps;
  }    
  bool is_null() { return !_issued && !_pending; }
#endif

  // track up to N revocations ---------
#if 1
  static const int _max_revoke = 3;
  __u32 _pending, _issued;
  __u32 _revoke_before[_max_revoke];  // caps before this issue
  capseq_t _revoke_seq[_max_revoke];
  int _num_revoke;

public:
  int pending() { return _pending; }
  int issued() {
    int c = _pending | _issued;
    for (int i=0; i<_num_revoke; i++)
      c |= _revoke_before[i];
    return c;
  }
  capseq_t issue(int c) {
    if (_pending & ~c) {
      // note _revoked_ caps prior to this revocation
      if (_num_revoke < _max_revoke) {
	_num_revoke++;
	_revoke_before[_num_revoke] = 0;
      }
      _revoke_before[_num_revoke] |= _pending|_issued;
      _revoke_seq[_num_revoke] = last_sent;
    }
    _pending = c;
    last_issue = ++last_sent;
    return last_sent;
  }
  void confirm_receipt(capseq_t seq, int caps) {
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
  }
  bool is_null() {
    return !_pending && !_issued && !_num_revoke;
  }
#endif


private:
  capseq_t last_sent, last_recv;
  capseq_t last_issue;
  capseq_t mseq;

  int suppress;
  bool stale;

public:
  int releasing;   // only allow a single in-progress release (it may be waiting for log to flush)

  snapid_t client_follows;
  
  xlist<Capability*>::item session_caps_item;

  xlist<Capability*>::item snaprealm_caps_item;

  Capability(CInode *i=0, int want=0, capseq_t s=0) :
    inode(i),
    wanted_caps(want),
    //_pending(0), _issued(0),
    _pending(0), _issued(0), _num_revoke(0),
    last_sent(s),
    last_recv(s),
    last_issue(0),
    mseq(0),
    suppress(0), stale(false), releasing(0),
    client_follows(0),
    session_caps_item(this), snaprealm_caps_item(this) { }
  
  capseq_t get_mseq() { return mseq; }

  capseq_t get_last_sent() { return last_sent; }

  capseq_t get_last_issue() { return last_issue; }

  bool is_suppress() { return suppress > 0; }
  void inc_suppress() { suppress++; }
  void dec_suppress() { suppress--; }

  bool is_stale() { return stale; }
  void set_stale(bool b) { stale = b; }

  CInode *get_inode() { return inode; }
  void set_inode(CInode *i) { inode = i; }


  // caps this client wants to hold
  int wanted() { return wanted_caps; }
  void set_wanted(int w) {
    wanted_caps = w;
  }

  capseq_t get_last_seq() { return last_sent; }


  Export make_export() {
    return Export(wanted_caps, issued(), pending(), client_follows, mseq+1);
  }
  void merge(Export& other) {
    // issued + pending
    int newpending = other.pending | pending();
    if (other.issued & ~newpending)
      issue(other.issued | newpending);
    issue(newpending);

    client_follows = other.client_follows;

    // wanted
    wanted_caps = wanted_caps | other.wanted;
    mseq = other.mseq;
  }
  void merge(int otherwanted, int otherissued) {
    // issued + pending
    int newpending = pending();
    if (otherissued & ~newpending)
      issue(otherissued | newpending);
    issue(newpending);

    // wanted
    wanted_caps = wanted_caps | otherwanted;
  }

  void revoke() {
    if (pending())
      issue(0);
    confirm_receipt(last_sent, 0);
  }

  // serializers
  void encode(bufferlist &bl) const {
    ::encode(wanted_caps, bl);
    ::encode(last_sent, bl);
    ::encode(last_recv, bl);

    ::encode(_pending, bl);
    ::encode(_issued, bl);
    ::encode(_num_revoke, bl);
    ::encode_array_nohead(_revoke_before, _num_revoke, bl);
    ::encode_array_nohead(_revoke_seq, _num_revoke, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(wanted_caps, bl);
    ::decode(last_sent, bl);
    ::decode(last_recv, bl);

    ::decode(_pending, bl);
    ::decode(_issued, bl);
    ::decode(_num_revoke, bl);
    ::decode_array_nohead(_revoke_before, _num_revoke, bl);
    ::decode_array_nohead(_revoke_seq, _num_revoke, bl);
  }
  
};

WRITE_CLASS_ENCODER(Capability::Export)
WRITE_CLASS_ENCODER(Capability)



#endif
