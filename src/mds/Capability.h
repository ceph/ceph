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
  - cap "update" (revocation, etc.) .. an MClientCaps message.
- if client has cap, the mds should have it too.

- if client has no dirty data, it can release it without waiting for an mds ack.
  - client may thus get a cap _update_ and not have the cap.  ignore it.

- mds should track seq of last issue OR update.  any release
  attempt will only succeed if the client has seen the latest.
- if the client gets a cap message and doesn't have the inode or cap, reply with a release.

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

  __u32 _wanted;     // what the client wants (ideally)

  utime_t last_issue_stamp;


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
  ceph_seq_t issue(int c) {
    _pending = c;
    _issued |= c;
    //last_issue = 
    ++last_sent;
    return last_sent;
  }
  void confirm_receipt(ceph_seq_t seq, int caps) {
    if (seq == last_sent)
      _pending = _issued = caps;
  }    
  bool is_null() { rinclude/eturn !_issued && !_pending; }
#endif


  // track up to N revocations ---------
#if 1
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

    check_rdcaps_list(_pending, c, _wanted, _wanted);
    _pending = c;
    //last_issue = 
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
  }
  bool is_null() {
    return !_pending && !_issued && !_num_revoke;
  }
#endif


private:
  ceph_seq_t last_sent;
  //ceph_seq_t last_issue;
  ceph_seq_t mseq;

  int suppress;
  bool stale;

public:
  int releasing;   // only allow a single in-progress release (it may be waiting for log to flush)

  snapid_t client_follows;
  version_t client_xattr_version;
  
  xlist<Capability*>::item session_caps_item;
  xlist<Capability*> *rdcaps_list;
  xlist<Capability*>::item rdcaps_item;

  xlist<Capability*>::item snaprealm_caps_item;

  Capability(CInode *i, int c, xlist<Capability*> *rl) : 
    inode(i), client(c),
    _wanted(0),
    _pending(0), _issued(0), _num_revoke(0),
    last_sent(0),
    mseq(0),
    suppress(0), stale(false), releasing(0),
    client_follows(0), client_xattr_version(0),
    session_caps_item(this), rdcaps_list(rl), rdcaps_item(this), snaprealm_caps_item(this) { }
  
  ceph_seq_t get_mseq() { return mseq; }

  ceph_seq_t get_last_sent() { return last_sent; }
  utime_t get_last_issue_stamp() { return last_issue_stamp; }
  void touch() {
    if (rdcaps_item.is_on_xlist())
      rdcaps_item.move_to_back();
  }

  void set_last_issue_stamp(utime_t t) { last_issue_stamp = t; }

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
    check_rdcaps_list(_pending, _pending, _wanted, w);
    _wanted = w;
  }

  ceph_seq_t get_last_seq() { return last_sent; }


  void check_rdcaps_list(int o, int n, int ow, int nw)
  {
    bool wastrimmable = rdcaps_item.is_on_xlist();//(o & ~CEPH_CAP_EXPIREABLE) == 0 && ow == 0;
    bool istrimmable =  (n & ~CEPH_CAP_EXPIREABLE) == 0 && nw == 0;
    
    if (!wastrimmable && istrimmable) 
      rdcaps_list->push_back(&rdcaps_item);
    else if (wastrimmable && !istrimmable)
      rdcaps_item.remove_myself();
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
    ::encode(_issued, bl);
    ::encode(_num_revoke, bl);
    ::encode_array_nohead(_revoke_before, _num_revoke, bl);
    ::encode_array_nohead(_revoke_seq, _num_revoke, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(last_sent, bl);
    ::decode(last_issue_stamp, bl);

    ::decode(_wanted, bl);
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
