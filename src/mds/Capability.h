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

  map<capseq_t, __u32>  cap_history;  // seq -> cap, [last_recv,last_sent]
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

  bool is_null() { return cap_history.empty() && wanted_caps == 0; }

  // most recently issued caps.
  int pending() { 
    if (!last_sent) 
      return 0;
    if (cap_history.count(last_sent))
      return cap_history[last_sent];
    else 
      return 0;
  }
  
  // caps client has confirmed receipt of
  int confirmed() { 
    if (!last_recv)
      return 0;
    if (cap_history.count(last_recv))
      return cap_history[last_recv];
    else
      return 0;
  }

  // caps issued, potentially still in hands of client
  int issued() { 
    int c = 0;
    for (map<capseq_t,__u32>::iterator p = cap_history.begin();
	 p != cap_history.end();
	 p++) {
      c |= p->second;
      generic_dout(10) << " cap issued: seq " << p->first << " " 
		       << ccap_string(p->second) << " -> " << ccap_string(c)
		       << dendl;
    }
    return c;
  }

  // caps this client wants to hold
  int wanted() { return wanted_caps; }
  void set_wanted(int w) {
    wanted_caps = w;
  }

  // issue caps; return seq number.
  capseq_t issue(int c) {
    last_issue = ++last_sent;
    cap_history[last_sent] = c;
    return last_sent;
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

  // confirm receipt of a previous sent/issued seq.
  int confirm_receipt(capseq_t seq, int caps) {
    int r = 0;

    generic_dout(10) << " confirm_receipt seq " << seq << " last_recv " << last_recv << " last_sent " << last_sent
		    << " cap_history " << cap_history << dendl;

    assert(last_recv <= last_sent);
    assert(seq <= last_sent);
    while (!cap_history.empty()) {
      map<capseq_t,__u32>::iterator p = cap_history.begin();

      if (p->first > seq)
	break;

      if (p->first == seq) {
	// note what we're releasing..
	if (p->second & ~caps) {
	  generic_dout(10) << " cap.confirm_receipt revising seq " << seq 
			  << " " << ccap_string(cap_history[seq]) << " -> " << ccap_string(caps) 
			  << dendl;
	  r |= cap_history[seq] & ~caps; 
	  cap_history[seq] = caps; // confirmed() now less than before..
	}

	// null?
	if (caps == 0 && seq == last_sent) {
	  generic_dout(10) << " cap.confirm_receipt making null seq " << last_recv
			  << " " << ccap_string(cap_history[last_recv]) << dendl;
	  cap_history.clear();  // viola, null!
	}
	break;
      }

      generic_dout(10) << " cap.confirm_receipt forgetting seq " << p->first
		      << " " << ccap_string(p->second) << dendl;
      r |= p->second;
      cap_history.erase(p);
    }
    last_recv = seq;
      
    return r;
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
    ::encode(cap_history, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(wanted_caps, bl);
    ::decode(last_sent, bl);
    ::decode(last_recv, bl);
    ::decode(cap_history, bl);
  }
  
};

WRITE_CLASS_ENCODER(Capability::Export)
WRITE_CLASS_ENCODER(Capability)



#endif
