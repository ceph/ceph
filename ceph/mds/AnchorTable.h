// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __ANCHORTABLE_H
#define __ANCHORTABLE_H

#include "include/types.h"
#include "include/Context.h"
#include "include/bufferlist.h"

#include <ext/hash_map>
using namespace __gnu_cxx;

class MDS;

class Anchor {
public:
  inodeno_t ino;      // my ino
  inodeno_t dirino;   // containing dir
  string    ref_dn;   // referring dentry
  int       nref;     // reference count

  Anchor() {}
  Anchor(inodeno_t ino, inodeno_t dirino, string& ref_dn, int nref=0) {
	this->ino = ino;
	this->dirino = dirino;
	this->ref_dn = ref_dn;
	this->nref = nref;
  }  

  void _rope(crope& r) {
	r.append((char*)&ino, sizeof(ino));
	r.append((char*)&dirino, sizeof(dirino));
	r.append((char*)&nref, sizeof(nref));
	::_rope(ref_dn, r);
  }
  void _unrope(crope& r, int& off) {
	r.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	r.copy(off, sizeof(dirino), (char*)&dirino);
	off += sizeof(dirino);
	r.copy(off, sizeof(nref), (char*)&nref);
	off += sizeof(nref);
	::_unrope(ref_dn, r, off);
  }
} ;


class AnchorTable {
  MDS *mds;
  hash_map<inodeno_t, Anchor*>  anchor_map;

  bool opening, opened;
  list<Context*> waiting_for_open;

  // remote state
  hash_map<inodeno_t, Context*>  pending_op;
  hash_map<inodeno_t, Context*>  pending_lookup_context;
  hash_map<inodeno_t, vector<Anchor*>*>  pending_lookup_trace;

 public:
  inode_t table_inode;

 public:
  AnchorTable(MDS *mds); 

 protected:
  // 
  bool have_ino(inodeno_t ino) { 
	return true;                  // always in memory for now.
  } 
  void fetch_ino(inodeno_t ino, Context *onfinish) {
	assert(!opened);
	load(onfinish);
  }

  // adjust table
  bool add(inodeno_t ino, inodeno_t dirino, string& ref_dn);
  void inc(inodeno_t ino);
  void dec(inodeno_t ino);

  
  // high level interface
  void lookup(inodeno_t ino, vector<Anchor*>& trace);
  void create(inodeno_t ino, vector<Anchor*>& trace);
  void destroy(inodeno_t ino);

  // messages
 public:
  void proc_message(class Message *m);
 protected:
  void handle_anchor_request(class MAnchorRequest *m);  
  void handle_anchor_reply(class MAnchorReply *m);  


 public:
  // user interface
  void lookup(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void create(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void update(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void destroy(inodeno_t ino, Context *onfinish);



  // load/save entire table for now!
  void reset() {
	opened = true;
	anchor_map.clear();
  }
  void save(Context *onfinish);
  void load(Context *onfinish);
  void load_2(size_t size, bufferlist& bl, Context *onfinish);


};

#endif
