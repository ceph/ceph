// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifndef __MDS_ETRACE_H
#define __MDS_ETRACE_H

#include <stdlib.h>
#include <string>
using namespace std;

#include "../CInode.h"
#include "../CDir.h"
#include "../CDentry.h"


// path trace for use in journal events

class ETrace {

  // <dir, dn, inode> segment.
  struct bit {
    inodeno_t dirino;
    version_t dirv;
    string dn;
    inode_t inode;
    
    bit(bufferlist& bl, int& off) { _decode(bl,off); }
    bit(inodeno_t di, version_t dv, const string& d, inode_t i) :
      dirino(di), dirv(dv), dn(d), inode(i) {}
    
    void _encode(bufferlist& bl) {
      bl.append((char*)&dirino, sizeof(dirino));
      bl.append((char*)&dirv, sizeof(dirv));
      ::_encode(dn, bl);
      bl.append((char*)&inode, sizeof(inode));
    }
    void _decode(bufferlist& bl, int& off) {
      bl.copy(off, sizeof(dirino), (char*)&dirino);  off += sizeof(dirino);
      bl.copy(off, sizeof(dirv), (char*)&dirv);  off += sizeof(dirv);
      ::_decode(dn, bl, off);
      bl.copy(off, sizeof(inode), (char*)&inode);  off += sizeof(inode);
    }
  };

 public:
  list<bit> trace;

  ETrace(CInode *in = 0) { 
    if (in) {
      CDir *dir;
      CDentry *dn;
      do {
	dn = in->get_parent_dn();
	if (!dn) break;
	dir = dn->get_dir();
	if (!dir) break;      
	
	trace.push_front(bit(dir->ino(),
			     dir->get_version(),
			     dn->get_name(),
			     in->inode));
	
	in = dir->get_inode();
      } while (!dir->is_import());
    }
  }
  
  bit& back() {
    return trace.back();
  }

  void decode(bufferlist& bl, int& off) {
    int n;
    bl.copy(off, sizeof(n), (char*)&n);
    off += sizeof(n);
    for (int i=0; i<n; i++) 
      trace.push_back( bit(bl, off) );
  }
  
  void encode(bufferlist& bl) {
    int n = trace.size();
    bl.append((char*)&n, sizeof(n));
    for (list<bit>::iterator i = trace.begin();
	 i != trace.end();
	 i++)
      i->_encode(bl);
  }
  
  void print(ostream& out) const {
    for (list<bit>::const_iterator p = trace.begin();
	 p != trace.end();
	 p++) {
      if (p == trace.begin()) 
	out << "[" << p->dirino << "]/" << p->dn;
      else 
	out << "/" << p->dn;
    }
  }
  
  CInode *restore_trace(MDS *mds);
  
};

inline ostream& operator<<(ostream& out, const ETrace& t) {
  t.print(out);
  return out;
}

#endif
