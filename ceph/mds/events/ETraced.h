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


#ifndef __MDS_ETRACED_H
#define __MDS_ETRACED_H

#include <stdlib.h>
#include <string>
using namespace std;

#include "../LogEvent.h"

// generic log event
class ETraced : public LogEvent {
  struct bit {
	inodeno_t dirino;
	version_t dirv;
	string dn;
	inodeno_t ino;
	version_t inov;

	bit() {}
	bit(inodeno_t di, version_t dv, string& d, inodeno_t i, version_t iv) :
	  dirino(di), dirv(dv), dn(d), ino(i), inov(iv) {}

	void _encode(bufferlist& bl) {
	  bl.append((char*)&dirino, sizeof(dirino));
	  bl.append((char*)&dirv, sizeof(dirv));
	  ::_encode(dn,bl);
	  bl.append((char*)&ino, sizeof(ino));
	  bl.append((char*)&inov, sizeof(inov));
	}
	void _decode(bufferlist& bl, int& off) {
	  bl.copy(off, sizeof(dirino), (char*)&dirino);  off += sizeof(dirino);
	  bl.copy(off, sizeof(dirv), (char*)&dirv);  off += sizeof(dirv);
	  ::_decode(dn, bl, off);
	  bl.copy(off, sizeof(ino), (char*)&ino);  off += sizeof(ino);
	  bl.copy(off, sizeof(inov), (char*)&inov);  off += sizeof(inov);
	}
  };

 protected:
  list<bit> trace;

 public:
  ETraced(int t) : LogEvent(t) { }

  void decode_trace(bufferlist& bl, int& off) {
	int n;
	bl.copy(off, sizeof(n), (char*)&n);
	off += n;
	for (int i=0; i<n; i++) {
	  trace.push_back(bit());
	  trace.back()._decode(bl,off);
	}
  }
  
  void encode_trace(bufferlist& bl) {
	int n = trace.size();
	bl.append((char*)&n, sizeof(n));
	for (list<bit>::iterator i = trace.begin();
		 i != trace.end();
		 i++)
	  i->_encode(bl);
  }

  void print(ostream& out) {
    out << '"' << event << '"';
  }

};

#endif
