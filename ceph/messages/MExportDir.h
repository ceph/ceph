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


#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "msg/Message.h"


class MExportDir : public Message {
  inodeno_t ino;
  
  int         ndirs;
  bufferlist  state;
  
  list<inodeno_t> exports;

  // hashed pre-discovers
  //map<inodeno_t, set<string> > hashed_prediscover;

 public:  
  MExportDir() {}
  MExportDir(CInode *in) : 
	Message(MSG_MDS_EXPORTDIR) {
	this->ino = in->inode.ino;
	ndirs = 0;
  }
  virtual char *get_type_name() { return "Ex"; }

  inodeno_t get_ino() { return ino; }
  int get_ndirs() { return ndirs; }
  bufferlist& get_state() { return state; }
  list<inodeno_t>& get_exports() { return exports; }
  
  void add_dir(bufferlist& dir) {
	state.claim_append( dir );
	ndirs++;
  }
  void add_export(CDir *dir) { exports.push_back(dir->ino()); }


  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	payload.copy(off, sizeof(ndirs), (char*)&ndirs);
	off += sizeof(ndirs);

    // exports
    int nex;
    payload.copy(off, sizeof(nex), (char*)&nex);
    off += sizeof(int);
	dout(12) << nex << " nested exports out" << endl;
	for (int i=0; i<nex; i++) {
	  inodeno_t dirino;
	  payload.copy(off, sizeof(dirino), (char*)&dirino);
	  off += sizeof(dirino);
      exports.push_back(dirino);
	}

	// dir data
	size_t len;
	payload.copy(off, sizeof(len), (char*)&len);
	off += sizeof(len);
	state.substr_of(payload, off, len);
	off += len;
  }
  virtual void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
	payload.append((char*)&ndirs, sizeof(ndirs));

    // exports
    int nex = exports.size();
	dout(12) << nex << " nested exports in" << endl;
    payload.append((char*)&nex, sizeof(int));
	for (list<inodeno_t>::iterator it = exports.begin();
		 it != exports.end();
		 it++) {
	  inodeno_t ino = *it;
	  payload.append((char*)&ino, sizeof(ino));
	}
	
	// dir data
	size_t len = state.length();
	payload.append((char*)&len, sizeof(len));
	payload.claim_append(state);
  }

};

#endif
