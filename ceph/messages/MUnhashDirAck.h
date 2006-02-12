// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef __MUNHASHDIRACK_H
#define __MUNHASHDIRACK_H

#include "msg/Message.h"
#include "include/bufferlist.h"

class MUnhashDirAck : public Message {
  inodeno_t ino;
  bufferlist state;
  int nden;
  
 public:  
  MUnhashDirAck() {}
  MUnhashDirAck(inodeno_t ino, bufferlist& bl, int nden) : 
	Message(MSG_MDS_UNHASHDIRACK) {
	this->ino = ino;
	state.claim(bl);
	this->nden = nden;
  }
  virtual char *get_type_name() { return "UHaA"; }

  inodeno_t get_ino() { return ino; }
  bufferlist& get_state() { return state; }
  bufferlist* get_state_ptr() { return &state; }
  int       get_nden() { return nden; }
  
  //void set_nden(int n) { nden = n; }
  //void inc_nden() { nden++; }

  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	payload.copy(off, sizeof(nden), (char*)&nden);
	off += sizeof(nden);

	size_t len;
	payload.copy(off, sizeof(len), (char*)&len);
	off += sizeof(len);
	state.substr_of(payload, off, len);
  }
  void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
	payload.append((char*)&nden, sizeof(nden));
	size_t size = state.length();
	payload.append((char*)&size, sizeof(size));
	payload.claim_append(state);
  }

};

#endif
