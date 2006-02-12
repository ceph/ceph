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

#ifndef __MHASHREADDIRREPLY_H
#define __MHASHREADDIRREPLY_H

#include "MClientReply.h"

class MHashReaddirReply : public Message {
  inodeno_t ino;
  list<c_inode_info*> dir_contents;
  
 public:
  MHashReaddirReply() { }
  MHashReaddirReply(inodeno_t ino, list<c_inode_info*>& ls) :
	Message(MSG_MDS_HASHREADDIRREPLY) {
	this->ino = ino;
	dir_contents.splice(dir_contents.begin(), ls);
  }
  ~MHashReaddirReply() {
	list<c_inode_info*>::iterator it;
	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  delete *it;
  }

  inodeno_t get_ino() { return ino; }
  list<c_inode_info*>& get_items() { return dir_contents; }

  virtual char *get_type_name() { return "Hls"; }

  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	int n;
	payload.copy(n, sizeof(n), (char*)&n);
	off += sizeof(n);
	for (int i=0; i<n; i++) {
	  c_inode_info *ci = new c_inode_info;
	  ci->_decode(payload, off);
	  dir_contents.push_back(ci);
	}
  }
  virtual void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
	int n = dir_contents.size();
	payload.append((char*)&n, sizeof(n));
	list<c_inode_info*>::iterator it;
	for (it = dir_contents.begin(); it != dir_contents.end(); it++) 
	  (*it)->_encode(payload);
  }

};

#endif
