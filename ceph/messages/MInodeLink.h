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

#ifndef __MINODELINK_H
#define __MINODELINK_H

typedef struct {
  inodeno_t ino;
  int from;
} MInodeLink_st;

class MInodeLink : public Message {
  MInodeLink_st st;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_from() { return st.from; }

  MInodeLink() {}
  MInodeLink(inodeno_t ino, int from) :
	Message(MSG_MDS_INODELINK) {
	st.ino = ino;
	st.from = from;
  }
  virtual char *get_type_name() { return "InL";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(st), (char*)&st);
	off += sizeof(st);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&st,sizeof(st));
  }
};

#endif
