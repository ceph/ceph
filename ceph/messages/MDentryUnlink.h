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

#ifndef __MDENTRYUNLINK_H
#define __MDENTRYUNLINK_H

class MDentryUnlink : public Message {
  inodeno_t dirino;
  string dn;

 public:
  inodeno_t get_dirino() { return dirino; }
  string& get_dn() { return dn; }

  MDentryUnlink() {}
  MDentryUnlink(inodeno_t dirino, string& dn) :
	Message(MSG_MDS_DENTRYUNLINK) {
	this->dirino = dirino;
	this->dn = dn;
  }
  virtual char *get_type_name() { return "Dun";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(dirino), (char*)&dirino);
	off += sizeof(dirino);
	_unrope(dn, s, off);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&dirino,sizeof(dirino));
	_rope(dn, s);
  }
};

#endif
