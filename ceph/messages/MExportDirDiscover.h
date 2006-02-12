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

#ifndef __MEXPORTDIRDISCOVER_H
#define __MEXPORTDIRDISCOVER_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscover : public Message {
  inodeno_t ino;
  string path;

 public:
  inodeno_t get_ino() { return ino; }
  string& get_path() { return path; }

  MExportDirDiscover() {}
  MExportDirDiscover(CInode *in) : 
	Message(MSG_MDS_EXPORTDIRDISCOVER) {
	in->make_path(path);
	ino = in->ino();
  }
  virtual char *get_type_name() { return "ExDis"; }


  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	_unrope(path, s, off);
  }

  virtual void encode_payload(crope& s) {
	s.append((char*)&ino, sizeof(ino));
	_rope(path, s);
  }
};

#endif
