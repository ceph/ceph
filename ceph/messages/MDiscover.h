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

#ifndef __MDISCOVER_H
#define __MDISCOVER_H

#include "msg/Message.h"
#include "mds/CDir.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;


class MDiscover : public Message {
  int             asker;
  inodeno_t       base_ino;          // 0 -> none, want root
  bool            want_base_dir;
  bool            want_root_inode;
  
  filepath        want;   // ... [/]need/this/stuff

 public:
  int       get_asker() { return asker; }
  inodeno_t get_base_ino() { return base_ino; }
  filepath& get_want() { return want; }
  const string&   get_dentry(int n) { return want[n]; }
  bool      wants_base_dir() { return want_base_dir; }

  MDiscover() { }
  MDiscover(int asker, 
			inodeno_t base_ino,
			filepath& want,
			bool want_base_dir = true,
			bool want_root_inode = false) :
	Message(MSG_MDS_DISCOVER) {
	this->asker = asker;
	this->base_ino = base_ino;
	this->want = want;
	this->want_base_dir = want_base_dir;
	this->want_root_inode = want_root_inode;
  }
  virtual char *get_type_name() { return "Dis"; }

  virtual void decode_payload(crope& r, int& off) {
	r.copy(off, sizeof(asker), (char*)&asker);
    off += sizeof(asker);
    r.copy(off, sizeof(base_ino), (char*)&base_ino);
    off += sizeof(base_ino);
    r.copy(off, sizeof(bool), (char*)&want_base_dir);
    off += sizeof(bool);
    want._unrope(r, off);
  }
  virtual void encode_payload(crope& r) {
	r.append((char*)&asker, sizeof(asker));
    r.append((char*)&base_ino, sizeof(base_ino));
    r.append((char*)&want_base_dir, sizeof(want_base_dir));
    want._rope(r);
  }

};

#endif
