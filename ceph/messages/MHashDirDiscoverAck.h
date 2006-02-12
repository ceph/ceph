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

#ifndef __MHASHDIRDISCOVERACK_H
#define __MHASHDIRDISCOVERACK_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MHashDirDiscoverAck : public Message {
  inodeno_t ino;
  bool success;

 public:
  inodeno_t get_ino() { return ino; }
  bool is_success() { return success; }

  MHashDirDiscoverAck() {}
  MHashDirDiscoverAck(inodeno_t ino, bool success=true) : 
	Message(MSG_MDS_HASHDIRDISCOVERACK) {
	this->ino = ino;
	this->success = false;
  }
  virtual char *get_type_name() { return "HDisA"; }


  void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	payload.copy(off, sizeof(success), (char*)&success);
	off += sizeof(success);
  }

  void encode_payload() {
	payload.append((char*)&ino, sizeof(ino));
	payload.append((char*)&success, sizeof(success));
  }
};

#endif
