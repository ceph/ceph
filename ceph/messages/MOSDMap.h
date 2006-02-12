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

#ifndef __MOSDGETMAPACK_H
#define __MOSDGETMAPACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDMap : public Message {
  bufferlist osdmap;
  __uint64_t version;
  bool mkfs;

 public:
  // osdmap
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  __uint64_t get_version() { return version; }
  bool is_mkfs() { return mkfs; }

  MOSDMap(OSDMap *oc, bool mkfs=false) :
	Message(MSG_OSD_MAP) {
	oc->encode(osdmap);
	version = oc->get_version();
	this->mkfs = mkfs;
  }
  MOSDMap() {}


  // marshalling
  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(version), (char*)&version);
	off += sizeof(version);
	payload.copy(off, sizeof(mkfs), (char*)&mkfs);
	off += sizeof(mkfs);
	payload.splice(0, off);
	osdmap.claim(payload);
  }
  virtual void encode_payload() {
	payload.append((char*)&version, sizeof(version));
	payload.append((char*)&mkfs, sizeof(mkfs));
	payload.claim_append(osdmap);
  }

  virtual char *get_type_name() { return "omap"; }
};

#endif
