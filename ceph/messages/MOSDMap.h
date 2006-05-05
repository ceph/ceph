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


#ifndef __MOSDGETMAPACK_H
#define __MOSDGETMAPACK_H

#include "msg/Message.h"
#include "osd/OSDMap.h"


class MOSDMap : public Message {
  bufferlist osdmap;
  epoch_t epoch;
  //bool mkfs;

 public:
  // osdmap
  bufferlist& get_osdmap() { 
	return osdmap;
  }

  epoch_t get_epoch() { return epoch; }
  //bool is_mkfs() { return mkfs; }

  MOSDMap(OSDMap *oc) :
	Message(MSG_OSD_MAP) {
	oc->encode(osdmap);
	epoch = oc->get_epoch();
  }
  MOSDMap() {}


  // marshalling
  virtual void decode_payload() {
	int off = 0;
	payload.copy(off, sizeof(epoch), (char*)&epoch);
	off += sizeof(epoch);
	//payload.copy(off, sizeof(mkfs), (char*)&mkfs);
	//off += sizeof(mkfs);
	payload.splice(0, off);
	osdmap.claim(payload);
  }
  virtual void encode_payload() {
	payload.append((char*)&epoch, sizeof(epoch));
	//payload.append((char*)&mkfs, sizeof(mkfs));
	payload.claim_append(osdmap);
  }

  virtual char *get_type_name() { return "omap"; }
};

#endif
