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

#ifndef __MCLIENTMOUNTACK_H
#define __MCLIENTMOUNTACK_H

#include "msg/Message.h"
#include "MClientMount.h"
#include "osd/OSDMap.h"


class MClientMountAck : public Message {
  long pcid;
  bufferlist osd_map_state;

 public:
  MClientMountAck() {}
  MClientMountAck(MClientMount *mnt, OSDMap *osdmap) : Message(MSG_CLIENT_MOUNTACK) { 
	this->pcid = mnt->get_pcid();
	osdmap->encode( osd_map_state );
  }
  
  bufferlist& get_osd_map_state() { return osd_map_state; }

  void set_pcid(long pcid) { this->pcid = pcid; }
  long get_pcid() { return pcid; }

  char *get_type_name() { return "CmntA"; }

  virtual void decode_payload() {  
	int off = 0;
	payload.copy(off, sizeof(pcid), (char*)&pcid);
	off += sizeof(pcid);
	if ((unsigned)off < payload.length())
	  payload.splice( off, payload.length()-off, &osd_map_state);
  }
  virtual void encode_payload() {  
	payload.append((char*)&pcid, sizeof(pcid));
	payload.claim_append(osd_map_state);
  }
};

#endif
