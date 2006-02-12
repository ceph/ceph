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

#ifndef __MHEARTBEAT_H
#define __MHEARTBEAT_H

#include "include/types.h"
#include "msg/Message.h"

class MHeartbeat : public Message {
  mds_load_t load;
  int        beat;
  map<int, float> import_map;

 public:
  mds_load_t& get_load() { return load; }
  int get_beat() { return beat; }

  map<int, float>& get_import_map() {
	return import_map;
  }

  MHeartbeat() {}
  MHeartbeat(mds_load_t& load, int beat) :
	Message(MSG_MDS_HEARTBEAT) {
	this->load = load;
	this->beat = beat;
  }

  virtual char *get_type_name() { return "HB"; }

  virtual void decode_payload(crope& s, int& off) {
	s.copy(off,sizeof(load), (char*)&load);
	off += sizeof(load);
	s.copy(off, sizeof(beat), (char*)&beat);
	off += sizeof(beat);

	int n;
	s.copy(off, sizeof(n), (char*)&n);
	off += sizeof(n);
	while (n--) {
	  int f;
	  s.copy(off, sizeof(f), (char*)&f);
	  off += sizeof(f);
	  float v;
	  s.copy(off, sizeof(v), (char*)&v);
	  off += sizeof(v);	  
	  import_map[f] = v;
	}
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&load, sizeof(load));
	s.append((char*)&beat, sizeof(beat));

	int n = import_map.size();
	s.append((char*)&n, sizeof(n));
	for (map<int, float>::iterator it = import_map.begin();
		 it != import_map.end();
		 it++) {
	  int f = it->first;
	  s.append((char*)&f, sizeof(f));
	  float v = it->second;
	  s.append((char*)&v, sizeof(v));
	}

  }

};

#endif
