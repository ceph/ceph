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


#ifndef __LOGEVENT_H
#define __LOGEVENT_H

#include <stdlib.h>
#include <string>
#include <ext/rope>
using namespace std;

#define EVENT_STRING       1
#define EVENT_INODEUPDATE  2
#define EVENT_UNLINK       3
#define EVENT_ALLOC        4

#include "config.h"


// generic log event
class LogEvent {
 private:
  int _type;

 public:
  LogEvent(int t) : _type(t) { }
  virtual ~LogEvent() { }
  
  int get_type() { return _type; }

  virtual void encode_payload(bufferlist& bl) = 0;
  virtual void decode_payload(bufferlist& bl, int& off) = 0;

  void encode(bufferlist& bl) {
	// type
	assert(_type > 0);
	bl.append((char*)&_type, sizeof(_type));

	// len placeholder
	int len = 0;   // we don't know just yet...
	int off = bl.length();
	bl.append((char*)&len, sizeof(len)); 

	// payload
	encode_payload(bl);

	// HACK: pad payload to match md log layout?
	int elen = bl.length() - off + sizeof(_type);
	if (elen % g_conf.mds_log_pad_entry > 0) {
	  int add = g_conf.mds_log_pad_entry - (elen % g_conf.mds_log_pad_entry);
	  //cout << "elen " << elen << "  adding " << add << endl;
	  buffer *b = new buffer(add);
	  memset(b->c_str(), 0, add);
	  bufferptr bp(b);
	  bl.append(bp);
	} 

	len = bl.length() - off - sizeof(len);

	bl.copy_in(off, sizeof(len), (char*)&len);
  }
  
  virtual bool obsolete(MDS *m) {
	return true;
  }

  virtual void retire(MDS *m, Context *c) {
	c->finish(0);
	delete c;
  }
};

#endif
