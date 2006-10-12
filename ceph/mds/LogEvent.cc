// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "LogEvent.h"

#include "MDS.h"

// events i know of
#include "events/EString.h"
#include "events/EInodeUpdate.h"
#include "events/EDirUpdate.h"
#include "events/EUnlink.h"
#include "events/EAlloc.h"

LogEvent *LogEvent::decode(bufferlist& bl)
{
  // parse type, length
  int off = 0;
  __uint32_t type;
  bl.copy(off, sizeof(type), (char*)&type);
  off += sizeof(type);

  int length = bl.length() - off;
  dout(15) << "decode_log_event type " << type << ", size " << length << endl;
  
  assert(type > 0);
  
  // create event
  LogEvent *le;
  switch (type) {
  case EVENT_STRING:  // string
    le = new EString();
    break;
    
  case EVENT_INODEUPDATE:
    le = new EInodeUpdate();
    break;
    
  case EVENT_DIRUPDATE:
    le = new EDirUpdate();
    break;
    
  case EVENT_UNLINK:
    le = new EUnlink();
    break;
    
  case EVENT_ALLOC:
    le = new EAlloc();
    break;

  default:
    dout(1) << "uh oh, unknown event type " << type << endl;
    assert(0);
  }

  // decode
  le->decode_payload(bl, off);
  
  return le;
}

