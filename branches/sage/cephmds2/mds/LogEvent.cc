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
#include "events/EImportMap.h"
#include "events/EMetaBlob.h"
#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EUnlink.h"
#include "events/EMount.h"
#include "events/EClientMap.h"
#include "events/EAnchor.h"
#include "events/EAnchorClient.h"
#include "events/EAlloc.h"
#include "events/EPurgeFinish.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"

LogEvent *LogEvent::decode(bufferlist& bl)
{
  // parse type, length
  int off = 0;
  int type;
  bl.copy(off, sizeof(type), (char*)&type);
  off += sizeof(type);

  int length = bl.length() - off;
  dout(15) << "decode_log_event type " << type << ", size " << length << endl;
  
  assert(type > 0);
  
  // create event
  LogEvent *le;
  switch (type) {
  case EVENT_STRING: le = new EString(); break;
  case EVENT_IMPORTMAP: le = new EImportMap; break;
  case EVENT_UPDATE: le = new EUpdate; break;
  case EVENT_SLAVEUPDATE: le = new ESlaveUpdate; break;
  case EVENT_UNLINK: le = new EUnlink(); break;
  case EVENT_PURGEFINISH: le = new EPurgeFinish(); break;
  case EVENT_MOUNT: le = new EMount(); break;
  case EVENT_CLIENTMAP: le = new EClientMap(); break;
  case EVENT_ANCHOR: le = new EAnchor(); break;
  case EVENT_ANCHORCLIENT: le = new EAnchorClient(); break;
  case EVENT_ALLOC: le = new EAlloc(); break;
  case EVENT_EXPORT: le = new EExport; break;
  case EVENT_IMPORTSTART: le = new EImportStart; break;
  case EVENT_IMPORTFINISH: le = new EImportFinish; break;
  default:
    dout(1) << "uh oh, unknown log event type " << type << endl;
    assert(0);
  }

  // decode
  le->decode_payload(bl, off);
  
  return le;
}

