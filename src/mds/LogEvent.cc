// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "events/ESubtreeMap.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/ESession.h"
#include "events/ESessions.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"

#include "events/EPurgeFinish.h"

#include "events/EAnchor.h"
#include "events/EAnchorClient.h"



LogEvent *LogEvent::decode(bufferlist& bl)
{
  // parse type, length
  int off = 0;
  int type;
  bl.copy(off, sizeof(type), (char*)&type);
  off += sizeof(type);

  int length = bl.length() - off;
  generic_dout(15) << "decode_log_event type " << type << ", size " << length << dendl;
  
  assert(type > 0);
  
  // create event
  LogEvent *le;
  switch (type) {
  case EVENT_STRING: le = new EString; break;

  case EVENT_SUBTREEMAP: le = new ESubtreeMap; break;
  case EVENT_EXPORT: le = new EExport; break;
  case EVENT_IMPORTSTART: le = new EImportStart; break;
  case EVENT_IMPORTFINISH: le = new EImportFinish; break;
  case EVENT_FRAGMENT: le = new EFragment; break;

  case EVENT_SESSION: le = new ESession; break;
  case EVENT_SESSIONS: le = new ESessions; break;

  case EVENT_UPDATE: le = new EUpdate; break;
  case EVENT_SLAVEUPDATE: le = new ESlaveUpdate; break;
  case EVENT_OPEN: le = new EOpen; break;

  case EVENT_PURGEFINISH: le = new EPurgeFinish; break;

  case EVENT_ANCHOR: le = new EAnchor; break;
  case EVENT_ANCHORCLIENT: le = new EAnchorClient; break;
  default:
    generic_dout(1) << "uh oh, unknown log event type " << type << dendl;
    assert(0);
  }

  // decode
  le->decode_payload(bl, off);
  
  return le;
}

