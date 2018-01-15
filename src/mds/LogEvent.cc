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

#include "common/config.h"
#include "LogEvent.h"

#include "MDSRank.h"

// events i know of
#include "events/ESubtreeMap.h"
#include "events/EExport.h"
#include "events/EImportStart.h"
#include "events/EImportFinish.h"
#include "events/EFragment.h"

#include "events/EResetJournal.h"
#include "events/ESession.h"
#include "events/ESessions.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"

#include "events/ENoOp.h"

#define dout_context g_ceph_context


LogEvent *LogEvent::decode(bufferlist& bl)
{
  // parse type, length
  bufferlist::iterator p = bl.begin();
  EventType type;
  LogEvent *event = NULL;
  using ceph::decode;
  decode(type, p);

  if (EVENT_NEW_ENCODING == type) {
    try {
      DECODE_START(1, p);
      decode(type, p);
      event = decode_event(bl, p, type);
      DECODE_FINISH(p);
    }
    catch (const buffer::error &e) {
      generic_dout(0) << "failed to decode LogEvent (type maybe " << type << ")" << dendl;
      return NULL;
    }
  } else { // we are using classic encoding
    event = decode_event(bl, p, type);
  }
  return event;
}


std::string LogEvent::get_type_str() const
{
  switch(_type) {
  case EVENT_SUBTREEMAP: return "SUBTREEMAP";
  case EVENT_SUBTREEMAP_TEST: return "SUBTREEMAP_TEST";
  case EVENT_EXPORT: return "EXPORT";
  case EVENT_IMPORTSTART: return "IMPORTSTART";
  case EVENT_IMPORTFINISH: return "IMPORTFINISH";
  case EVENT_FRAGMENT: return "FRAGMENT";
  case EVENT_RESETJOURNAL: return "RESETJOURNAL";
  case EVENT_SESSION: return "SESSION";
  case EVENT_SESSIONS_OLD: return "SESSIONS_OLD";
  case EVENT_SESSIONS: return "SESSIONS";
  case EVENT_UPDATE: return "UPDATE";
  case EVENT_SLAVEUPDATE: return "SLAVEUPDATE";
  case EVENT_OPEN: return "OPEN";
  case EVENT_COMMITTED: return "COMMITTED";
  case EVENT_TABLECLIENT: return "TABLECLIENT";
  case EVENT_TABLESERVER: return "TABLESERVER";
  case EVENT_NOOP: return "NOOP";

  default:
    generic_dout(0) << "get_type_str: unknown type " << _type << dendl;
    return "UNKNOWN";
  }
}

const std::map<std::string, LogEvent::EventType> LogEvent::types = {
  {"SUBTREEMAP", EVENT_SUBTREEMAP},
  {"SUBTREEMAP_TEST", EVENT_SUBTREEMAP_TEST},
  {"EXPORT", EVENT_EXPORT},
  {"IMPORTSTART", EVENT_IMPORTSTART},
  {"IMPORTFINISH", EVENT_IMPORTFINISH},
  {"FRAGMENT", EVENT_FRAGMENT},
  {"RESETJOURNAL", EVENT_RESETJOURNAL},
  {"SESSION", EVENT_SESSION},
  {"SESSIONS_OLD", EVENT_SESSIONS_OLD},
  {"SESSIONS", EVENT_SESSIONS},
  {"UPDATE", EVENT_UPDATE},
  {"SLAVEUPDATE", EVENT_SLAVEUPDATE},
  {"OPEN", EVENT_OPEN},
  {"COMMITTED", EVENT_COMMITTED},
  {"TABLECLIENT", EVENT_TABLECLIENT},
  {"TABLESERVER", EVENT_TABLESERVER},
  {"NOOP", EVENT_NOOP}
};

/*
 * Resolve type string to type enum
 *
 * Return -1 if not found
 */
LogEvent::EventType LogEvent::str_to_type(std::string_view str)
{
  return LogEvent::types.at(std::string(str));
}


LogEvent *LogEvent::decode_event(bufferlist& bl, bufferlist::iterator& p, LogEvent::EventType type)
{
  int length = bl.length() - p.get_off();
  generic_dout(15) << "decode_log_event type " << type << ", size " << length << dendl;
  
  // create event
  LogEvent *le;
  switch (type) {
  case EVENT_SUBTREEMAP: le = new ESubtreeMap; break;
  case EVENT_SUBTREEMAP_TEST: 
    le = new ESubtreeMap;
    le->set_type(type);
    break;
  case EVENT_EXPORT: le = new EExport; break;
  case EVENT_IMPORTSTART: le = new EImportStart; break;
  case EVENT_IMPORTFINISH: le = new EImportFinish; break;
  case EVENT_FRAGMENT: le = new EFragment; break;

  case EVENT_RESETJOURNAL: le = new EResetJournal; break;

  case EVENT_SESSION: le = new ESession; break;
  case EVENT_SESSIONS_OLD: le = new ESessions; (static_cast<ESessions *>(le))->mark_old_encoding(); break;
  case EVENT_SESSIONS: le = new ESessions; break;

  case EVENT_UPDATE: le = new EUpdate; break;
  case EVENT_SLAVEUPDATE: le = new ESlaveUpdate; break;
  case EVENT_OPEN: le = new EOpen; break;
  case EVENT_COMMITTED: le = new ECommitted; break;

  case EVENT_TABLECLIENT: le = new ETableClient; break;
  case EVENT_TABLESERVER: le = new ETableServer; break;

  case EVENT_NOOP: le = new ENoOp; break;

  default:
    generic_dout(0) << "uh oh, unknown log event type " << type << " length " << length << dendl;
    return NULL;
  }

  // decode
  try {
    le->decode(p);
  }
  catch (const buffer::error &e) {
    generic_dout(0) << "failed to decode LogEvent type " << type << dendl;
    delete le;
    return NULL;
  }

  assert(p.end());
  return le;
}

