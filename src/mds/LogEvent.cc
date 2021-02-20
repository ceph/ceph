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
#include "events/EPeerUpdate.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"
#include "events/EPurged.h"

#include "events/ETableClient.h"
#include "events/ETableServer.h"

#include "events/ENoOp.h"

#define dout_context g_ceph_context


std::unique_ptr<LogEvent> LogEvent::decode_event(bufferlist::const_iterator p)
{
  // parse type, length
  EventType type;
  std::unique_ptr<LogEvent> event;
  using ceph::decode;
  decode(type, p);

  if (EVENT_NEW_ENCODING == type) {
    try {
      DECODE_START(1, p);
      decode(type, p);
      event = decode_event(p, type);
      DECODE_FINISH(p);
    }
    catch (const buffer::error &e) {
      generic_dout(0) << "failed to decode LogEvent (type maybe " << type << ")" << dendl;
      return NULL;
    }
  } else { // we are using classic encoding
    event = decode_event(p, type);
  }
  return event;
}


std::string_view LogEvent::get_type_str() const
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
  case EVENT_PEERUPDATE: return "PEERUPDATE";
  case EVENT_OPEN: return "OPEN";
  case EVENT_COMMITTED: return "COMMITTED";
  case EVENT_PURGED: return "PURGED";
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
  {"PEERUPDATE", EVENT_PEERUPDATE},
  {"OPEN", EVENT_OPEN},
  {"COMMITTED", EVENT_COMMITTED},
  {"PURGED", EVENT_PURGED},
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


std::unique_ptr<LogEvent> LogEvent::decode_event(bufferlist::const_iterator& p, LogEvent::EventType type)
{
  const auto length = p.get_remaining();
  generic_dout(15) << "decode_log_event type " << type << ", size " << length << dendl;
  
  // create event
  std::unique_ptr<LogEvent> le;
  switch (type) {
  case EVENT_SUBTREEMAP:
    le = std::make_unique<ESubtreeMap>();
    break;
  case EVENT_SUBTREEMAP_TEST: 
    le = std::make_unique<ESubtreeMap>();
    le->set_type(type);
    break;
  case EVENT_EXPORT:
    le = std::make_unique<EExport>();
    break;
  case EVENT_IMPORTSTART:
    le = std::make_unique<EImportStart>();
    break;
  case EVENT_IMPORTFINISH:
    le = std::make_unique<EImportFinish>();
    break;
  case EVENT_FRAGMENT:
    le = std::make_unique<EFragment>();
    break;
  case EVENT_RESETJOURNAL:
    le = std::make_unique<EResetJournal>();
    break;
  case EVENT_SESSION:
    le = std::make_unique<ESession>();
    break;
  case EVENT_SESSIONS_OLD:
    {
      auto e = std::make_unique<ESessions>();
      e->mark_old_encoding();
      le = std::move(e);
    }
    break;
  case EVENT_SESSIONS:
    le = std::make_unique<ESessions>();
    break;
  case EVENT_UPDATE:
    le = std::make_unique<EUpdate>();
    break;
  case EVENT_PEERUPDATE:
    le = std::make_unique<EPeerUpdate>();
    break;
  case EVENT_OPEN:
    le = std::make_unique<EOpen>();
    break;
  case EVENT_COMMITTED:
    le = std::make_unique<ECommitted>();
    break;
  case EVENT_PURGED:
    le = std::make_unique<EPurged>();
    break;
  case EVENT_TABLECLIENT:
    le = std::make_unique<ETableClient>();
    break;
  case EVENT_TABLESERVER:
    le = std::make_unique<ETableServer>();
    break;
  case EVENT_NOOP:
    le = std::make_unique<ENoOp>();
    break;
  default:
    generic_dout(0) << "uh oh, unknown log event type " << type << " length " << length << dendl;
    return nullptr;
  }

  // decode
  try {
    le->decode(p);
  }
  catch (const buffer::error &e) {
    generic_dout(0) << "failed to decode LogEvent type " << type << dendl;
    return nullptr;
  }

  ceph_assert(p.end());
  return le;
}

