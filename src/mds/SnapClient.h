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

#ifndef __SNAPCLIENT_H
#define __SNAPCLIENT_H

#include <vector>
using std::vector;
#include <ext/hash_map>
using __gnu_cxx::hash_map;

#include "include/types.h"
#include "msg/Dispatcher.h"

#include "snap.h"

class Context;
class MDS;
class LogSegment;

class MDSTableClient : public Dispatcher {
  MDS *mds;

  // prepares
  struct _pending_prepare {
    Context *onfinish;
    version_t *patid;
    bufferlist mutation;
  };

  hash_map<metareqid_t, _pending_prepare> pending_prepare;

  // pending commits
  map<version_t, LogSegment*> pending_commit;
  map<version_t, list<Context*> > ack_waiters;

  void handle_reply(class MAnchor *m);  


};

#endif
