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

#ifndef __ANCHORCLIENT_H
#define __ANCHORCLIENT_H

#include "MDSTableClient.h"
#include "Anchor.h"

class Context;
class MDS;
class LogSegment;

class AnchorClient : public MDSTableClient {
  // lookups
  struct _pending_lookup {
    vector<Anchor> *trace;
    Context *onfinish;
  };
  map<inodeno_t, list<_pending_lookup> > pending_lookup;

public:
  AnchorClient(MDS *m) : MDSTableClient(m, TABLE_ANCHOR) {}
  
  void handle_query_result(MMDSTableRequest *m);
  void lookup(inodeno_t ino, vector<Anchor>& trace, Context *onfinish);
  void _lookup(inodeno_t ino);
  void resend_queries();

  void prepare_create(inodeno_t ino, vector<Anchor>& trace, version_t *atid, Context *onfinish);
  void prepare_destroy(inodeno_t ino, version_t *atid, Context *onfinish);
  void prepare_update(inodeno_t ino, vector<Anchor>& trace, version_t *atid, Context *onfinish);
};

#endif
