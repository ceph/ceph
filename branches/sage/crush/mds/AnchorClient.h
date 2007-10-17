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

#include <vector>
using std::vector;
#include <ext/hash_map>
using __gnu_cxx::hash_map;

#include "include/types.h"
#include "msg/Dispatcher.h"

#include "Anchor.h"

class Context;
class MDS;
class LogSegment;

class AnchorClient : public Dispatcher {
  MDS *mds;

  // lookups
  struct _pending_lookup { 
    vector<Anchor> *trace;
    Context *onfinish;
  };
  hash_map<inodeno_t, _pending_lookup> pending_lookup;

  // prepares
  struct _pending_prepare { 
    vector<Anchor> trace;
    Context *onfinish;
    version_t *patid;  // ptr to atid
  };
  hash_map<inodeno_t, _pending_prepare> pending_create_prepare;
  hash_map<inodeno_t, _pending_prepare> pending_destroy_prepare;
  hash_map<inodeno_t, _pending_prepare> pending_update_prepare;

  // pending commits
  map<version_t, LogSegment*> pending_commit;
  map<version_t, list<Context*> > ack_waiters;

  void handle_anchor_reply(class MAnchor *m);  

  class C_LoggedAck : public Context {
    AnchorClient *ac;
    version_t atid;
  public:
    C_LoggedAck(AnchorClient *a, version_t t) : ac(a), atid(t) {}
    void finish(int r) {
      ac->_logged_ack(atid);
    }
  };
  void _logged_ack(version_t atid);

public:
  AnchorClient(MDS *m) : mds(m) {}
  
  void dispatch(Message *m);

  // async user interface
  void lookup(inodeno_t ino, vector<Anchor>& trace, Context *onfinish);

  void prepare_create(inodeno_t ino, vector<Anchor>& trace, version_t *atid, Context *onfinish);
  void prepare_destroy(inodeno_t ino, version_t *atid, Context *onfinish);
  void prepare_update(inodeno_t ino, vector<Anchor>& trace, version_t *atid, Context *onfinish);

  void commit(version_t atid, LogSegment *ls);

  // for recovery (by other nodes)
  void handle_mds_recovery(int mds); // called when someone else recovers

  void resend_commits();
  void resend_prepares(hash_map<inodeno_t, _pending_prepare>& prepares, int op);

  // for recovery (by me)
  void got_journaled_agree(version_t atid, LogSegment *ls) {
    pending_commit[atid] = ls;
  }
  void got_journaled_ack(version_t atid) {
    pending_commit.erase(atid);
  }
  bool has_committed(version_t atid) {
    return pending_commit.count(atid) == 0;
  }
  void wait_for_ack(version_t atid, Context *c) {
    ack_waiters[atid].push_back(c);
  }
  void finish_recovery();                // called when i recover and go active


};

#endif
