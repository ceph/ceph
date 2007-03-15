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

#ifndef __ANCHORCLIENT_H
#define __ANCHORCLIENT_H

#include <vector>
using std::vector;
#include <ext/hash_map>
using __gnu_cxx::hash_map;

#include "include/types.h"
#include "msg/Dispatcher.h"

#include "Anchor.h"

class Messenger;
class MDSMap;
class Context;

class AnchorClient : public Dispatcher {
  Messenger *messenger;
  MDSMap *mdsmap;

  // remote state
  hash_map<inodeno_t, Context*>  pending_op;
  hash_map<inodeno_t, Context*>  pending_lookup_context;
  hash_map<inodeno_t, vector<Anchor*>*>  pending_lookup_trace;

  void handle_anchor_reply(class MAnchorReply *m);  


public:
  AnchorClient(Messenger *ms, MDSMap *mm) : messenger(ms), mdsmap(mm) {}
  
  // async user interface
  void lookup(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void create(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void update(inodeno_t ino, vector<Anchor*>& trace, Context *onfinish);
  void destroy(inodeno_t ino, Context *onfinish);

  void dispatch(Message *m);
};

#endif
