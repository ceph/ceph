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

  // lookups
  hash_map<inodeno_t, Context*>         pending_lookup;
  hash_map<inodeno_t, vector<Anchor>*>  pending_lookup_trace;

  // updates
  hash_map<inodeno_t, Context*>  pending_op;

  void handle_anchor_reply(class MAnchor *m);  


public:
  AnchorClient(Messenger *ms, MDSMap *mm) : messenger(ms), mdsmap(mm) {}
  
  void dispatch(Message *m);

  // async user interface
  void lookup(inodeno_t ino, vector<Anchor>& trace, Context *onfinish);

  void prepare_create(inodeno_t ino, vector<Anchor>& trace, Context *onfinish);
  void commit_create(inodeno_t ino);

  void prepare_destroy(inodeno_t ino, Context *onfinish);
  void commit_destroy(inodeno_t ino);

  void prepare_update(inodeno_t ino, vector<Anchor>& trace, Context *onfinish);
  void commit_update(inodeno_t ino);


};

#endif
