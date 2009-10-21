// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/*
 * The Monmap Monitor is used to track the monitors in the cluster.
 */

#ifndef __MONMAPMONITOR_H
#define __MONMAPMONITOR_H

#include <map>
#include <set>

using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "PaxosService.h"
#include "MonMap.h"

class MMonGetMap;
class MMonMap;
class MMonCommand;

class MonmapMonitor : public PaxosService {
 public:
  MonmapMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p) {}
  MonMap pending_map; //the pending map awaiting passage

  void create_initial(bufferlist& bl);

  bool update_from_paxos();

  void create_pending();

  void encode_pending(bufferlist& bl);


  bool preprocess_query(PaxosServiceMessage *m);
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);


  /*
   * Since monitors are pretty
   * important, this implementation will just write 0.0.
   */
  bool should_propose(double& delay);

  void committed();

  void tick();

 private:
  bufferlist monmap_bl;
};


#endif
