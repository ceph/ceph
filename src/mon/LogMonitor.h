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

#ifndef __LOGMONITOR_H
#define __LOGMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"
#include "PaxosService.h"

#include "include/LogEntry.h"

class MMonCommand;
class MLog;

class LogMonitor : public PaxosService {
private:
  bufferlist pending_inc;
  version_t log_version;

  void create_initial();
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool preprocess_query(Message *m);  // true if processed.
  bool prepare_update(Message *m);

  bool preprocess_log(MLog *m);
  bool prepare_log(MLog *m);
  void _updated_log(MLog *m, entity_inst_t who);

  struct C_Log : public Context {
    LogMonitor *logmon;
    MLog *ack;
    entity_inst_t who;
    C_Log(LogMonitor *p, MLog *a, entity_inst_t w) : logmon(p), ack(a), who(w) {}
    void finish(int r) {
      logmon->_updated_log(ack, who);
    }    
  };

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

 public:
  LogMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p), log_version(0) { }
  
  void tick();  // check state, take actions
};

#endif
