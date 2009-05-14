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

#ifndef __CLASSMONITOR_H
#define __CLASSMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"
#include "PaxosService.h"
#include "mon/Monitor.h"

#include "include/ClassEntry.h"

class MMonCommand;
class MClass;

class ClassMonitor : public PaxosService {
private:
  multimap<utime_t,ClassLibraryIncremental> pending_class;
  ClassList pending_list, list;

  void create_initial(bufferlist& bl);
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool preprocess_query(Message *m);  // true if processed.
  bool prepare_update(Message *m);

  bool preprocess_class(MClass *m);
  bool prepare_class(MClass *m);
  void _updated_class(MClass *m, entity_inst_t who);

  struct C_Class : public Context {
    ClassMonitor *classmon;
    MClass *ack;
    entity_inst_t who;
    C_Class(ClassMonitor *p, MClass *a, entity_inst_t w) : classmon(p), ack(a), who(w) {}
    void finish(int r) {
      classmon->_updated_class(ack, who);
    }    
  };
  struct C_ClassMonCmd : public Context {
    Monitor *mon;
    MMonCommand *m;
    string rs;
    C_ClassMonCmd(Monitor *monitor, MMonCommand *m_, string& s) : 
      mon(monitor), m(m_), rs(s) {}
    void finish(int r) {
      mon->reply_command(m, 0, rs);
    }
  };

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);
  bool store_impl(ClassLibrary& info, ClassImpl& impl);
 public:
  ClassMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p) { }
  void handle_request(MClass *m);
  
  void tick();  // check state, take actions
};

#endif
