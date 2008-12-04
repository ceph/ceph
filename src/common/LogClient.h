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

#ifndef __LOGCLIENT_H
#define __LOGCLIENT_H

#include "msg/Dispatcher.h"

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "common/ThreadPool.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"

#include "mon/MonMap.h"

#include "os/ObjectStore.h"

#include "common/DecayCounter.h"

#include "include/LogEntry.h"

#include <map>
using namespace std;

#include <ext/hash_map>
#include <ext/hash_set>
using namespace __gnu_cxx;


class Messenger;
class Message;
class Logger;
class ObjectStore;
class OSDMap;
class MLog;

class LogClient {
  Messenger *messenger;
  MonMap *monmap;
 public:

  // -- log --
  Mutex log_lock;
  deque<LogEntry> log_queue;
  version_t last_log;

  void log(__u8 level, string s);
  void send_log();
  void handle_log(MLog *m);

  LogClient(Messenger *m, MonMap *mm) : messenger(m), monmap(mm), 
                                        log_lock("LogClient::log_lock"), last_log(0) {}
};

#endif
