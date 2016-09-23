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

//class C_MDC_Recover;
//
#ifndef RECOVERY_QUEUE_H
#define RECOVERY_QUEUE_H

#include <set>
#include <common/Mutex.h>
#include "CObject.h"

class CInode;
class MDSRank;
class PerfCounters;

class RecoveryQueue {
public:
  void enqueue(CInode *in);
  void advance();
  void prioritize(CInode *in);   ///< do this inode now/soon
  explicit RecoveryQueue(MDSRank *mds_);

  void set_logger(PerfCounters *p) {logger=p;}

private:
  Mutex lock;
  void _advance();
  bool _start(CInodeRef& in);  ///< start recovering this file

  std::set<CInode*> file_recover_queue;   ///< the queue
  std::set<CInode*> file_recover_queue_front;  ///< elevated priority items
  std::set<CInode*> file_recovering;
  void _recovered(CInodeRef& in, int r, uint64_t size, utime_t mtime);
  MDSRank *mds;
  PerfCounters *logger;

  MDSContextBase *kick;

  friend class C_MDC_Recovered;
  friend class C_MDC_RecoveryKick;
};

#endif // RECOVERY_QUEUE_H
