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

#include "include/common_fwd.h"
#include "include/elist.h"
#include "osdc/Filer.h"

#include <cstddef>
#include <map>

class CInode;
class MDSRank;

class RecoveryQueue {
public:
  explicit RecoveryQueue(MDSRank *mds_);

  void enqueue(CInode *in);
  void advance();
  void prioritize(CInode *in);   ///< do this inode now/soon

  void set_logger(PerfCounters *p) {logger=p;}

private:
  friend class C_MDC_Recover;

  void _start(CInode *in);  ///< start recovering this file
  void _recovered(CInode *in, int r, uint64_t size, utime_t mtime);

  size_t file_recover_queue_size = 0;
  size_t file_recover_queue_front_size = 0;

  elist<CInode*> file_recover_queue;   ///< the queue
  elist<CInode*> file_recover_queue_front;  ///< elevated priority items
  std::map<CInode*, bool> file_recovering; // inode -> need_restart

  MDSRank *mds;
  PerfCounters *logger = nullptr;
  Filer filer;
};

#endif // RECOVERY_QUEUE_H
