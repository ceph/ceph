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


#ifndef CEPH_Sem_Posix__H
#define CEPH_Sem_Posix__H

#include "common/ceph_mutex.h"

class Semaphore
{
  ceph::mutex m = ceph::make_mutex("Semaphore::m");
  ceph::condition_variable c;
  int count = 0;

  public:

  void Put()
  { 
    std::lock_guard l(m);
    count++;
    c.notify_all();
  }

  void Get() 
  {
    std::unique_lock l(m);
    while(count <= 0) {
      c.wait(l);
    }
    count--;
  }
};

#endif // !_Mutex_Posix_
