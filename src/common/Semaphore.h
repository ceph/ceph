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

class Semaphore
{
  Mutex m;
  Cond c;
  int count;

  public:

  Semaphore() : m("Semaphore::m")
  {
    count = 0;
  }

  void Put()
  { 
    m.Lock();
    count++;
    c.Signal();
    m.Unlock();
  }

  void Get() 
  { 
    m.Lock();
    while(count <= 0) {
      c.Wait(m);
    }
    count--;
    m.Unlock();
  }
};

#endif // !_Mutex_Posix_
