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


#include "mdstypes.h"
#include "SimpleLock.h"
#include "Locker.h"

#include "config.h"
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " " << this << " "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_derr << dbeginl << g_clock.now() << " " << this << " "

ClientLease *MDSCacheObject::add_client_lease(int c, int mask) 
{
  ClientLease *l;
  if (client_lease_map.count(c))
    l = client_lease_map[c];
  else {
    if (client_lease_map.empty())
      get(PIN_CLIENTLEASE);
    l = client_lease_map[c] = new ClientLease(c, this);
  }
  
  int adding = ~l->mask & mask;
  dout(20) << " had " << l->mask << " adding " << mask 
	   << " -> " << adding
	   << " ... now " << (l->mask | mask)
	   << dendl;
  int b = 0;
  while (adding) {
    if (adding & 1) {
      SimpleLock *lock = get_lock(1 << b);
      if (lock) {
	lock->get_client_lease();
	dout(20) << "get_client_lease on " << (1 << b) << " " << *lock << dendl;
      }
    }
    b++;
    adding = adding >> 1;
  }
  l->mask |= mask;
  
  return l;
}

int MDSCacheObject::remove_client_lease(ClientLease *l, int mask, Locker *locker) 
{
  assert(l->parent == this);

  list<SimpleLock*> to_gather;

  int removing = l->mask & mask;
  dout(20) << "had " << l->mask << " removing " << mask << " -> " << removing
	   << " ... now " << (l->mask & ~mask) << dendl;
  int b = 0;
  while (removing) {
    if (removing & 1) {
      SimpleLock *lock = get_lock(1 << b);
      if (lock) {
	lock->put_client_lease();
	dout(20) << "put_client_lease on " << (1 << b) << " " << *lock << dendl;
	if (lock->get_num_client_lease() == 0 && !lock->is_stable())
	  to_gather.push_back(lock);
      }
    }
    b++;
    removing = removing >> 1;
  }

  l->mask &= ~mask;
  int rc = l->mask;

  if (rc == 0) {
    dout(20) << "removing lease for client" << l->client << dendl;
    client_lease_map.erase(l->client);
    delete l;
    if (client_lease_map.empty())
      put(PIN_CLIENTLEASE);
  }

  // do pending gathers.
  while (!to_gather.empty()) {
    locker->eval_gather(to_gather.front());
    to_gather.pop_front();
  }
   
  return rc;
}

