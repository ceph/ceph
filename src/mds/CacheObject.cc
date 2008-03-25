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

#include "config.h"
#define  dout(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_dout << dbeginl << g_clock.now() << " " << this << " "
#define  derr(l)    if (l<=g_conf.debug || l <= g_conf.debug_mds) *_derr << dbeginl << g_clock.now() << " " << this << " "

ClientReplica *MDSCacheObject::add_client_replica(int c, int mask) 
{
  ClientReplica *r;
  if (client_replica_map.count(c))
    r = client_replica_map[c];
  else {
    if (client_replica_map.empty())
      get(PIN_CLIENTREPLICA);
    r = client_replica_map[c] = new ClientReplica(c, this);
  }
  
  int adding = ~r->mask & mask;
  dout(10) << " had " << r->mask << " adding " << mask << " -> new " << adding << dendl;
  int b = 0;
  while (adding) {
    if (adding & 1) {
      SimpleLock *lock = get_lock(1 << b);
      if (lock) {
	lock->get_client_lease();
	dout(10) << "get_client_lease on " << (1 << b) << " " << *lock << dendl;
      }
    }
    b++;
    adding = adding >> 1;
  }
  r->mask |= mask;
  
  return r;
}

int MDSCacheObject::remove_client_replica(ClientReplica *r, int mask) 
{
  assert(r->parent == this);
  
  int removing = r->mask & mask;
  dout(10) << "had " << r->mask << " removing " << mask << " -> " << removing << dendl;
  int b = 0;
  while (removing) {
    if (removing & 1) {
      SimpleLock *lock = get_lock(1 << b);
      if (lock) {
	lock->put_client_lease();
	dout(10) << "put_client_lease on " << (1 << b) << " " << *lock << dendl;
      }
    }
    b++;
    removing = removing >> 1;
  }

  r->mask &= ~mask;
  if (r->mask)
    return r->mask;

  // remove!
  client_replica_map.erase(r->client);
  delete r;
  if (client_replica_map.empty())
    put(PIN_CLIENTREPLICA);
  return 0;
}

