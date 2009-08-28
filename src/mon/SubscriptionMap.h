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

#ifndef __MON_SUBSCRIPTIONMAP_H
#define __MON_SUBSCRIPTIONMAP_H

#include "msg/msg_types.h"

struct SubscriptionMap {
  struct sub_info {
    version_t last;
    utime_t until;
  };
  map<entity_inst_t, sub_info> subs;

  void subscribe(entity_inst_t a, version_t h, utime_t u) {
    if (!subs.count(a))
      subs[a].last = h;
    subs[a].until = u;
  }

  void trim(utime_t now) {
    map<entity_inst_t, sub_info>::iterator p = subs.begin();
    while (p != subs.end())
      if (p->second.until < now)
	subs.erase(p++);
      else
	p++;
  }
};

#endif
