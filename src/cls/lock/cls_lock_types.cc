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

#include "include/types.h"
#include "msg/msg_types.h"
#include "common/Formatter.h"

#include "cls/lock/cls_lock_types.h"

using namespace rados::cls::lock;

static void generate_lock_id(locker_id_t& i, int n, const string& cookie)
{
  i.locker = entity_name_t(entity_name_t::CLIENT(n));
  i.cookie = cookie;
}

void locker_id_t::dump(Formatter *f) const
{
  f->dump_stream("locker") << locker;
  f->dump_string("cookie", cookie);
}

void locker_id_t::generate_test_instances(list<locker_id_t*>& o)
{
  locker_id_t *i = new locker_id_t;
  generate_lock_id(*i, 1, "cookie");
  o.push_back(i);
  o.push_back(new locker_id_t);
}

void locker_info_t::dump(Formatter *f) const
{
  f->dump_stream("expiration") << expiration;
  f->dump_stream("addr") << addr;
  f->dump_string("description", description);
}

static void generate_test_addr(entity_addr_t& a, int nonce, int port)
{
  a.set_nonce(nonce);
  a.set_family(AF_INET);
  a.set_in4_quad(0, 127);
  a.set_in4_quad(1, 0);
  a.set_in4_quad(2, 1);
  a.set_in4_quad(3, 2);
  a.set_port(port);
}

void locker_info_t::generate_test_instances(list<locker_info_t*>& o)
{
  locker_info_t *i = new locker_info_t;
  i->expiration = utime_t(5, 0);
  generate_test_addr(i->addr, 1, 2);
  i->description = "description";
  o.push_back(i);
  o.push_back(new locker_info_t);
}

