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
#include "cls/lock/cls_lock_ops.h"

using namespace rados::cls::lock;

static void generate_lock_id(locker_id_t& i, int n, const string& cookie)
{
  i.locker = entity_name_t(entity_name_t::CLIENT(n));
  i.cookie = cookie;
}

void cls_lock_lock_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", cls_lock_type_str(type));
  f->dump_string("cookie", cookie);
  f->dump_string("tag", tag);
  f->dump_string("description", description);
  f->dump_stream("duration") << duration;
  f->dump_int("flags", (int)flags);
}

void cls_lock_lock_op::generate_test_instances(list<cls_lock_lock_op*>& o)
{
  cls_lock_lock_op *i = new cls_lock_lock_op;
  i->name = "name";
  i->type = LOCK_SHARED;
  i->cookie = "cookie";
  i->tag = "tag";
  i->description = "description";
  i->duration = utime_t(5, 0);
  i->flags = LOCK_FLAG_RENEW;
  o.push_back(i);
  o.push_back(new cls_lock_lock_op);
}

void cls_lock_unlock_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("cookie", cookie);
}

void cls_lock_unlock_op::generate_test_instances(list<cls_lock_unlock_op*>& o)
{
  cls_lock_unlock_op *i = new cls_lock_unlock_op;
  i->name = "name";
  i->cookie = "cookie";
  o.push_back(i);
  o.push_back(new cls_lock_unlock_op);
}

void cls_lock_break_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("cookie", cookie);
  f->dump_stream("locker") << locker;
}

void cls_lock_break_op::generate_test_instances(list<cls_lock_break_op*>& o)
{
  cls_lock_break_op *i = new cls_lock_break_op;
  i->name = "name";
  i->cookie = "cookie";
  i->locker =  entity_name_t(entity_name_t::CLIENT(1));
  o.push_back(i);
  o.push_back(new cls_lock_break_op);
}

void cls_lock_get_info_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
}

void cls_lock_get_info_op::generate_test_instances(list<cls_lock_get_info_op*>& o)
{
  cls_lock_get_info_op *i = new cls_lock_get_info_op;
  i->name = "name";
  o.push_back(i);
  o.push_back(new cls_lock_get_info_op);
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

void cls_lock_get_info_reply::dump(Formatter *f) const
{
  f->dump_string("lock_type", cls_lock_type_str(lock_type));
  f->dump_string("tag", tag);
  f->open_array_section("lockers");
  map<locker_id_t, locker_info_t>::const_iterator iter;
  for (iter = lockers.begin(); iter != lockers.end(); ++iter) {
    const locker_id_t& id = iter->first;
    const locker_info_t& info = iter->second;
    f->open_object_section("object");
    f->dump_stream("locker") << id.locker;
    f->dump_string("description", info.description);
    f->dump_string("cookie", id.cookie);
    f->dump_stream("expiration") << info.expiration;
    f->dump_stream("addr") << info.addr;
    f->close_section();
  }
  f->close_section();
}

void cls_lock_get_info_reply::generate_test_instances(list<cls_lock_get_info_reply*>& o)
{
  cls_lock_get_info_reply *i = new cls_lock_get_info_reply;
  i->lock_type = LOCK_SHARED;
  i->tag = "tag";
  locker_id_t id1, id2;
  entity_addr_t addr1, addr2;
  generate_lock_id(id1, 1, "cookie1");
  generate_test_addr(addr1, 10, 20);
  i->lockers[id1] = locker_info_t(utime_t(10, 0), addr1, "description1");
  generate_lock_id(id2, 2, "cookie2");
  generate_test_addr(addr2, 30, 40);
  i->lockers[id2] = locker_info_t(utime_t(20, 0), addr2, "description2");

  o.push_back(i);
  o.push_back(new cls_lock_get_info_reply);
}

void cls_lock_list_locks_reply::dump(Formatter *f) const
{
  list<string>::const_iterator iter;
  f->open_array_section("locks");
  for (iter = locks.begin(); iter != locks.end(); ++iter) {
    f->open_array_section("object");
    f->dump_string("lock", *iter);
    f->close_section();
  }
  f->close_section();
}

void cls_lock_list_locks_reply::generate_test_instances(list<cls_lock_list_locks_reply*>& o)
{
  cls_lock_list_locks_reply *i = new cls_lock_list_locks_reply;
  i->locks.push_back("lock1");
  i->locks.push_back("lock2");
  i->locks.push_back("lock3");

  o.push_back(i);
  o.push_back(new cls_lock_list_locks_reply);
}

void cls_lock_assert_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", cls_lock_type_str(type));
  f->dump_string("cookie", cookie);
  f->dump_string("tag", tag);
}

void cls_lock_assert_op::generate_test_instances(list<cls_lock_assert_op*>& o)
{
  cls_lock_assert_op *i = new cls_lock_assert_op;
  i->name = "name";
  i->type = LOCK_SHARED;
  i->cookie = "cookie";
  i->tag = "tag";
  o.push_back(i);
  o.push_back(new cls_lock_assert_op);
}

void cls_lock_set_cookie_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", cls_lock_type_str(type));
  f->dump_string("cookie", cookie);
  f->dump_string("tag", tag);
  f->dump_string("new_cookie", new_cookie);
}

void cls_lock_set_cookie_op::generate_test_instances(list<cls_lock_set_cookie_op*>& o)
{
  cls_lock_set_cookie_op *i = new cls_lock_set_cookie_op;
  i->name = "name";
  i->type = LOCK_SHARED;
  i->cookie = "cookie";
  i->tag = "tag";
  i->new_cookie = "new cookie";
  o.push_back(i);
  o.push_back(new cls_lock_set_cookie_op);
}

