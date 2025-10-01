// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab
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

#include "msg/msg_types.h"
#include "common/Formatter.h"

#include "cls/lock/cls_lock_ops.h"

using namespace rados::cls::lock;
using std::list;
using std::map;
using std::string;

static void generate_lock_id(locker_id_t& i, int n, const string& cookie)
{
  i.locker = entity_name_t::CLIENT(n);
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

list<cls_lock_lock_op> cls_lock_lock_op::generate_test_instances()
{
  list<cls_lock_lock_op> o;
  cls_lock_lock_op i;
  i.name = "name";
  i.type = ClsLockType::SHARED;
  i.cookie = "cookie";
  i.tag = "tag";
  i.description = "description";
  i.duration = utime_t(5, 0);
  i.flags = LOCK_FLAG_MAY_RENEW;
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

void cls_lock_unlock_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("cookie", cookie);
}

list<cls_lock_unlock_op> cls_lock_unlock_op::generate_test_instances()
{
  list<cls_lock_unlock_op> o;
  cls_lock_unlock_op i;
  i.name = "name";
  i.cookie = "cookie";
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

void cls_lock_break_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("cookie", cookie);
  f->dump_stream("locker") << locker;
}

list<cls_lock_break_op> cls_lock_break_op::generate_test_instances()
{
  list<cls_lock_break_op> o;
  cls_lock_break_op i;
  i.name = "name";
  i.cookie = "cookie";
  i.locker =  entity_name_t::CLIENT(1);
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

void cls_lock_get_info_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
}

list<cls_lock_get_info_op> cls_lock_get_info_op::generate_test_instances()
{
  list<cls_lock_get_info_op> o;
  cls_lock_get_info_op i;
  i.name = "name";
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

static void generate_test_addr(entity_addr_t& a, int nonce, int port)
{
  a.set_type(entity_addr_t::TYPE_LEGACY);
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
    f->dump_string("addr", info.addr.get_legacy_str());
    f->close_section();
  }
  f->close_section();
}

list<cls_lock_get_info_reply> cls_lock_get_info_reply::generate_test_instances()
{
  list<cls_lock_get_info_reply> o;
  cls_lock_get_info_reply i;
  i.lock_type = ClsLockType::SHARED;
  i.tag = "tag";
  locker_id_t id1, id2;
  entity_addr_t addr1, addr2;
  generate_lock_id(id1, 1, "cookie1");
  generate_test_addr(addr1, 10, 20);
  i.lockers[id1] = locker_info_t(utime_t(10, 0), addr1, "description1");
  generate_lock_id(id2, 2, "cookie2");
  generate_test_addr(addr2, 30, 40);
  i.lockers[id2] = locker_info_t(utime_t(20, 0), addr2, "description2");

  o.push_back(std::move(i));
  o.emplace_back();
  return o;
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

list<cls_lock_list_locks_reply> cls_lock_list_locks_reply::generate_test_instances()
{
  list<cls_lock_list_locks_reply> o;
  cls_lock_list_locks_reply i;
  i.locks.push_back("lock1");
  i.locks.push_back("lock2");
  i.locks.push_back("lock3");

  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

void cls_lock_assert_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", cls_lock_type_str(type));
  f->dump_string("cookie", cookie);
  f->dump_string("tag", tag);
}

list<cls_lock_assert_op> cls_lock_assert_op::generate_test_instances()
{
  list<cls_lock_assert_op> o;
  cls_lock_assert_op i;
  i.name = "name";
  i.type = ClsLockType::SHARED;
  i.cookie = "cookie";
  i.tag = "tag";
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

void cls_lock_set_cookie_op::dump(Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", cls_lock_type_str(type));
  f->dump_string("cookie", cookie);
  f->dump_string("tag", tag);
  f->dump_string("new_cookie", new_cookie);
}

list<cls_lock_set_cookie_op> cls_lock_set_cookie_op::generate_test_instances()
{
  list<cls_lock_set_cookie_op> o;
  cls_lock_set_cookie_op i;
  i.name = "name";
  i.type = ClsLockType::SHARED;
  i.cookie = "cookie";
  i.tag = "tag";
  i.new_cookie = "new cookie";
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}

