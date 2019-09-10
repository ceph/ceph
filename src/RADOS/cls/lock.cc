// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/buffer.h"

#include "cls/lock/cls_lock_ops.h"

#include "lock.h"

namespace RADOS::CLS::lock {
namespace cb = ceph::buffer;

WriteOp lock(std::string_view name, ClsLockType type,
             std::string_view cookie, std::string_view tag,
             std::string_view description,
             ceph::timespan duration, uint8_t flags) {
  WriteOp wop;
  cls_lock_lock_op op;
  op.name = name;
  op.type = type;
  op.cookie = cookie;
  op.tag = tag;
  op.description = description;
  op.duration = duration;
  op.flags = flags;
  cb::list in;
  encode(op, in);
  wop.exec("lock", "lock", in);
  return wop;
}

WriteOp unlock(std::string_view name, std::string_view cookie) {
  WriteOp wop;
  cls_lock_unlock_op op;
  op.name = name;
  op.cookie = cookie;
  bufferlist in;
  encode(op, in);

  wop.exec("lock", "unlock", in);
  return wop;
}

WriteOp break_lock(std::string_view name, std::string_view cookie,
                   const entity_name_t& locker) {
  WriteOp wop;
  cls_lock_break_op op;
  op.name = name;
  op.cookie = cookie;
  op.locker = locker;
  bufferlist in;
  encode(op, in);
  wop.exec("lock", "break_lock", in);
  return wop;
}

ReadOp list_locks(std::string_view oid, const IOContext& ioc,
		  std::vector<string>* locks, bs::error_code* oec) {
  ReadOp op;
  op.exec("lock", "list_locks", {},
	  [locks, oec](bs::error_code ec, cb::list bl) {
	    cls_lock_list_locks_reply ret;
	    if (!ec && locks) try {
		auto iter = cbegin(bl);
		decode(ret, iter);
		*locks = std::move(ret.locks);
	      } catch (const cb::error& err) {
		ec = err.code();
	      }
	    if (oec)
	      *oec = ec;
	  });
  return op;
}

ReadOp get_lock_info(std::string_view name,
		     bc::flat_map<rados::cls::lock::locker_id_t,
		                  rados::cls::lock::locker_info_t>* lockers,
		     ClsLockType* type, std::string* tag,
		     bs::error_code* oec) {
  ReadOp rop;
  cls_lock_get_info_op op;
  op.name = name;
  cb::list bl;
  encode(op, bl);
  rop.exec("lock", "get_info", bl,
	   [lockers, type, tag, oec](bs::error_code ec, cb::list bl) {
	     cls_lock_get_info_reply ret;
	     try {
	       auto iter = cbegin(bl);
	       decode(ret, iter);
	       if (lockers)
		 *lockers = std::move(ret.lockers);
	       if (type)
		 *type = std::move(ret.lock_type);
	       if (tag)
		 *tag = std::move(ret.tag);
	     } catch (const cb::error& err) {
	       ec = err.code();
	     }
	     if (oec)
	       *oec = ec;
	   });
  return rop;
}


WriteOp assert_locked(std::string_view name, ClsLockType type,
                      std::string_view cookie, std::string_view tag) {
  WriteOp wop;
  cls_lock_assert_op op;
  op.name = name;
  op.type = type;
  op.cookie = cookie;
  op.tag = tag;
  bufferlist in;
  encode(op, in);
  wop.exec("lock", "assert_locked", in);
  return wop;
}

WriteOp set_cookie(std::string_view name, ClsLockType type,
                   std::string_view cookie, std::string_view tag,
                   std::string_view new_cookie) {
  WriteOp wop;
  cls_lock_set_cookie_op op;
  op.name = name;
  op.type = type;
  op.cookie = cookie;
  op.tag = tag;
  op.new_cookie = new_cookie;
  bufferlist in;
  encode(op, in);
  wop.exec("lock", "set_cookie", in);
  return wop;
}
}
