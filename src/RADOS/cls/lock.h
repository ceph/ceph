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

#ifndef CEPH_RADOS_CLS_LOCK_H
#define CEPH_RADOS_CLS_LOCK_H

#include <cstdint>
#include <string_view>

#include <boost/container/flat_map.hpp>
#include <boost/system/error_code.hpp>

#include "include/RADOS/RADOS.hpp"

#include "common/ceph_time.h"

#include "cls/lock/cls_lock_types.h"

namespace RADOS::CLS::lock {
namespace bc = boost::container;
namespace bs = boost::system;

WriteOp lock(std::string_view name, ClsLockType type,
	     std::string_view cookie, std::string_view tag,
	     std::string_view description,
	     ceph::timespan duration, std::uint8_t flags);

WriteOp unlock(std::string_view name, std::string_view cookie);

WriteOp break_lock(std::string_view name, std::string_view cookie,
		      const entity_name_t& locker);

ReadOp list_locks(std::string_view oid, const IOContext& ioc,
		  std::vector<string>* locks, bs::error_code* oec = nullptr);

ReadOp get_lock_info(std::string_view name,
		     bc::flat_map<rados::cls::lock::locker_id_t,
		                  rados::cls::lock::locker_info_t>* lockers,
		     ClsLockType* type, std::string* tag,
		     bs::error_code* oec = nullptr);

WriteOp assert_locked(std::string_view name, ClsLockType type,
		      std::string_view cookie, std::string_view tag);

WriteOp set_cookie(std::string_view name, ClsLockType type,
		   std::string_view cookie, std::string_view tag,
		   std::string_view new_cookie);
}

#endif // CEPH_RADOS_CLS_LOCK_H
