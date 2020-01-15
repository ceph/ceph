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

#ifndef CEPH_RADOS_CLS_OTP_H
#define CEPH_RADOS_CLS_OTP_H

#include <string>
#include <string_view>
#include <vector>

#include <boost/system/error_code.hpp>

#include "include/RADOS/RADOS.hpp"
#include "include/expected.hpp"

#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/ceph_time.h"

#include "cls/otp/cls_otp_types.h"

namespace RADOS::CLS::OTP {
namespace bs = boost::system;
inline constexpr std::size_t TOKEN_LEN = 16;
using token = std::array<char, TOKEN_LEN + 1>;


void create(WriteOp& wop, rados::cls::otp::otp_info_t const& config);
void remove(WriteOp& wop, std::string_view id);

token random_token(CephContext* cct);

ReadOp check(std::string_view id,
	     std::string_view val,
	     token const& t);

ReadOp get_result(token const& t,
		  rados::cls::otp::otp_check_t* res,
		  bs::error_code* oec = nullptr);

void get(ReadOp& rop, std::vector<std::string> const* ids, bool get_all,
	 std::vector<rados::cls::otp::otp_info_t>* res,
	 bs::error_code* oec = nullptr);

ReadOp get(std::vector<std::string> const* ids, bool get_all,
	   std::vector<rados::cls::otp::otp_info_t>* res,
	   bs::error_code* oec = nullptr);

ReadOp get_current_time(ceph::real_time* t, bs::error_code* oec = nullptr);


void set(WriteOp& rados_op,
	 std::vector<rados::cls::otp::otp_info_t> const& entries);
}

#endif // CEPH_RADOS_CLS_OTP_H
