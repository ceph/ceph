// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <string>
#include "common/hobject.h"
#include "osd_types.h"

namespace ECInject {

  // Error inject interfaces
  std::string read_error(const ghobject_t& o, const int64_t type, const int64_t when, const int64_t duration);
  std::string write_error(const ghobject_t& o, const int64_t type, const int64_t when, const int64_t duration);
  std::string clear_read_error(const ghobject_t& o, const int64_t type);
  std::string clear_write_error(const ghobject_t& o, const int64_t type);
  bool test_read_error0(const ghobject_t& o);
  bool test_read_error1(const ghobject_t& o);
  bool test_write_error0(const hobject_t& o,const osd_reqid_t& reqid);
  bool test_write_error1(const ghobject_t& o);
  bool test_write_error2(const hobject_t& o);
  bool test_write_error3(const hobject_t& o);

} // ECInject
