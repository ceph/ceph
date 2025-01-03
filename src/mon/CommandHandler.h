// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Ltd
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef COMMAND_HANDLER_H_
#define COMMAND_HANDLER_H_

#include <ostream>
#include <string_view>

class CommandHandler
{
public:
  /**
   * Parse true|yes|1 style boolean string from `bool_str`
   * `result` must be non-null.
   * `ss` will be populated with error message on error.
   *
   * @return 0 on success, else -EINVAL
   */
  int parse_bool(std::string_view str, bool* result, std::ostream& ss);
};

#endif
