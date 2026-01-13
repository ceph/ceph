// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef MGR_JSON_COMMAND_H_
#define MGR_JSON_COMMAND_H_

#include "MgrContext.h"
#include "common/ceph_json.h"

class JSONCommand : public Command
{
public:
  glz::generic json_result;

  void wait() override
  {
    Command::wait();

    if (0 != r) {
      return;
    }

    if (auto ec = glz::read_json(json_result, outbl.to_str()); ec) {
	r = -EINVAL;
    }
  }
};

#endif

