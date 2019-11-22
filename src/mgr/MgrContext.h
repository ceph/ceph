// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef MGR_CONTEXT_H_
#define MGR_CONTEXT_H_

#include <memory>

#include "common/ceph_json.h"
#include "mon/MonClient.h"

class Command
{
protected:
  C_SaferCond cond;
public:
  ceph::buffer::list outbl;
  std::string outs;
  int r;

  void run(MonClient *monc, const std::string &command)
  {
    monc->start_mon_command({command}, {},
        &outbl, &outs, &cond);
  }

  virtual void wait()
  {
    r = cond.wait();
  }

  virtual ~Command() {}
};


class JSONCommand : public Command
{
public:
  json_spirit::mValue json_result;

  void wait() override
  {
    Command::wait();

    if (r == 0) {
      bool read_ok = json_spirit::read(
          outbl.to_str(), json_result);
      if (!read_ok) {
        r = -EINVAL;
      }
    }
  }
};

#endif

