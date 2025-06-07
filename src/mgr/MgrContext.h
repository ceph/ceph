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
#include "common/Cond.h"
#include "mon/MonClient.h"

#include <boost/json.hpp>

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

  void run(MonClient *monc, const std::string &command, const ceph::buffer::list &inbl)
  {
    monc->start_mon_command({command}, inbl,
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
  boost::json::value json_result;

  void wait() override
  {
    Command::wait();

    if (0 != r) {
      return;
    }

    boost::system::error_code ec;
    if (json_result = boost::json::parse(outbl.to_str(), ec); ec) {
	r = -EINVAL;
    }
  }
};

#endif

