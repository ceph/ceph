// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include "common/ostream_temp.h"

//MockLog - simple stub
class MockLog : public LoggerSinkSet {
 public:
  void debug(std::stringstream& s) final
  {
    std::cout << "\n<<debug>> " << s.str() << std::endl;
  }

  void info(std::stringstream& s) final
  {
    std::cout << "\n<<info>> " << s.str() << std::endl;
  }

  void sec(std::stringstream& s) final
  {
    std::cout << "\n<<sec>> " << s.str() << std::endl;
  }

  void warn(std::stringstream& s) final
  {
    std::cout << "\n<<warn>> " << s.str() << std::endl;
  }

  void error(std::stringstream& s) final
  {
    err_count++;
    std::cout << "\n<<error>> " << s.str() << std::endl;
  }

  OstreamTemp info() final { return OstreamTemp(CLOG_INFO, this); }
  OstreamTemp warn() final { return OstreamTemp(CLOG_WARN, this); }
  OstreamTemp error() final { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp sec() final { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp debug() final { return OstreamTemp(CLOG_DEBUG, this); }

  void do_log(clog_type prio, std::stringstream& ss) final
  {
    switch (prio) {
      case CLOG_DEBUG:
        debug(ss);
        break;
      case CLOG_INFO:
        info(ss);
        break;
      case CLOG_SEC:
        sec(ss);
        break;
      case CLOG_WARN:
        warn(ss);
        break;
      case CLOG_ERROR:
      default:
        error(ss);
        break;
    }
  }

  void do_log(clog_type prio, const std::string& ss) final
  {
    switch (prio) {
      case CLOG_DEBUG:
        debug() << ss;
        break;
      case CLOG_INFO:
        info() << ss;
        break;
      case CLOG_SEC:
        sec() << ss;
        break;
      case CLOG_WARN:
        warn() << ss;
        break;
      case CLOG_ERROR:
      default:
        error() << ss;
        break;
    }
  }

  virtual ~MockLog() {}

  int err_count{0};
  int expected_err_count{0};
  void set_expected_err_count(int c) { expected_err_count = c; }
};

