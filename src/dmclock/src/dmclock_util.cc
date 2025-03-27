// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#include <signal.h>

#include <iomanip>
#include <sstream>

#include "dmclock_util.h"


std::string crimson::dmclock::format_time(const Time& time, unsigned modulo) {
  long subtract = long(time / modulo) * modulo;
  std::stringstream ss;
  ss << std::fixed << std::setprecision(4) << (time - subtract);
  return ss.str();
}


void crimson::dmclock::debugger() {
#ifndef _WIN32
  raise(SIGCONT);
#else
  DebugBreak();
#endif
}
