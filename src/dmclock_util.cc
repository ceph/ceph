// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <signal.h>

#include <iomanip>
#include <sstream>

#include "dmclock_util.h"


std::string crimson::dmclock::format_time(const Time& time, uint modulo) {
  long subtract = long(time / modulo) * modulo;
  std::stringstream ss;
  ss << std::fixed << std::setprecision(4) << time - subtract;
  return ss.str();
}


void crimson::dmclock::debugger() {
    raise(SIGCONT);
}
