// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


TimePoint now() { return std::chrono::steady_clock::now(); }


// If for debugging purposes we need to TimePoints, this converts them
// into more easily read doubles in the unit of seconds. It also uses
// modulo to strip off the upper digits (keeps 5 to the left of the
// decimal point).
double fmt_tp(const TimePoint& t) {
  auto c = t.time_since_epoch().count();
  return uint64_t(c / 1000000.0 + 0.5) % 100000 / 1000.0;
}


