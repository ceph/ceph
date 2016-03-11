// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <unistd.h>
#include <assert.h>
#include <sys/time.h>

#include <limits>


namespace crimson {
  namespace dmclock {
    // we're using double to represent time, but we could change it by
    // changing the following declarations (and by making sure a min
    // function existed)
    using Time = double;
    static const Time TimeZero = 0.0;
    static const Time TimeMax = std::numeric_limits<Time>::max();


    inline Time get_time() {
      struct timeval now;
      assert(0 == gettimeofday(&now, NULL));
      return now.tv_sec + (now.tv_usec / 1000000.0);
    }

    std::string format_time(const Time& time, uint modulo = 1000);

    void debugger();
  }
}
