// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


#include <mutex>


namespace crimson {
  namespace dmclock {
    typedef std::unique_lock<std::mutex> Lock;
    typedef std::lock_guard<std::mutex> Guard;

    void debugger();
  }
}
