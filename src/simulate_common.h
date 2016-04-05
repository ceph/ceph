// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;


extern double fmt_tp(const TimePoint&);
extern TimePoint now();
