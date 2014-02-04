// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_BENCHMARK_H
#define CEPH_ERASURE_CODE_BENCHMARK_H

#include <string>

using namespace std;

class ErasureCodeBench {
  int in_size;
  int max_iterations;
  string plugin;
  int erasures;
  string workload;
  map<string,string> parameters;
public:
  int setup(int argc, char** argv);
  int run();
  int decode();
  int encode();
};

#endif
