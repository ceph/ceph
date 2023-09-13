// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
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
#include <map>
#include <vector>

#include <boost/intrusive_ptr.hpp>

#include "include/buffer.h"

#include "common/ceph_context.h"

#include "erasure-code/ErasureCodeInterface.h"

class ErasureCodeBench {
  int in_size;
  int max_iterations;
  int erasures;
  int k;
  int m;

  std::string plugin;

  bool exhaustive_erasures;
  std::vector<int> erased;
  std::string workload;

  ceph::ErasureCodeProfile profile;

  bool verbose;
  boost::intrusive_ptr<CephContext> cct;
public:
  int setup(int argc, char** argv);
  int run();
  int decode_erasures(const std::map<int, ceph::buffer::list> &all_chunks,
		      const std::map<int, ceph::buffer::list> &chunks,
		      unsigned i,
		      unsigned want_erasures,
		      ErasureCodeInterfaceRef erasure_code);
  int decode();
  int encode();
};

#endif
