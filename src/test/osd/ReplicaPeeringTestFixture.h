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

#include "test/osd/PeeringTestFixture.h"

class ReplicaPeeringTestFixture : public PeeringTestFixture {
public:
  ReplicaPeeringTestFixture();

  // Recover an object to every acting OSD that is missing it: pull it to the
  // primary if the primary is missing it, then push it to the replicas.
  void run_recovery(const std::string& obj_name);
};
