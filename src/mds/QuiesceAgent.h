/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM, Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once
#include "QuiesceDb.h"

class QuiesceAgent {
  // subscribe to the QM map
  // keeps runtime version of the map
  // notifies the QM when runtime version is different from the last know requested version
};