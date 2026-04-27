// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef  __NVMEOFGWUTILS_H__
#define  __NVMEOFGWUTILS_H__
#include "mon/NVMeofGwTypes.h"
#include <list>

// utility for diffing nvmeof subsystems changes
void determine_subsystem_changes(const BeaconSubsystems& old_subsystems,
                                BeaconSubsystems& new_subsystems);

#endif
