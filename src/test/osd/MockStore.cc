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

#include "MockStore.h"
#include <iostream>

std::shared_ptr<MockStore> MockStore::create(CephContext *cct, int osd_id) {
  // Without a path MemStore keeps its contents in memory only: mount() and
  // umount() neither load nor save them, and there is nothing to mkfs().
  auto store = std::make_shared<MockStore>(cct, "");

  if (store->mount() != 0) {
    std::cerr << "MockStore::create: mount failed for OSD " << osd_id << std::endl;
    return nullptr;
  }

  return store;
}
