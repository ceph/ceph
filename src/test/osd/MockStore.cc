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
#include "common/errno.h"
#include <filesystem>
#include <random>
#include <sstream>
#include <iomanip>
#include <iostream>

std::shared_ptr<MockStore> MockStore::create(CephContext *cct, int osd_id) {
  // Generate unique data directory for this OSD's store
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;
  uint64_t random_num = dis(gen);
  
  std::ostringstream oss;
  oss << "memstore_test_osd" << osd_id << "_" << std::hex << std::setfill('0') 
      << std::setw(16) << random_num;
  std::string data_dir = oss.str();
  
  // Create data directory
  int r = ::mkdir(data_dir.c_str(), 0777);
  if (r < 0) {
    r = -errno;
    std::cerr << "MockStore::create: unable to create " 
              << data_dir << ": " << cpp_strerror(r) << std::endl;
    return nullptr;
  }
  
  // Create and initialize the store
  auto store = std::make_shared<MockStore>(cct, data_dir);
  if (store->mkfs() != 0) {
    std::cerr << "MockStore::create: mkfs failed for OSD " << osd_id << std::endl;
    // Clean up the directory
    try {
      std::filesystem::remove_all(data_dir);
    } catch (const std::filesystem::filesystem_error& e) {
      std::cerr << "MockStore::create: failed to remove " 
                << data_dir << ": " << e.what() << std::endl;
    }
    return nullptr;
  }
  
  if (store->mount() != 0) {
    std::cerr << "MockStore::create: mount failed for OSD " << osd_id << std::endl;
    // Clean up the directory
    try {
      std::filesystem::remove_all(data_dir);
    } catch (const std::filesystem::filesystem_error& e) {
      std::cerr << "MockStore::create: failed to remove " 
                << data_dir << ": " << e.what() << std::endl;
    }
    return nullptr;
  }
  
  return store;
}

// Made with Bob
