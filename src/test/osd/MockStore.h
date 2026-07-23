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

#include "os/memstore/MemStore.h"
#include <map>
#include <mutex>

/**
 * MockMemStore - MemStore wrapper with error injection capabilities
 *
 * This class extends MemStore to allow injecting read errors for specific
 * objects. This is useful for testing error handling in EC recovery scenarios.
 *
 * Error injection is one-time: after an error is injected and returned,
 * it is automatically cleared so subsequent reads succeed.
 */
class MockStore : public MemStore {
private:
  /// Map of object -> error code to inject on next read
  std::map<ghobject_t, int> injected_read_errors;
  
  /// Mutex to protect injected_read_errors map
  std::mutex error_injection_mutex;

public:
  MockStore(CephContext *cct, const std::string& path)
    : MemStore(cct, path) {}
  
  ~MockStore() override = default;

  /**
   * Inject a read error for a specific object.
   * The error will be returned on the next read() call for this object,
   * then automatically cleared.
   *
   * @param oid The object to inject an error for
   * @param error_code The error code to return (should be negative, e.g., -EIO)
   */
  void inject_read_error(const ghobject_t& oid, int error_code) {
    std::lock_guard<std::mutex> lock(error_injection_mutex);
    injected_read_errors[oid] = error_code;
  }

  /**
   * Clear any injected read error for a specific object.
   *
   * @param oid The object to clear the error for
   */
  void clear_read_error(const ghobject_t& oid) {
    std::lock_guard<std::mutex> lock(error_injection_mutex);
    injected_read_errors.erase(oid);
  }

  /**
   * Clear all injected read errors.
   */
  void clear_all_read_errors() {
    std::lock_guard<std::mutex> lock(error_injection_mutex);
    injected_read_errors.clear();
  }

  /**
   * Override read() to check for injected errors before calling parent.
   * If an error is injected for this object, return it and clear the injection.
   * Otherwise, call the parent MemStore::read().
   */
  int read(
    CollectionHandle &c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    ceph::buffer::list& bl,
    uint32_t op_flags = 0) override
  {
    // Check if we should inject an error for this object
    int error_code = 0;
    {
      std::lock_guard<std::mutex> lock(error_injection_mutex);
      auto it = injected_read_errors.find(oid);
      if (it != injected_read_errors.end()) {
        error_code = it->second;
        // Clear the error after using it (one-time injection)
        injected_read_errors.erase(it);
      }
    }

    // If we have an injected error, return it
    if (error_code != 0) {
      return error_code;
    }

    // Otherwise, call the parent implementation
    return MemStore::read(c, oid, offset, len, bl, op_flags);
  }
};
