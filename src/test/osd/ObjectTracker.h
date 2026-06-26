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

#include <map>
#include <string>
#include <vector>
#include <optional>
#include "include/buffer.h"
#include "osd/osd_types.h"

/**
 * ObjectTracker - Track object operations for testing
 *
 * This class tracks data writes, attribute writes, and omap operations
 * for objects in the test framework. It maintains the expected state
 * of objects based on operations performed, allowing tests to verify
 * that the actual state matches expectations.
 *
 * The tracker is designed to work with the PGBackendTestFixture framework
 * and can be used to verify object consistency after peering, recovery,
 * and failover scenarios.
 */
class PGBackendTestFixture;

class ObjectTracker {
  friend class PGBackendTestFixture;

public:
  /**
   * Information about a data write operation
   */
  struct DataWrite {
    uint64_t offset;
    uint64_t length;
    std::string data;
    eversion_t version;  // Version when this write occurred
    
    DataWrite(uint64_t off, uint64_t len, const std::string& d, const eversion_t& v)
      : offset(off), length(len), data(d), version(v) {}
  };
  
  /**
   * Information about an attribute write operation
   */
  struct AttributeWrite {
    std::string name;
    std::string value;
    eversion_t version;  // Version when this write occurred
    
    AttributeWrite() = default;
    AttributeWrite(const std::string& n, const std::string& v, const eversion_t& ver)
      : name(n), value(v), version(ver) {}
  };
  
  /**
   * Information about an omap operation
   */
  struct OMapWrite {
    std::string key;
    std::string value;
    eversion_t version;  // Version when this write occurred
    
    OMapWrite() = default;
    OMapWrite(const std::string& k, const std::string& v, const eversion_t& ver)
      : key(k), value(v), version(ver) {}
  };
  
  /**
   * Complete state of an object
   */
  struct ObjectState {
    std::string object_name;
    uint64_t size = 0;
    bool exists = false;
    eversion_t version;  // Current version of the object
    
    // Data writes in chronological order
    std::vector<DataWrite> data_writes;
    
    // Attributes (excluding OI which is tracked separately)
    std::map<std::string, AttributeWrite> attributes;
    
    // OMap entries
    std::map<std::string, OMapWrite> omap_entries;
    
    ObjectState() = default;
    explicit ObjectState(const std::string& name) : object_name(name) {}
  };

private:
  // Map from object name to its tracked state
  std::map<std::string, ObjectState> objects_;

public:
  ObjectTracker() = default;

  /**
   * Get the tracked state for an object
   *
   * @param obj_name Name of the object
   * @return Pointer to ObjectState if tracked, nullptr otherwise
   */
  const ObjectState* get_object_state(const std::string& obj_name) const;
  
  /**
   * Get the expected data for an object at a given offset and length
   *
   * This reconstructs the data by applying all writes in order.
   *
   * @param obj_name Name of the object
   * @param offset Offset to read from
   * @param length Length to read
   * @return Expected data, or empty string if object doesn't exist
   */
  std::string get_expected_data(
    const std::string& obj_name,
    uint64_t offset,
    uint64_t length) const;
  
  /**
   * Get the expected value for an attribute
   *
   * @param obj_name Name of the object
   * @param attr_name Name of the attribute
   * @return Expected value, or nullopt if attribute doesn't exist
   */
  std::optional<std::string> get_expected_attribute(
    const std::string& obj_name,
    const std::string& attr_name) const;
  
  /**
   * Get the expected value for an omap key
   *
   * @param obj_name Name of the object
   * @param key OMap key
   * @return Expected value, or nullopt if key doesn't exist
   */
  std::optional<std::string> get_expected_omap(
    const std::string& obj_name,
    const std::string& key) const;
  
  /**
   * Check if an object exists in the tracker
   *
   * @param obj_name Name of the object
   * @return true if object exists, false otherwise
   */
  bool object_exists(const std::string& obj_name) const;
  
  /**
   * Get the current version of an object
   *
   * @param obj_name Name of the object
   * @return Current version, or eversion_t() if object doesn't exist
   */
  eversion_t get_object_version(const std::string& obj_name) const;
  
  /**
   * Get the expected size of an object
   *
   * @param obj_name Name of the object
   * @return Expected size in bytes
   */
  uint64_t get_expected_size(const std::string& obj_name) const;
  
  /**
   * Clear all tracked state
   */
  void clear();
  
  /**
   * Get all tracked object names
   *
   * @return Vector of object names
   */
  std::vector<std::string> get_tracked_objects() const;
  
  /**
   * Get statistics about tracked operations
   *
   * @return String with statistics
   */
  std::string get_stats() const;

private:
  // These methods are called exclusively by PGBackendTestFixture completion
  // callbacks after a transaction commits.  Tests must not supply data directly.
  void record_data_write(
    const std::string& obj_name,
    uint64_t offset,
    const std::string& data,
    const eversion_t& version);

  void record_create(
    const std::string& obj_name,
    const std::string& data,
    const eversion_t& version);

  void record_attribute_write(
    const std::string& obj_name,
    const std::string& attr_name,
    const std::string& attr_value,
    const eversion_t& version);

  void record_omap_write(
    const std::string& obj_name,
    const std::string& key,
    const std::string& value,
    const eversion_t& version);

  void record_delete(
    const std::string& obj_name,
    const eversion_t& version);
};

// Made with Bob
