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

#include "test/osd/ObjectTracker.h"
#include <algorithm>
#include <sstream>

void ObjectTracker::record_data_write(
  const std::string& obj_name,
  uint64_t offset,
  const std::string& data,
  const eversion_t& version)
{
  auto& obj = objects_[obj_name];
  obj.object_name = obj_name;
  obj.exists = true;
  obj.version = version;
  
  // Update size if this write extends the object
  uint64_t write_end = offset + data.length();
  if (write_end > obj.size) {
    obj.size = write_end;
  }
  
  // Record the write
  obj.data_writes.emplace_back(offset, data, version);
}

void ObjectTracker::record_create(
  const std::string& obj_name,
  const std::string& data,
  const eversion_t& version)
{
  auto& obj = objects_[obj_name];
  obj.object_name = obj_name;
  obj.exists = true;
  obj.size = data.length();
  obj.version = version;
  
  // Clear any previous state (in case of recreate)
  obj.data_writes.clear();
  obj.attributes.clear();
  obj.omap_entries.clear();
  
  // Only record a data write if there is actual data
  if (data.length() > 0) {
    obj.data_writes.emplace_back(0, data, version);
  }
}

void ObjectTracker::record_attribute_write(
  const std::string& obj_name,
  const std::string& attr_name,
  const std::string& attr_value,
  const eversion_t& version)
{
  // Attribute writes only make sense on an object that already exists (via
  // record_create()/record_data_write()); objects_[obj_name] would silently
  // default-construct an exists==false entry for an unknown name, and
  // get_expected_attribute()/verify_object() don't check `exists`, so a
  // caller bug here would otherwise surface as a false-positive attribute
  // match on a nonexistent object instead of a loud failure.
  auto it = objects_.find(obj_name);
  ceph_assert(it != objects_.end() && it->second.exists);
  auto& obj = it->second;
  obj.version = version;

  // Update or insert the attribute
  obj.attributes[attr_name] = AttributeWrite(attr_value, version);
}

void ObjectTracker::record_omap_write(
  const std::string& obj_name,
  const std::string& key,
  const std::string& value,
  const eversion_t& version)
{
  // See record_attribute_write() above: omap writes only make sense on an
  // object that already exists.
  auto it = objects_.find(obj_name);
  ceph_assert(it != objects_.end() && it->second.exists);
  auto& obj = it->second;
  obj.version = version;

  // Update or insert the omap entry
  obj.omap_entries[key] = OMapWrite(value, version);
}

void ObjectTracker::record_delete(
  const std::string& obj_name,
  const eversion_t& version)
{
  auto it = objects_.find(obj_name);
  if (it != objects_.end()) {
    it->second.exists = false;
    it->second.size = 0;
    it->second.version = version;
    it->second.data_writes.clear();
    it->second.attributes.clear();
    it->second.omap_entries.clear();
  }
}

void ObjectTracker::record_truncate_and_write(
  const std::string& obj_name,
  std::optional<uint64_t> truncate_size,
  const std::vector<std::pair<uint64_t, std::string>>& writes,
  const eversion_t& version)
{
  auto& obj = objects_[obj_name];
  obj.object_name = obj_name;
  obj.exists = true;
  obj.version = version;

  if (truncate_size.has_value()) {
    uint64_t trunc_size = truncate_size.value();
    // Rebuild the data up to trunc_size
    std::string current_data = get_expected_data(obj_name, 0, trunc_size);
    if (current_data.length() < trunc_size) {
      current_data.resize(trunc_size, '\0');
    }
    obj.data_writes.clear();
    obj.size = trunc_size;
    if (trunc_size > 0) {
      obj.data_writes.emplace_back(0, current_data, version);
    }
  }

  for (const auto& [offset, data] : writes) {
    record_data_write(obj_name, offset, data, version);
  }
}

const ObjectTracker::ObjectState* ObjectTracker::get_object_state(
  const std::string& obj_name) const
{
  auto it = objects_.find(obj_name);
  if (it != objects_.end()) {
    return &it->second;
  }
  return nullptr;
}

std::string ObjectTracker::get_expected_data(
  const std::string& obj_name,
  uint64_t offset,
  uint64_t length) const
{
  auto it = objects_.find(obj_name);
  if (it == objects_.end() || !it->second.exists) {
    return "";
  }
  
  const auto& obj = it->second;
  
  // Build the complete object data by applying writes in order
  std::string result(obj.size, '\0');
  
  for (const auto& write : obj.data_writes) {
    // Copy the write data into the result buffer
    for (uint64_t i = 0; i < write.data.length() && (write.offset + i) < obj.size; ++i) {
      result[write.offset + i] = write.data[i];
    }
  }
  
  // Extract the requested range
  if (offset >= result.length()) {
    return "";
  }
  
  uint64_t available = result.length() - offset;
  uint64_t to_read = std::min(length, available);
  
  return result.substr(offset, to_read);
}

std::optional<std::string> ObjectTracker::get_expected_attribute(
  const std::string& obj_name,
  const std::string& attr_name) const
{
  auto it = objects_.find(obj_name);
  if (it == objects_.end()) {
    return std::nullopt;
  }
  
  const auto& obj = it->second;
  auto attr_it = obj.attributes.find(attr_name);
  if (attr_it != obj.attributes.end()) {
    return attr_it->second.value;
  }
  
  return std::nullopt;
}

std::optional<std::string> ObjectTracker::get_expected_omap(
  const std::string& obj_name,
  const std::string& key) const
{
  auto it = objects_.find(obj_name);
  if (it == objects_.end()) {
    return std::nullopt;
  }
  
  const auto& obj = it->second;
  auto omap_it = obj.omap_entries.find(key);
  if (omap_it != obj.omap_entries.end()) {
    return omap_it->second.value;
  }
  
  return std::nullopt;
}

bool ObjectTracker::object_exists(const std::string& obj_name) const
{
  auto it = objects_.find(obj_name);
  return it != objects_.end() && it->second.exists;
}

eversion_t ObjectTracker::get_object_version(const std::string& obj_name) const
{
  auto it = objects_.find(obj_name);
  if (it != objects_.end()) {
    return it->second.version;
  }
  return eversion_t();
}

uint64_t ObjectTracker::get_expected_size(const std::string& obj_name) const
{
  auto it = objects_.find(obj_name);
  if (it != objects_.end() && it->second.exists) {
    return it->second.size;
  }
  return 0;
}

void ObjectTracker::clear()
{
  objects_.clear();
}

std::vector<std::string> ObjectTracker::get_tracked_objects() const
{
  std::vector<std::string> result;
  result.reserve(objects_.size());
  
  for (const auto& pair : objects_) {
    if (pair.second.exists) {
      result.push_back(pair.first);
    }
  }
  
  return result;
}

std::string ObjectTracker::get_stats() const
{
  std::ostringstream oss;
  
  size_t total_objects = 0;
  size_t total_data_writes = 0;
  size_t total_attributes = 0;
  size_t total_omap_entries = 0;
  
  for (const auto& pair : objects_) {
    if (pair.second.exists) {
      total_objects++;
      total_data_writes += pair.second.data_writes.size();
      total_attributes += pair.second.attributes.size();
      total_omap_entries += pair.second.omap_entries.size();
    }
  }
  
  oss << "ObjectTracker Statistics:\n"
      << "  Objects tracked: " << total_objects << "\n"
      << "  Data writes: " << total_data_writes << "\n"
      << "  Attributes: " << total_attributes << "\n"
      << "  OMap entries: " << total_omap_entries;
  
  return oss.str();
}
