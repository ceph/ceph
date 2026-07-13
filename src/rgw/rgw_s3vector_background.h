// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace rgw::sal {
  class Driver;
}
class DoutPrefixProvider;
struct LanceDBSession;

namespace rgw::s3vector {
  bool init(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  void shutdown();
  void pause();
  void resume(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  // update whenever vectors are added to an index (row_count = number of rows mutated)
  bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name, uint64_t row_count);
  // update whenever vectors are deleted from an index (row_count = number of keys deleted)
  bool notify_index_delete(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name, uint64_t row_count);
  // update whenever a index is removed
  bool notify_index_remove(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name);
  // get LanceDB session for a bucket, returns nullptr if session doesn't exist or manager is not initialized
  std::shared_ptr<const LanceDBSession> get_session(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // notify manager for session creation
  bool notify_session_create(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // notify manager for session deletion
  bool notify_session_delete(const DoutPrefixProvider* dpp, const std::string& bucket_name);
}

