// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "lancedb.h"

#include <memory>
#include <string>

namespace rgw::sal {
  class Driver;
}
class DoutPrefixProvider;

namespace rgw::s3vector {
  bool init(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  void shutdown();
  void pause();
  void resume(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver);
  // update whenever new vectors are added to an index
  bool notify_index_update(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name);
  // update whenever a index is removed
  bool notify_index_remove(const DoutPrefixProvider* dpp, const std::string& bucket_name, const std::string& index_name);
  // get LanceDB session for a bucket, returns nullptr if session doesn't exist or manager is not initialized
  std::shared_ptr<const LanceDBSession> get_session(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // notify manager for session creation
  bool notify_session_create(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // notify manager for session deletion
  bool notify_session_delete(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // delete one session immediately by bucket name
  int delete_session(const DoutPrefixProvider* dpp, const std::string& bucket_name);
  // get index cache stats for a session by bucket name
  int get_index_cache_stats(const DoutPrefixProvider* dpp, const std::string& bucket_name, LanceDBSessionCacheStats& out_stats);
  // get metadata cache stats for a session by bucket name
  int get_metadata_cache_stats(const DoutPrefixProvider* dpp, const std::string& bucket_name, LanceDBSessionCacheStats& out_stats);
}
