#pragma once

#include <string>
#include <lmdb.h>
#include <cstdint>

class RGWUsageCache {
  MDB_env* env = nullptr;
  MDB_dbi user_dbi = 0;
  MDB_dbi bucket_dbi = 0;
public:
  RGWUsageCache(const std::string& path);
  ~RGWUsageCache();
  int put_user(const std::string& id, uint64_t bytes, uint64_t objects);
  int put_bucket(const std::string& name, uint64_t bytes, uint64_t objects);
  bool get_user(const std::string& id, uint64_t& bytes, uint64_t& objects);
  bool get_bucket(const std::string& name, uint64_t& bytes, uint64_t& objects);
};
