#pragma once

#include <string>
#include <cstdint>
#include <lmdb.h>

struct RGWUsageRecord {
  uint64_t used_bytes{0};
  uint64_t num_objects{0};
};

class RGWUsageCache {
  MDB_env* env{nullptr};
  MDB_dbi user_dbi{};
  MDB_dbi bucket_dbi{};
public:
  explicit RGWUsageCache(const std::string& path);
  ~RGWUsageCache();

  int put_user(const std::string& user, const RGWUsageRecord& record);
  int get_user(const std::string& user, RGWUsageRecord* record);

  int put_bucket(const std::string& bucket, const RGWUsageRecord& record);
  int get_bucket(const std::string& bucket, RGWUsageRecord* record);
};

