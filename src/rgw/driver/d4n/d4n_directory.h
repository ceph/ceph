#pragma once

#include "rgw_common.h"

#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

namespace rgw { namespace d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;
using boost::redis::ignore_t;

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty{false};
  std::unordered_set<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID;
  std::string version;
  bool dirty{false};
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  std::unordered_set<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};

class Directory {
  public:
    Directory() {}
};

class ObjectDirectory: public Directory {
  public:
    ObjectDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}

    int exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);

    int set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y); /* If nx is true, set only if key doesn't exist */
    int get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheObj* object, std::string copyName, std::string copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheObj* object, std::string field, std::string value, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(std::shared_ptr<connection>& conn) : conn(conn) {}
    
    int exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);

    int set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    int get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    int copy(const DoutPrefixProvider* dpp, CacheBlock* block, std::string copyName, std::string copyBucketName, optional_yield y);
    int del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y);
    int update_field(const DoutPrefixProvider* dpp, CacheBlock* block, std::string field, std::string value, optional_yield y);
    int remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string value, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheBlock* block);
};

} } // namespace rgw::d4n
