#pragma once

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>
#include <boost/lexical_cast.hpp>

namespace rgw { namespace d4n {

struct Address {
  std::string host;
  int port;
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  time_t creationTime; /* Creation time of the S3 Object */
  bool dirty;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockId; /* block ID */
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};

class Directory {
  public:
    Directory() {}
    CephContext* cct;
};

class ObjectDirectory: public Directory { // weave into write workflow -Sam
  public:
    ObjectDirectory() {}
    ObjectDirectory(std::string host, int port) {
      addr.host = host;
      addr.port = port;
    }

    void init(CephContext* _cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }

    int find_client(cpp_redis::client* client);
    int exist_key(std::string key);
    Address get_addr() { return addr; }

    int set_value(CacheObj* object);
    int get_value(CacheObj* object);
    int copy_value(CacheObj* object, CacheObj* copyObject);
    int del_value(CacheObj* object);

  private:
    cpp_redis::client client;
    Address addr;
    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory() {}
    BlockDirectory(std::string host, int port) {
      addr.host = host;
      addr.port = port;
    }
    
    void init(CephContext* _cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }
	
    int find_client(cpp_redis::client* client);
    int exist_key(std::string key);
    Address get_addr() { return addr; }

    int set_value(CacheBlock* block);
    int get_value(CacheBlock* block);
    int copy_value(CacheBlock* block, CacheBlock* copyBlock);
    int del_value(CacheBlock* block);

    int update_field(CacheBlock* block, std::string field, std::string value);

  private:
    cpp_redis::client client;
    Address addr;
    std::string build_index(CacheBlock* block);
};

} } // namespace rgw::d4n
