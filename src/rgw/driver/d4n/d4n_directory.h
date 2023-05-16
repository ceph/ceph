#ifndef CEPH_D4NDIRECTORY_H
#define CEPH_D4NDIRECTORY_H

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

namespace rgw { namespace d4n {

struct Address {
  std::string host;
  int port;
};

struct CacheObj {
  std::string bucketName; /* S3 bucket name */
  std::string objName; /* S3 object name */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0;
  int localWeight = 0;
  std::vector<std::string> hostsList; /* Currently not supported: list of hostnames <ip:port> of block locations */
};

class Directory {
  public:
    Directory() {}
    CephContext* cct;
};

class BlockDirectory: Directory {
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
    int copy_value(CacheBlock* block, CacheBlock* copy_block);
    int del_value(CacheBlock* block);


  private:
    cpp_redis::client client;
    Address addr;
    std::string build_index(CacheBlock* block);
};

} } // namespace rgw::d4n

#endif
