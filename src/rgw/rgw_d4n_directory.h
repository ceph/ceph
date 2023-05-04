#ifndef CEPH_RGWD4NDIRECTORY_H
#define CEPH_RGWD4NDIRECTORY_H

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
  std::string bucketName; /* s3 bucket name */
  std::string objName; /* s3 obj name */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t size; /* block size in bytes */
  int globalWeight = 0;
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
    BlockDirectory(std::string blockHost, int blockPort):host(blockHost), port(blockPort) {}
    
    void init(CephContext* _cct) {
      cct = _cct;
      host = cct->_conf->rgw_d4n_host;
      port = cct->_conf->rgw_d4n_port;
    }
	
    int find_client(cpp_redis::client* client);
    int exist_key(std::string key);
    int set_value(CacheBlock* block);
    int get_value(CacheBlock* block);
    int copy_value(CacheBlock* block, CacheBlock* copy_block);
    int del_value(CacheBlock* block);

    std::string get_host() { return host; }
    int get_port() { return port; }

  private:
    cpp_redis::client client;
    std::string build_index(CacheBlock* block);
    std::string host = "";
    int port = 0;
};

} } // namespace rgw::d4n

#endif
