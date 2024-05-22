#ifndef CEPH_RGWD4NDIRECTORY_H
#define CEPH_RGWD4NDIRECTORY_H

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

struct cache_obj {
  std::string bucket_name; /* s3 bucket name */
  std::string obj_name; /* s3 obj name */
};

struct cache_block {
  cache_obj c_obj;
  uint64_t size_in_bytes; /* block size_in_bytes */
  std::vector<std::string> hosts_list; /* Currently not supported: list of hostnames <ip:port> of block locations */
};

class RGWDirectory {
  public:
    RGWDirectory() {}
    CephContext *cct;
};

class RGWBlockDirectory: RGWDirectory {
  public:
    RGWBlockDirectory() {}
    RGWBlockDirectory(std::string blockHost, int blockPort):host(blockHost), port(blockPort) {}
    
    void init(CephContext *_cct) {
      cct = _cct;
      host = cct->_conf->rgw_d4n_host;
      port = cct->_conf->rgw_d4n_port;
    }
	
    int findClient(cpp_redis::client *client);
    int existKey(std::string key);
    int setValue(cache_block *ptr);
    int getValue(cache_block *ptr);
    int delValue(cache_block *ptr);

    std::string get_host() { return host; }
    int get_port() { return port; }

  private:
    cpp_redis::client client;
    std::string buildIndex(cache_block *ptr);
    std::string host = "";
    int port = 0;
};

#endif
