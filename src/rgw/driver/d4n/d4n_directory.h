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

struct cache_obj {
  std::string bucket_name; /* s3 bucket name */
  std::string obj_name; /* s3 obj name */
};

struct cache_block {
  cache_obj c_obj;
  uint64_t size_in_bytes; /* block size_in_bytes */
  std::vector<std::string> hosts_list; /* Currently not supported: list of hostnames <ip:port> of block locations */
};

class D4NDirectory {
  public:
    RGWDirectory() {}
    CephContext *cct;
};

class BlockDirectory: D4NDirectory {
  public:
    RGWBlockDirectory() {}
    RGWBlockDirectory(std::string host, int port) {
      addr.host = host;
      addr.port = port;
    }
    
    void init(CephContext *_cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }
	
    int findClient(cpp_redis::client *client);
    int existKey(std::string key);
    int setValue(cache_block *ptr);
    int getValue(cache_block *ptr);
    int delValue(cache_block *ptr);

    Address get_addr() { return addr; }

  private:
    cpp_redis::client client;
    Address addr;
    std::string buildIndex(cache_block *ptr);
};

} } // namespace rgw::d4n
   
#endif
