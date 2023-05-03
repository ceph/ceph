#ifndef CEPH_RGWD4NPOLICY_H
#define CEPH_RGWD4NPOLICY_H

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "rgw_d4n_directory.h"

namespace rgw { namespace d4n {

class PolicyDriver {
  public:
    CephContext *cct;

    PolicyDriver() {}
    PolicyDriver(std::string cacheHost, int cachePort):host(cacheHost), port(cachePort) {}

    void init(CephContext *_cct) {
      cct = _cct;
      host = cct->_conf->rgw_d4n_host;
      port = cct->_conf->rgw_d4n_port;
    }

    int find_client(cpp_redis::client *client);
    int exist_key(std::string key);

    int update_gw(CacheBlock* block);
    int get_block(CacheBlock* block);
    int eviction();
    bool should_cache(int objSize, int minSize); /* In bytes */
    bool should_cache(std::string uploadType); 
    
  private:
    cpp_redis::client client;
    std::string host = "";
    int port = 0;
};

} } // namespace rgw::d4n

#endif
