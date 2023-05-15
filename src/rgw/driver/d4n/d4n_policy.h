#ifndef CEPH_D4NPOLICY_H
#define CEPH_D4NPOLICY_H

#include <string>
#include <iostream>
#include <cpp_redis/cpp_redis>
#include "rgw_common.h"
#include "d4n_directory.h"

namespace rgw { namespace d4n {

class CachePolicy {
  public:
    CephContext* cct;

    CachePolicy() : addr() {}

    void init(CephContext *_cct) {
      cct = _cct;
      addr.host = cct->_conf->rgw_d4n_host;
      addr.port = cct->_conf->rgw_d4n_port;
    }
    int find_client(cpp_redis::client *client);
    int exist_key(std::string key);
    Address get_addr() { return addr; }
    int get_block(CacheBlock* block/*, CacheDriver* cacheNode*/) { return 0; }
    int eviction(/*CacheDriver* cacheNode*/) { return 0; }

  private:
    cpp_redis::client client;
    Address addr;
};

class LFUDAPolicy : public CachePolicy {
  public:
    LFUDAPolicy() : CachePolicy()/*, addr()*/ {}

    int get_block(CacheBlock* block/*, CacheDriver* cacheNode*/);
    int eviction(/*CacheDriver* cacheNode*/);

  private:
    cpp_redis::client client;
    int localWeight = 0;
};

class PolicyDriver {
  public:
    CachePolicy* cachePolicy;

    PolicyDriver(std::string _policyName) : policyName(_policyName) {}

    int set_policy();
    int delete_policy();

  private:
    std::string policyName;
};

} } // namespace rgw::d4n

#endif
