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
    uint64_t eviction(/*CacheDriver* cacheNode*/) { return 0; }

  private:
    cpp_redis::client client;
    Address addr;
};

class LFUDAPolicy : public CachePolicy {
  public:
    LFUDAPolicy() : CachePolicy() {}

    int set_age(int age);
    int get_age();
    int set_global_weight(std::string key, int weight);
    int get_global_weight(std::string key);
    int set_min_avg_weight(int weight, std::string cacheLocation);
    int get_min_avg_weight();

    int get_block(CacheBlock* block/*, CacheDriver* cacheNode*/);
    uint64_t eviction(/*CacheDriver* cacheNode*/);

  private:
    cpp_redis::client client;
};

class PolicyDriver {
  public:
    CachePolicy* cachePolicy;
    //CacheDriver* cacheDriver; // might place elsewhere -Sam

    PolicyDriver(std::string _policyName) : policyName(_policyName) {}

    int set_policy();
    int delete_policy();

  private:
    std::string policyName;
};

} } // namespace rgw::d4n

#endif
