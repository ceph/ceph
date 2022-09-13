#ifndef CEPH_RGWD4NCACHE_H
#define CEPH_RGWD4NCACHE_H

#include "rgw_common.h"
#include "cpp_redis/cpp_redis" 
#include <string>
#include <iostream>

class RGWD4NCache {
  public:
    CephContext *cct;

    RGWD4NCache() {}
    RGWD4NCache(std::string cacheHost, int cachePort):host(cacheHost), port(cachePort) {}

    void init(CephContext *_cct) {
      cct = _cct;
      host = cct->_conf->rgw_directory_host;
      port = cct->_conf->rgw_directory_port;
    }

    void findClient(cpp_redis::client *client);
    int existKey(std::string key);
    int setObject(std::string oid, rgw::sal::Attrs* baseAttrs, rgw::sal::Attrs* newAttrs);
    int getObject(std::string oid, rgw::sal::Attrs* baseAttrs, rgw::sal::Attrs* newAttrs);
    int delObject(std::string oid);
    int delAttrs(std::string oid, std::vector<std::string>& baseFields, std::vector<std::string>& deleteFields);

  private:
    cpp_redis::client client;
    std::string host = "";
    int port = 0;
    std::vector< std::pair<std::string, std::string> > buildObject(rgw::sal::Attrs* baseBinary, rgw::sal::Attrs* newBinary);
};

#endif
