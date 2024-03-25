#ifndef CEPH_RGWD4NCACHE_H
#define CEPH_RGWD4NCACHE_H

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

class RGWD4NCache {
  public:
    CephContext *cct;

    RGWD4NCache() {}
    RGWD4NCache(std::string cacheHost, int cachePort):host(cacheHost), port(cachePort) {}

    void init(CephContext *_cct) {
      cct = _cct;
      host = cct->_conf->rgw_d4n_host;
      port = cct->_conf->rgw_d4n_port;
    }

    int findClient(cpp_redis::client *client);
    int existKey(std::string key);
    int setObject(std::string oid, rgw::sal::Attrs* attrs);
    int getObject(std::string oid, rgw::sal::Attrs* newAttrs, std::vector< std::pair<std::string, std::string> >* newMetadata);
    int copyObject(std::string original_oid, std::string copy_oid, rgw::sal::Attrs* attrs);
    int delObject(std::string oid);
    int updateAttr(std::string oid, rgw::sal::Attrs* attr);
    int delAttrs(std::string oid, std::vector<std::string>& baseFields, std::vector<std::string>& deleteFields);
    int appendData(std::string oid, buffer::list& data);
    int deleteData(std::string oid);

  private:
    cpp_redis::client client;
    std::string host = "";
    int port = 0;
    std::vector< std::pair<std::string, std::string> > buildObject(rgw::sal::Attrs* binary);
};

#endif
