#ifndef CEPH_RGWD4NCACHE_H
#define CEPH_RGWD4NCACHE_H

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

class RGWD4NCache {
  public:
    CephContext* cct;

    RGWD4NCache() {}
    RGWD4NCache(std::string cacheHost, int cachePort):host(cacheHost), port(cachePort) {}

    void init(CephContext* _cct) {
      cct = _cct;
      host = cct->_conf->rgw_d4n_host;
      port = cct->_conf->rgw_d4n_port;
    }

    int find_client(cpp_redis::client *client);
    int exist_key(std::string key);

    int copy_data(std::string originalOid, std::string copyOid);
    int append_data(std::string oid, buffer::list& data);
    int del_data(std::string oid);

    int set_attrs(std::string oid, rgw::sal::Attrs* attrs);
    int get_attrs(std::string oid, rgw::sal::Attrs* newAttrs, std::vector< std::pair<std::string, std::string> >* newMetadata);
    int copy_attrs(std::string originalOid, std::string copyOid, rgw::sal::Attrs* attrs);
    int update_attr(std::string oid, rgw::sal::Attrs* attr);
    int del_attrs(std::string oid, std::vector<std::string>& baseFields, std::vector<std::string>& deleteFields);

    int del_object(std::string oid);

  private:
    cpp_redis::client client;
    std::string host = "";
    int port = 0;
    std::vector< std::pair<std::string, std::string> > build_data(bufferlist&& data);
    std::vector< std::pair<std::string, std::string> > build_attrs(rgw::sal::Attrs* binary);
};

#endif
