#ifndef CEPH_RGWD4NCACHE_H
#define CEPH_RGWD4NCACHE_H

#include "rgw_common.h"
#include "d4n_directory.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

namespace rgw { namespace d4n {

class RGWD4NCache {
  public:
    CephContext *cct;

    RGWD4NCache() {}
    RGWD4NCache(std::string host, int port) {
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
    Address addr;
    std::vector< std::pair<std::string, std::string> > buildObject(rgw::sal::Attrs* binary);
};

} } // namespace rgw::d4n

#endif
