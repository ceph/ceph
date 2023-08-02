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
  std::vector<std::string> hosts_list;
  bool cachedOnRemote;
};

class RGWDirectory {
  public:
    RGWDirectory() {}
    virtual ~RGWDirectory() = default;
    RGWDirectory(std::string blockHosts): hosts(blockHosts) {}
    CephContext *cct;
    virtual std::string get_hosts() { return hosts; }
    virtual void set_hosts(std::string blockHosts) { hosts = blockHosts; }
    virtual int process_hosts(std::string hosts, std::vector<std::pair<std::string, int>> *hosts_vector);
    virtual int findClient(std::string key, cpp_redis::client *client);
    virtual int findHost(std::string key, std::vector<std::pair<std::string, int>> *hosts_vector);

  private:
    virtual unsigned int hash_slot(const char *key, int keylen);
    virtual uint16_t crc16(const char *buf, int len);
  protected:
    std::string hosts = ""; //a config option in this format: "host1:port1;host2:port2;..."

};

class RGWBlockDirectory: RGWDirectory {
  public:
    RGWBlockDirectory() {}
    RGWBlockDirectory(std::string blockHosts): RGWDirectory(blockHosts) {}
    
    void init(CephContext *_cct) {
      cct = _cct;
      set_hosts(cct->_conf->rgw_d4n_directory_hosts);
    }
	
    virtual std::string get_hosts() { return hosts; }
    int existKey(std::string key, cpp_redis::client *client);
    int setValue(cache_block *ptr);
    int getValue(cache_block *ptr);
    int delValue(cache_block *ptr);
    int updateField(cache_block *ptr, std::string field, std::string value);
    int getHosts(cache_block *ptr, std::string &hosts);


  private:
    std::string buildIndex(cache_block *ptr);

};

#endif
