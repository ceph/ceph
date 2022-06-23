#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"
#include "cpp_redis/cpp_redis"
#include <string>
#include <iostream>
#include <vector>
#include <list>
#include <cstdint>

struct cache_obj {
  std::string bucket_name; // s3 bucket name
  std::string obj_name; //s3 obj name
};

struct cache_block {
  cache_obj c_obj;
  uint64_t size_in_bytes; // block size_in_bytes
  std::vector<std::string> hosts_list; // list of hostnames <ip:post> of block locations
};

class RGWDirectory {
  public:
    RGWDirectory() {}
    virtual ~RGWDirectory() { std::cout << "RGW Directory is destroyed!"; }
    CephContext *cct;
};

class RGWBlockDirectory: RGWDirectory {
  public:
    RGWBlockDirectory() {}
    
    void init(CephContext *_cct) {
      cct = _cct;
    }
	
    virtual ~RGWBlockDirectory() { 
      std::cout << "RGWObject Directory is destroyed!";
      client.disconnect(true);
    }
    
    void findClient(std::string key, cpp_redis::client *client, int port);
    int existKey(std::string key, cpp_redis::client *client);
    int setValue(cache_block *ptr, int port);
    int setValue(cache_block *ptr);
    int getValue(cache_block *ptr, int port);
    int getValue(cache_block *ptr);
  
  private:
    std::string buildIndex(cache_block *ptr);
    cpp_redis::client client;
};

#endif
