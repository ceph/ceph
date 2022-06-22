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
#define dout_subsys ceph_subsys_rgw

class RGWDirectory {
  public:
    RGWDirectory() {}
    virtual ~RGWDirectory() { ldout(cct, 5) << "RGW Directory is destroyed!" << dendl; }
    CephContext *cct;
};

class RGWBlockDirectory: RGWDirectory {
  public:
    RGWBlockDirectory() {}
    
    void init(CephContext *_cct) {
      cct = _cct;
    }
	
    virtual ~RGWBlockDirectory() { 
      ldout(cct, 5) << "RGWBlock Directory is destroyed!" << dendl;
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
