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

using namespace std;

class RGWDirectory {
  public:
    RGWDirectory() {}
    virtual ~RGWDirectory() { cout << "RGW Directory is destroyed!"; }
    CephContext *cct;
};

class RGWBlockDirectory: RGWDirectory {
  public:
    RGWBlockDirectory() {}
    
    void init(CephContext *_cct) {
      cct = _cct;
    }
	
    virtual ~RGWBlockDirectory() { 
      cout << "RGWObject Directory is destroyed!";
      client.disconnect(true);
    }
    
    void findClient(string key, cpp_redis::client *client, int port);
    int existKey(string key, cpp_redis::client *client);
    int setValue(cache_block *ptr, int port);
    int setValue(cache_block *ptr);
    int getValue(cache_block *ptr, int port);
    int getValue(cache_block *ptr);
  
  private:
    string buildIndex(cache_block *ptr);
    cpp_redis::client client;
};

#endif
