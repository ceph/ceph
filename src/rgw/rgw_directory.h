
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

class RGWDirectory{
public:
	RGWDirectory() {}
	virtual ~RGWDirectory(){ cout << "RGW Directory is destroyed!";}
	CephContext *cct;

private:

};

class RGWObjectDirectory: public RGWDirectory {
public:

	RGWObjectDirectory() {}
	//cpp_redis::client *client;
	//cpp_redis::client client1;
	//cpp_redis::client client2;
	//cpp_redis::client client3;
	void init(CephContext *_cct) {
      		cct = _cct;
		//client1.connect(cct->_conf->rgw_directory_address1, cct->_conf->rgw_directory_port1);
		//client2.connect(cct->_conf->rgw_directory_address2, cct->_conf->rgw_directory_port2);
		//client3.connect(cct->_conf->rgw_directory_address3, cct->_conf->rgw_directory_port3);
    	
    	}

	virtual ~RGWObjectDirectory() { cout << "RGWObject Directory is destroyed!";}

	void findClient(string key, cpp_redis::client *client);
	int delKey(string key);
	int existKey(string key, cpp_redis::client *client);
	int setValue(cache_obj *ptr);
	int getValue(cache_obj *ptr);
	int updateField(cache_obj *ptr, string field, string value);
	int delValue(cache_obj *ptr);
	int setTTL(cache_obj *ptr, int seconds);
	vector<pair<vector<string>, time_t>> get_aged_keys(time_t startTime_t, time_t endTime_t);

private:
	string buildIndex(cache_obj *ptr);
	

};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	void init(CephContext *_cct) {
		cct = _cct;
	}
	virtual ~RGWBlockDirectory() { cout << "RGWObject Directory is destroyed!";}

	void findClient(string key, cpp_redis::client *client);
	int delKey(string key);
	int existKey(string key, cpp_redis::client *client);
	int setValue(cache_block *ptr);
	int getValue(cache_block *ptr);
	int getValue(cache_block *ptr, string key);
	int updateField(cache_block *ptr, string field, string value);
	int updateField(string key, string field, string value);
	int updateGlobalWeight(string key,  size_t weight , bool evict);
	int resetGlobalWeight(string key);
	int delValue(cache_block *ptr);
	int delValue(string key);
	int updateAccessCount(string key, int incr);
	int setTTL(cache_block *ptr, int seconds);
	int getHosts(string key, string hosts);
	int setAvgCacheWeight(int64_t avg_weight);
	int getAvgCacheWeight(string endpoint);
private:
	string buildIndex(cache_block *ptr);
};




#endif
