
#ifndef CEPH_RGWDIRECTORY_H
#define CEPH_RGWDIRECTORY_H

#include <stdlib.h>
#include <sys/types.h>
#include <sstream>
#include "rgw_common.h"

#include <string>
#include <iostream>
#include <vector>
#include <list>

using namespace std;

class RGWDirectory{
public:
	RGWDirectory() {}
	virtual ~RGWDirectory(){ cout << "RGW Directory is destroyed!";}
	virtual int getValue(cache_obj *ptr);
	int setValue(RGWDirectory *dirObj, cache_obj *ptr);
	int updateLastAcessTime(RGWDirectory *dirObj, cache_obj *ptr);
	int updateHostList(RGWDirectory *dirObj, cache_obj *ptr);
	int updateACL(RGWDirectory *dirObj, cache_obj *ptr);
	int delValue(RGWDirectory *dirObj, cache_obj *ptr);
	//std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);

private:
	virtual int setKey(string key, cache_obj *ptr);
	int delKey(string key);
	int existKey(string key);
	virtual string buildIndex(cache_obj *ptr);

};

class RGWObjectDirectory: public RGWDirectory {
public:

	RGWObjectDirectory() {}
	virtual ~RGWObjectDirectory() { cout << "RGWObject Directory is destroyed!";}
	int getValue(cache_obj *ptr);
	vector<pair<vector<string>, time_t>> get_aged_keys(time_t startTime, time_t endTime);

private:
	int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};

class RGWBlockDirectory: RGWDirectory {
public:

	RGWBlockDirectory() {}
	virtual ~RGWBlockDirectory() { cout << "RGWObject Directory is destroyed!";}
	int getValue(cache_obj *ptr);
	//std::vector<std::pair<std::string, std::string>> get_aged_keys(string startTime, string endTime);

private:
	int setKey(string key, cache_obj *ptr);
	string buildIndex(cache_obj *ptr);
	
};




#endif
