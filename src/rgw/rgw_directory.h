
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

/* the metadata which is written to the directory
 * you can add or remove some of the fields based on
 * your required caching policy
 */
/*
typedef struct objDirectoryStruct {
    string key; //bucketID_ObjectID
    string owner;
    string location;
    uint8_t dirty;
    uint64_t size;
    string createTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;
}objectDirectoryStruct_t;

typedef struct blockDirectoryStruct {
    string key; //bucketID_ObjectID_offset
    string owner;
    string location;
    string size;
    string createTime;
    string lastAccessTime;
    string etag;
	std::vector<std::pair<std::string, std::string>> popularTenants;
}blockDirectoryStruct_t;

typedef struct cacheStatDirectoryStruct {
    uint64_t hitCount;
    uint64_t reqCount;
    uint64_t capacity;
    string ID;
}cacheStatDirectoryStruct_t;
*/
class RGWDirectory{
public:
	RGWDirectory() {}
	virtual ~RGWDirectory(){ cout << "RGW Directory is destroyed!";}
	virtual int getValue(cache_obj *ptr);
	int setValue(RGWDirectory *dirObj, cache_obj *ptr);
	int updateLastAcessTime(RGWDirectory *dirObj, cache_obj *ptr, string lastAccessTime);
	int updateACL(RGWDirectory *dirObj, cache_obj *ptr, string acl);
	int updateHostList(RGWDirectory *dirObj, cache_obj *ptr, string host);
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
	vector<vector<string>> get_aged_keys(string startTime, string endTime);

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
