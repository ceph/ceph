


#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>

inline const string BoolToString(bool b)
{	
	return b ? "true" : "false";
}

inline const bool StringToBool(string s)
{
	if (s == "true")
		return true;
	else 
		return false;	
}

string locationToString( int enumVal )
{
	switch(enumVal)
	{
		case CacheLocation::LOCAL_READ_CACHE:
			return "readCache";
		case CacheLocation::WRITE_BACK_CACHE:
			return "writeCache";
		case CacheLocation::REMOTE_CACHE:
			return "remoteCache" ;
		case CacheLocation::DATALAKE:
			return "dataLake";

		default:
			return "Not recognized..";
	}
}

string protocolToString( int enumVal )
{
	switch(enumVal)
	{
		case BackendProtocol::S3:
			return "s3";
		case BackendProtocol::LIBRADOS:
			return "librados";
		case BackendProtocol::SWIFT:
			return "swift" ;

		default:
			return "Not recognized..";
	}
}

BackendProtocol stringToProtocol(string protocol)
{
	if (protocol == "s3")
		return BackendProtocol::S3;
	else if (protocol == "librados")
		return BackendProtocol::LIBRADOS;
	else if (protocol == "swift")
		return BackendProtocol::SWIFT;
	else
		return BackendProtocol::S3;
}



/* this function should be implemented in their own respected classes */
int RGWDirectory::getValue(cache_obj *ptr){
	cout << "wrong function!";
	return -1;
}

int RGWDirectory::setKey(string key, cache_obj *ptr){
	cout << "wrong function!";
	return -1;
}

string RGWDirectory::buildIndex(cache_obj *ptr){
	cout << "wrong function!";
	return NULL;
}



int RGWDirectory::existKey(string key){
	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	int result = 0;

    vector<string> keys;
    keys.push_back(key);

	client.exists(keys, [&result](cpp_redis::reply &reply){
		result = reply.as_integer();
	});
	return result;
}

/* updatinh the directory value of host_list
 * its input is a host which has a copy of the data
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWDirectory::updateHostList(RGWDirectory *dirObj, cache_obj *ptr, string host){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = dirObj->buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		//updating the desired field
		tmpObj.host_list.push_back(host);

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}

/* updatinh the directory value of acl_obj
 * its input is a new acl of the object
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWDirectory::updateACL(RGWDirectory *dirObj, cache_obj *ptr, string acl){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = dirObj->buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		tmpObj.acl = acl;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}

/* updatinh the directory value of lastAccessTime
 * its input is object's ceph::real_time last access time 
 * and bucket_name, obj_name and chunk_id in *ptr
 */
int RGWDirectory::updateLastAcessTime(RGWDirectory *dirObj, cache_obj *ptr, string lastAccessTime){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = dirObj->buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		tmpObj.lastAccessTime = lastAccessTime;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		return -1;
	return 0;
	
}

int RGWDirectory::delValue(RGWDirectory *dirObj, cache_obj *ptr){
    string key = dirObj->buildIndex(ptr);
	int result = 0;

	result += delKey(key);
	return result;
}

int RGWDirectory::delKey(string key){
	int result = 0;
    vector<string> keys;
    keys.push_back(key);

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	client.del(keys, [&result](cpp_redis::reply &reply){
		result = reply.as_integer();
	});
	return result;
}




/* builds the index for the directory
 * based on bucket_name, obj_name, and chunk_id
 */
string RGWObjectDirectory::buildIndex(cache_obj *ptr){
	return ptr->bucket_name + "_" + ptr->obj_name + "_" + to_string(ptr->chunk_id);
}

/* builds the index for the directory
 * based on bucket_name, obj_name, chunk_id, and etag
 */
string RGWBlockDirectory::buildIndex(cache_obj *ptr){
	return ptr->bucket_name + "_" + ptr->obj_name + "_" + to_string(ptr->chunk_id) + ptr->etag;
}


/* adding a key to the directory
 * if the key exists, it will be deleted and then the new value be added
 */
int RGWDirectory::setValue(RGWDirectory *dirObj, cache_obj *ptr){

	//creating the index based on bucket_name, obj_name, and chunk_id
	string key = dirObj->buildIndex(ptr);

	//delete the existing key, 
	//to update an existing key, updateValue() should be used
	if (existKey(key))
		delKey(key);

	return dirObj->setKey(key, ptr);
	
}

/* the horse function to add a new key to the directory
 */
int RGWObjectDirectory::setKey(string key, cache_obj *ptr){
	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	vector<pair<string, string>> list;
	vector<string> keys;
	multimap<string, string> timeKey;
	vector<string> options;
	string host;

	stringstream ss;
	for(size_t i = 0; i < ptr->host_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
		ss << ptr->host_list[i];
	}
	host = ss.str();

	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("owner", ptr->user));
	list.push_back(make_pair("obj_acl", ptr->acl));
	list.push_back(make_pair("aclTimeStamp", ptr->aclTimeStamp));
	list.push_back(make_pair("host", host));
	list.push_back(make_pair("dirty", BoolToString(ptr->dirty)));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("creationTime", ptr->creationTime));
	list.push_back(make_pair("lastAccessTime", ptr->lastAccessTime));
	list.push_back(make_pair("etag", ptr->etag));
	list.push_back(make_pair("backendProtocol", protocolToString(ptr->backendProtocol)));
	list.push_back(make_pair("bucket_name", ptr->bucket_name));
	list.push_back(make_pair("obj_name", ptr->obj_name));
	list.push_back(make_pair("chunk_id", to_string(ptr->chunk_id)));

	//creating a key entry
	keys.push_back(key);

	//making key and time a pair
	timeKey.emplace(ptr->creationTime,key);

	client.hmset(key, list, [](cpp_redis::reply &reply){
	});

	client.rpush("objectDirectory", keys, [](cpp_redis::reply &reply){
	});

	//this will be used for aging policy
	client.zadd("keyObjectDirectory", options, timeKey, [](cpp_redis::reply &reply){
	});

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;

}

/* the horse function to add a new key to the directory
 */
int RGWBlockDirectory::setKey(string key, cache_obj *ptr){
	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	vector<pair<string, string>> list;
	vector<string> keys;
	multimap<string, string> timeKey;
	vector<string> options;
	string host;

	stringstream ss;
	for(size_t i = 0; i < ptr->host_list.size(); ++i)
	{
		if(i != 0)
			ss << "_";
		ss << ptr->host_list[i];
	}
	host = ss.str();

	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("owner", ptr->user));
	list.push_back(make_pair("block_acl", ptr->acl));
	list.push_back(make_pair("aclTimeStamp", ptr->aclTimeStamp));
	list.push_back(make_pair("host", host));
	list.push_back(make_pair("dirty", BoolToString(ptr->dirty)));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("creationTime", ptr->creationTime));
	list.push_back(make_pair("lastAccessTime", ptr->lastAccessTime));
	list.push_back(make_pair("etag", ptr->etag));
	list.push_back(make_pair("bucket_name", ptr->bucket_name));
	list.push_back(make_pair("obj_name", ptr->obj_name));
	list.push_back(make_pair("chunk_id", to_string(ptr->chunk_id)));

	//creating a key entry
	keys.push_back(key);

	//making key and time a pair
	timeKey.emplace(ptr->creationTime,key);

	client.hmset(key, list, [](cpp_redis::reply &reply){
	});

	client.rpush("blockDirectory", keys, [](cpp_redis::reply &reply){
	});

	//this will be used for aging policy
	client.zadd("keyBlockDirectory", options, timeKey, [](cpp_redis::reply &reply){
	});

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;

}

int RGWObjectDirectory::getValue(cache_obj *ptr){

    string key = buildIndex(ptr);
    string owner;
    string obj_acl;
    string aclTimeStamp;
    string host;
    string dirty;
    string size;
    string creationTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;
	string chunk_id;

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("obj_acl");
	fields.push_back("aclTimeStamp");
	fields.push_back("host");
	fields.push_back("dirty");
	fields.push_back("size");
	fields.push_back("creationTime");
	fields.push_back("lastAccessTime");
	fields.push_back("etag");
	fields.push_back("backendProtocol");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("chunk_id");

	client.hmget(key, fields, [&key, &owner, &obj_acl, &aclTimeStamp, &host, &dirty, &size, &creationTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name, &chunk_id](cpp_redis::reply &reply){
	      key = reply.as_string()[0];
	      owner = reply.as_string()[1];
	      obj_acl = reply.as_string()[2];
	      aclTimeStamp = reply.as_string()[3];
	      host = reply.as_string()[4];
	      dirty = reply.as_string()[5];
	      size = reply.as_string()[6];
	      creationTime = reply.as_string()[7];
	      lastAccessTime = reply.as_string()[8];
	      etag = reply.as_string()[9];
  		  backendProtocol = reply.as_string()[10];
  		  bucket_name = reply.as_string()[11];
	      obj_name = reply.as_string()[12];
	      chunk_id = reply.as_string()[13];
	});

	stringstream sloction(host);
	string tmp;

	//passing the values to the requester
	ptr->user = owner;
	ptr->acl = obj_acl;
	ptr->aclTimeStamp = aclTimeStamp;

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
		ptr->host_list.push_back(tmp);

	ptr->dirty = StringToBool(dirty);
	ptr->size_in_bytes = stoull(size);
	ptr->creationTime = creationTime;
	ptr->lastAccessTime = lastAccessTime;
	ptr->etag = etag;
	ptr->backendProtocol = stringToProtocol(backendProtocol);
	ptr->bucket_name = bucket_name;
	ptr->obj_name = obj_name;
	ptr->chunk_id = stoull(chunk_id);

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;
}


int RGWBlockDirectory::getValue(cache_obj *ptr){

    string key = buildIndex(ptr);
    string owner;
    string block_acl;
    string aclTimeStamp;
    string host;
    string dirty;
    string size;
    string creationTime;
    string lastAccessTime;
    string etag;
    string backendProtocol;
    string bucket_name;
    string obj_name;
	string chunk_id;

	cpp_redis::client client;
	client.connect("127.0.0.1", 7000);

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("block_acl");
	fields.push_back("aclTimeStamp");
	fields.push_back("host");
	fields.push_back("dirty");
	fields.push_back("size");
	fields.push_back("creationTime");
	fields.push_back("lastAccessTime");
	fields.push_back("etag");
	fields.push_back("backendProtocol");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("chunk_id");

	client.hmget(key, fields, [&key, &owner, &block_acl, &aclTimeStamp, &host, &dirty, &size, &creationTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name, &chunk_id](cpp_redis::reply &reply){
	      key = reply.as_string()[0];
	      owner = reply.as_string()[1];
	      block_acl = reply.as_string()[2];
	      aclTimeStamp = reply.as_string()[3];
	      host = reply.as_string()[4];
	      dirty = reply.as_string()[5];
	      size = reply.as_string()[6];
	      creationTime = reply.as_string()[7];
	      lastAccessTime = reply.as_string()[8];
	      etag = reply.as_string()[9];
  		  backendProtocol = reply.as_string()[10];
  		  bucket_name = reply.as_string()[11];
	      obj_name = reply.as_string()[12];
	      chunk_id = reply.as_string()[13];
	});

	stringstream sloction(host);
	string tmp;

	//passing the values to the requester
	ptr->user = owner;
	ptr->acl = block_acl;
	ptr->aclTimeStamp = aclTimeStamp;

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
		ptr->host_list.push_back(tmp);

	ptr->dirty = StringToBool(dirty);
	ptr->size_in_bytes = stoull(size);
	ptr->creationTime = creationTime;
	ptr->lastAccessTime = lastAccessTime;
	ptr->etag = etag;
	ptr->backendProtocol = stringToProtocol(backendProtocol);
	ptr->bucket_name = bucket_name;
	ptr->obj_name = obj_name;
	ptr->chunk_id = stoull(chunk_id);

	// synchronous commit, no timeout
	client.sync_commit();

	return 0;
}



/* updatinh the directory value*/
/* value should be string even for fields such as size or chunk_id */
/*
int RGWObjectDirectory::updateValue(cache_obj *ptr, string field, string value){

	cache_obj tmpObj;
	
	//we need to build the key to find the object in the directory
	tmpObj.bucket_name = ptr->bucket_name;
	tmpObj.obj_name = ptr->obj_name;
	tmpObj.chunk_id = ptr->chunk_id;

	string key = buildIndex(&tmpObj);

	if (existKey(key))
	{
		//getting old values from the directory
		getValue(&tmpObj);

		//updating the desired field
		if (field == "user")
			tmpObj.user = value;	
		else if (field == "bucket_name")
			tmpObj.bucket_name = value;
		else if (field == "obj_name")
			tmpObj.obj_name = value;
		else if (field == "host")
			tmpObj.host_list.push_back(value);
		else if (field == "size_in_bytes")
			tmpObj.size_in_bytes = stoull(value);
		else if (field == "dirty")
			tmpObj.dirty = value[0];
		else if (field == "chunk_id")
			tmpObj.chunk_id = stoull(value);
		else if (field == "etag")
			tmpObj.etag = value;
		else if (field == "creationTime")
			tmpObj.creationTime = value;
		else if (field == "lastAccessTime")
			tmpObj.lastAccessTime = value;
		else if (field == "backendProtocol")
			tmpObj.backendProtocol = value;
		else if (field == "obj_acl")
			tmpObj.obj_acl = value;
		else if (field == "aclTimeStamp")
			tmpObj.aclTimeStamp = value;

		//updating the directory value 
		setKey(key, &tmpObj);
	}
	else
		setValue(&tmpObj);
	
}
*/


//returns all the keys between startTime and endTime as <key, time> paris
std::vector<std::pair<std::string, std::string>> RGWObjectDirectory::get_aged_keys(string startTime, string endTime){
	std::vector<std::pair<std::string, std::string>> list;
	std::string key;
	std::string time;

	cpp_redis::client client;
	//	//client.connect();
	client.connect("127.0.0.1", 7000);

	std::string dirKey = "keyObjectDirectory";
	client.zrangebyscore(dirKey, startTime, endTime, true, [&key, &time, &list](cpp_redis::reply &reply){
	      for (unsigned i = 0; i < reply.as_array().size(); i+=2)
	      {
	          key = reply.as_string()[i];
	          time = reply.as_string()[i+1];

			  list.push_back(make_pair(key, time));
	      }
	      //if (reply.is_error() == false)
  });

	// synchronous commit, no timeout
	client.sync_commit();

	return list;
}



