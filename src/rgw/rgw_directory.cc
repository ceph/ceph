

#include <errno.h>
#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>
#define dout_subsys ceph_subsys_rgw

void tokenize(string &str, char delim, list<string> &out)
{
	size_t start;
	size_t end = 0;

	while ((start = str.find_first_not_of(delim, end)) != string::npos)
	{
		end = str.find(delim, start);
		out.push_back(str.substr(start, end - start));
	}
}


static const uint16_t crc16tab[256]= {
  0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
  0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
  0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
  0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
  0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
  0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
  0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
  0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
  0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
  0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
  0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
  0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
  0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
  0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
  0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
  0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
  0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
  0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
  0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
  0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
  0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
  0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
  0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
  0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
  0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
  0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
  0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
  0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
  0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
  0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
  0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
  0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
};

uint16_t crc16(const char *buf, int len) {
  int counter;
  uint16_t crc = 0;
  for (counter = 0; counter < len; counter++)
	crc = (crc<<8) ^ crc16tab[((crc>>8) ^ *buf++)&0x00FF];
  return crc;
}

unsigned int hash_slot(const char *key, int keylen) {
  int s, e; /* start-end indexes of { and } */

  /* Search the first occurrence of '{'. */
  for (s = 0; s < keylen; s++)
	if (key[s] == '{') break;

  /* No '{' ? Hash the whole key. This is the base case. */
  if (s == keylen) return crc16(key,keylen) & 16383;

  /* '{' found? Check if we have the corresponding '}'. */
  for (e = s+1; e < keylen; e++)
	if (key[e] == '}') break;

  /* No '}' or nothing between {} ? Hash the whole key. */
  if (e == keylen || e == s+1) return crc16(key,keylen) & 16383;

  /* If we are here there is both a { and a } on its right. Hash
   * what is in the middle between { and }. */
  return crc16(key+s+1,e-s-1) & 16383;
}

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

string homeToString( int enumVal )
{
  switch(enumVal)
  {
	case HomeLocation::CACHE:
	  return "cache";
	case HomeLocation::BACKEND:
	  return "backend";

	default:
	  return "Not recognized..";
  }
}

HomeLocation stringToHome(string home)
{
  if (home == "cache")
	return HomeLocation::CACHE;
  else if (home == "backend")
	return HomeLocation::BACKEND;
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

time_t stringToTime(string time)
{
  struct tm tm;
  strptime(time.c_str(), "%Y-%m-%d %H:%M:%S", &tm);
  time_t t = mktime(&tm); 
  return t;
}

string timeToString(time_t time)
{
  struct tm *tm = new struct tm;
  tm = gmtime(&time);

  // format: %Y-%m-%d %H:%M:%S
  string time_s = to_string(tm->tm_year + 1900)+"-"+to_string(tm->tm_mon + 1)+"-"+to_string(tm->tm_mday)+" "+to_string(tm->tm_hour)+":"+to_string(tm->tm_min)+":"+to_string(tm->tm_sec);
  return time_s;
}



void RGWObjectDirectory::findClient(string key, cpp_redis::client *client){
  int slot = 0;
  slot = hash_slot(key.c_str(), key.size());

  try {
	if (slot < 4096)
	  client->connect(cct->_conf->rgw_directory_address, cct->_conf->rgw_directory_port, nullptr, 0, 10, 1000);
	else if (slot < 8192)
	  client->connect(cct->_conf->rgw_directory_address2, cct->_conf->rgw_directory_port2 ,nullptr, 0, 10, 1000);
	else if (slot < 12288)
	  client->connect(cct->_conf->rgw_directory_address3, cct->_conf->rgw_directory_port3 ,nullptr, 0, 10, 1000);
	else
	  client->connect(cct->_conf->rgw_directory_address4, cct->_conf->rgw_directory_port4 ,nullptr, 0, 10, 1000);

  } catch(exception &e) {
	ldout(cct,10) << __func__ <<"Redis client connected failed with " << e.what()<< dendl;

  }
}

void RGWBlockDirectory::findClient(string key, cpp_redis::client *client){
  int slot = 0;
  slot = hash_slot(key.c_str(), key.size());

  try {
	/* if you had four *4* redis masters */
	if (slot < 4096)
	  client->connect(cct->_conf->rgw_directory_address, cct->_conf->rgw_directory_port, nullptr , 0, 5, 1000);
	else if (slot < 8192)
	  client->connect(cct->_conf->rgw_directory_address2, cct->_conf->rgw_directory_port2 ,nullptr, 0, 5, 1000);
	//client = &client2;
	else if (slot < 12288)
	  client->connect(cct->_conf->rgw_directory_address3, cct->_conf->rgw_directory_port3,nullptr, 0, 5, 1000);
	else
	  client->connect(cct->_conf->rgw_directory_address4, cct->_conf->rgw_directory_port4, nullptr, 0, 5, 1000);
	//client = &client3;

  }  catch(exception &e) {
	ldout(cct,10) << __func__ <<"Redis client connected failed with " << e.what()<< dendl;

  }

}




/* builds the index for the directory
 * based on bucket_name, obj_name, and chunk_id
 */
string RGWObjectDirectory::buildIndex(cache_obj *ptr){
  return ptr->bucket_name + "_" + ptr->obj_name;
}

string RGWBlockDirectory::buildIndex(cache_block *ptr){
  return ptr->c_obj.bucket_name + "_" + ptr->c_obj.obj_name + "_" + to_string(ptr->block_id);
}
/*
 * 1 if the key exists.
 * 0 if the key does not exist.
 */
int RGWObjectDirectory::existKey(string key, cpp_redis::client *client){

  ldout(cct,10) << __func__ << "  key1 " << key << dendl;
  int result = 0;

  try {
	vector<string> keys;
	keys.push_back(key);
//	ldout(cct,10) << __func__ << "  key2.1 " << key << dendl;

	client->exists(keys, [&result](cpp_redis::reply &reply){
		if (reply.is_integer())
		  result = reply.as_integer();
		});
	client->sync_commit(std::chrono::milliseconds(1000));
//	ldout(cct,10) << __func__ << " res " << result << " key " << key << dendl;
  }
  catch(exception &e) {
//	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	result =  0;
  }
  
  ldout(cct,10) << __func__ << "  key3 " << key <<" result:" <<result <<dendl;
  return result;
}

int RGWBlockDirectory::existKey(string key,cpp_redis::client *client){

  int result = 0;
  vector<string> keys;
  keys.push_back(key);
  if (!client->is_connected()){
	return result;
  }
  try {
	client->exists(keys, [&result](cpp_redis::reply &reply){
		if (reply.is_integer())
		  result = reply.as_integer();
		});
	client->sync_commit(std::chrono::milliseconds(1000));
	ldout(cct,10) << __func__ << " res dir " << result << " key " << key <<  dendl;
  }
  catch(exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
  
  }
  return result;
}

int RGWBlockDirectory::resetGlobalWeight(string key){
  auto start = chrono::steady_clock::now();
  cpp_redis::client client;
  findClient(key, &client);
  if (!client.is_connected()){
	return 0;
  }
  vector<pair<string, string>> list;
  list.push_back(make_pair("accessCount", "0"));
  client.hmset(key, list, [](cpp_redis::reply &reply){
	  });
  client.sync_commit(std::chrono::milliseconds(1000));
  auto end2 = chrono::steady_clock::now();
  ldout(cct,10) << __func__  << " ms " << chrono::duration_cast<chrono::microseconds>(end2 - start).count()  << dendl;
//  client.commit();

}

int RGWBlockDirectory::updateGlobalWeight(string key,  size_t weight , bool evict){
  auto start = chrono::steady_clock::now();
  cpp_redis::client client;
  findClient(key, &client);
  if (!client.is_connected()){
	ldout(cct,10) << __func__ << " Error: redis connect " << dendl;
	return -1;
  }
  string old_val;
  string endpoint=cct->_conf->remote_cache_addr;
  vector<string> keys;
  keys.push_back(key);
  std::vector<std::string> fields;
  int result = 0;
  if (evict) {
	ldout(cct,10) << __func__ << " key: " << key << " evict: "<<evict  <<dendl;
	try { 
	fields.push_back("hosts");
	client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
		if (reply.is_array()){
		  auto arr = reply.as_array();
		  if (!arr[0].is_null())
		  old_val = arr[0].as_string();
		}
		});
		
	client.sync_commit(std::chrono::milliseconds(1000));
	ldout(cct,10) << __func__ << " old_val: " << old_val << " evict: "<<evict  <<dendl;
	vector<pair<string, string>> list;
	string new_host_list;
	stringstream sloction(old_val);
	string tmp;
	bool first = true;
	while(getline(sloction, tmp, '_')) {
	  if (tmp.compare(endpoint) != 0) {
	  if (!first)
		new_host_list += "_";
	  else
		first = false;
	  new_host_list += tmp;
	  }
	}
	if (new_host_list.empty()){
	  ldout(cct,10) << __func__ << " old_val: " << old_val << " new_host: "<< new_host_list  <<dendl;
	  client.del(keys, [&result](cpp_redis::reply &reply){
		  if (reply.is_integer())
		  result = reply.as_integer(); });
	  client.sync_commit(std::chrono::milliseconds(1000));
	} else {
	  ldout(cct,10) << __func__ << " old_val: " << old_val << " new_host: "<< new_host_list  <<dendl;
	  list.push_back(make_pair("hosts", new_host_list));
	  client.hmset(key, list, [](cpp_redis::reply &reply){});
//	  client.sync_commit(std::chrono::milliseconds(1000));
	  client.hincrby(key, "accessCount", weight,  [&result](cpp_redis::reply &reply){
		  if (reply.is_integer())
		  result = reply.as_integer(); });
	  client.sync_commit(std::chrono::milliseconds(1000));
	}
  }
	catch(exception &e) {
	  ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
      return -1;
    }
  }

  ldout(cct,10) << __func__ <<" block2 key:" << key <<dendl;
  return result;
}
int RGWBlockDirectory::updateField(string key, string field, string value){

  vector<pair<string, string>> list;
  list.push_back(make_pair(field, value));
  cpp_redis::client client;

  findClient(key, &client);
  client.hmset(key, list, [](cpp_redis::reply &reply){
	  });
  client.sync_commit(std::chrono::milliseconds(1000));

  return 0;
}

int RGWObjectDirectory::updateField(cache_obj *ptr, string field, string value){

  vector<pair<string, string>> list;
  list.push_back(make_pair(field, value));
  cpp_redis::client client;
  string key = buildIndex(ptr);
  findClient(key, &client);
  if (!(client.is_connected())){
	return 0;
  }
  client.hmset(key, list, [](cpp_redis::reply &reply){
	  });
  client.sync_commit(std::chrono::milliseconds(1000));	

  return 0;
}

int RGWBlockDirectory::updateField(cache_block *ptr, string field, string value){

  vector<pair<string, string>> list;
  list.push_back(make_pair(field, value));
  cpp_redis::client client;

  string key = buildIndex(ptr);
  findClient(key, &client);
  client.hmset(key, list, [](cpp_redis::reply &reply){
	  });
  client.sync_commit(std::chrono::milliseconds(1000));	

  return 0;
}


int RGWObjectDirectory::delValue(cache_obj *ptr){
  int result = 0;
  vector<string> keys;
  cpp_redis::client client;
  string key = buildIndex(ptr);
  ldout(cct,10) << __func__ << " key " << key << dendl;
  keys.push_back(key);
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  try {
	client.del(keys, [&result](cpp_redis::reply &reply){
		if  (reply.is_integer())
		{result = reply.as_integer();}
		});
	client.sync_commit(std::chrono::milliseconds(1000));	
	ldout(cct,10) << __func__ << "DONE" << dendl;
	return result-1;
  }
  catch(exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -1;
  }
}
int RGWBlockDirectory::getHosts(string key, string hosts){
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return 0;
  }
  client.hget(key, "hosts", [&hosts](cpp_redis::reply &reply){
	  if(!reply.is_null())
	  hosts = reply.as_string();
	  });
  client.sync_commit(std::chrono::milliseconds(1000));
  return 0;
}

int RGWBlockDirectory::delValue(cache_block *ptr){
  int result = 0;
  vector<string> keys;

  cpp_redis::client client;
  string key = buildIndex(ptr);
  keys.push_back(key);
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  try {
	client.del(keys, [&result](cpp_redis::reply &reply){
		if (reply.is_array()){
		auto arr = reply.as_array();
		result = arr[0].as_integer();
		}
		});
	client.sync_commit(std::chrono::milliseconds(1000));	
	return result-1;
  }
  catch(exception &e) {
	ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	return -1;
  }
}


int RGWBlockDirectory::updateAccessCount(string key, int incr){
  int result = 0;
  //  int incr = 1;
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  client.hincrby(key, "accessCount", incr,  [&result](cpp_redis::reply &reply){
	  if (reply.is_integer()) {
	  result = reply.as_integer();
	  }
	  });
  client.sync_commit(std::chrono::milliseconds(1000));
  return result-1;

}


int RGWBlockDirectory::delValue(string key){
  int result = 0;
  vector<string> keys;
  cpp_redis::client client;
  keys.push_back(key);
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  client.del(keys, [&result](cpp_redis::reply &reply){
	  if( reply.is_integer()){
	  result = reply.as_integer();
	  }
	  });
  client.sync_commit(std::chrono::milliseconds(1000));
  return result-1;
}

/* adding a key to the directory
 * if the key exists, it will be deleted and then the new value be added
 */
int RGWObjectDirectory::setValue(cache_obj *ptr){

  cpp_redis::client client;
  string key = buildIndex(ptr);
  string result;

  vector<pair<string, string>> list;
  vector<string> keys;
  multimap<string, string> timeKey;
  vector<string> options;
  string hosts;

  stringstream ss;
  for(size_t i = 0; i < ptr->hosts_list.size(); ++i)
  {
	if(i != 0)
	  ss << "_";
	ss << ptr->hosts_list[i];
  }
  hosts = ss.str();

  //creating a list of key's properties
  list.push_back(make_pair("key", key));
  list.push_back(make_pair("owner", ptr->owner));
  list.push_back(make_pair("obj_acl", ptr->acl));
  list.push_back(make_pair("aclTimeStamp", to_string(ptr->aclTimeStamp)));
  list.push_back(make_pair("hosts", hosts));
  list.push_back(make_pair("dirty", BoolToString(ptr->dirty)));
  list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
  list.push_back(make_pair("creationTime", to_string(ptr->creationTime)));
  list.push_back(make_pair("lastAccessTime", to_string(ptr->lastAccessTime)));
  list.push_back(make_pair("etag", ptr->etag));
  list.push_back(make_pair("backendProtocol", protocolToString(ptr->backendProtocol)));
  list.push_back(make_pair("bucket_name", ptr->bucket_name));
  list.push_back(make_pair("obj_name", ptr->obj_name));
  list.push_back(make_pair("home_location", homeToString(ptr->home_location)));
  list.push_back(make_pair("intermediate", BoolToString(ptr->intermediate)));
  list.push_back(make_pair("mapping_id", ptr->mapping_id));
  list.push_back(make_pair("offset", to_string(ptr->offset)));

  //creating a key entry
  keys.push_back(key);

  //making key and time a pair
  timeKey.emplace(to_string(ptr->creationTime),key);

  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  client.hmset(key, list, [&result](cpp_redis::reply &reply){
	  result = reply.as_string();
	  });



  // synchronous commit, no timeout
  client.sync_commit(std::chrono::milliseconds(1000));

  //this will be used for aging policy
  //clientzadd("keyObjectDirectory", options, timeKey, [](cpp_redis::reply &reply){
  //});
  if (result.find("OK") != std::string::npos)
	return 0;
  else
	return -1;

}

int RGWBlockDirectory::getAvgCacheWeight(string endpoint){
  ldout(cct,10) <<__func__<<" endpoint: " << endpoint<<  dendl;
  cpp_redis::client client;
  findClient(endpoint, &client);
  if (!client.is_connected()){
	return -1;
  }
  string val ="-1";
  bool exist = false;
  client.get(endpoint, [&val,&exist](cpp_redis::reply& reply) {
	  if (!reply.is_null() && reply.is_string()){
	  val = reply.as_string();
	  exist = true;
	  }
	  });
  client.sync_commit(std::chrono::milliseconds(1000));
  if(!exist)
	return -1;
  return stoi(val);
}


int RGWBlockDirectory::setAvgCacheWeight(int64_t weight){
  int result = 0;
  if (weight == 0)
	return result;
  ldout(cct,10) <<__func__<< " weight: " << weight <<  dendl;
  string key = cct->_conf->remote_cache_addr;
  cpp_redis::client client;
  findClient(key, &client);
  if (!client.is_connected()){
	return result;
  }
  client.set(key, std::to_string(weight), [&result](cpp_redis::reply &reply){});
//  client.commit();
  client.sync_commit(std::chrono::milliseconds(1000));
  return result;
}

int RGWBlockDirectory::setValue(cache_block *ptr){

  //creating the index based on bucket_name, obj_name, and chunk_id
  string key = buildIndex(ptr);
  cpp_redis::client client;
  findClient(key, &client);
  if (!(client.is_connected())){
	return -1;
  }
  string result;
  string endpoint=cct->_conf->remote_cache_addr;
  int exist = 0;
  vector<string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	client.sync_commit(std::chrono::milliseconds(1000));
	ldout(cct,10) <<__func__<<" update directory for block:  " << key <<  dendl;
  }
  catch(exception &e) {
    exist = 0;
  }
	if (!exist)
	{
	vector<pair<string, string>> list;
	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("owner", ptr->c_obj.owner));
	list.push_back(make_pair("hosts", endpoint));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
	list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
	list.push_back(make_pair("block_id", to_string(ptr->block_id)));
	list.push_back(make_pair("lastAccessTime", to_string(ptr->lastAccessTime)));
	list.push_back(make_pair("accessCount", "0"));
	//list.push_back(make_pair("accessCount", to_string(ptr->access_count)));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
		if (!reply.is_null())
		result = reply.as_string();
		});
	client.sync_commit(std::chrono::milliseconds(1000));
	if (result.find("OK") != std::string::npos)
	  ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
	else
	  ldout(cct,10) <<__func__<<" else key res  " << result <<dendl;
	return 0;
  }
  else
  {ldout(cct,10) <<__func__<<" existing key  " << key <<dendl;
	string old_val;
	std::vector<std::string> fields;
	fields.push_back("hosts");
	try{
	client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
		if (reply.is_array()){
		auto arr = reply.as_array();
		if (!arr[0].is_null())
		old_val = arr[0].as_string();
		}
		});
	client.sync_commit(std::chrono::milliseconds(1000));
	}
	catch(exception &e) {
	  return 0;
	}
  
	string hosts;
	stringstream ss;
	bool new_cache = true;
	stringstream sloction(old_val);
	string tmp;
	while(getline(sloction, tmp, '_'))
	{
	  if (tmp.compare(endpoint) == 0)
		new_cache=false;
	}
	if(new_cache){	  
	//  if (old_val.compare("") == 0 )
		hosts = old_val +"_"+ endpoint;
	}
	vector<pair<string, string>> list;
	list.push_back(make_pair("hosts", hosts));
	list.push_back(make_pair("lastAccessTime", to_string(ptr->lastAccessTime)));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
//		result = reply.as_string();
		});
	client.hincrby(key, "accessCount", ptr->access_count, [&result](cpp_redis::reply &reply){});
	client.sync_commit(std::chrono::milliseconds(1000));
	//client.sync_commit(std::chrono::milliseconds(1000));
	//client.exec();
	return 0;

  }
}

int RGWObjectDirectory::setTTL(cache_obj *ptr, int seconds){

  int result;
  cpp_redis::client client;
  string key = buildIndex(ptr);

  findClient(key, &client);
  client.expire(key, seconds,  [&result](cpp_redis::reply& reply){
	  result = reply.as_integer();
	  });
  // synchronous commit, no timeout
  client.sync_commit(std::chrono::milliseconds(1000));

  return result-1;

}

int RGWBlockDirectory::setTTL(cache_block *ptr, int seconds){

  int result;
  cpp_redis::client client;
  string key = buildIndex(ptr);

  findClient(key, &client);
  client.expire(key, seconds,  [&result](cpp_redis::reply& reply){
	  result = reply.as_integer();
	  });
  // synchronous commit, no timeout
  client.sync_commit(std::chrono::milliseconds(1000));

  return result-1;

}




int RGWObjectDirectory::getValue(cache_obj *ptr){

  //delete the existing key,
  //to update an existing key, updateValue() should be used
  cpp_redis::client client;
  string key = buildIndex(ptr);
  ldout(cct,10) << __func__ << " object in func getValue "<< key << dendl;
  findClient(key, &client);
  int key_exist = -1;
  if (!client.is_connected()){
	return key_exist;
  }
  ldout(cct,10) << __func__ << " object in func getValue "<< key << dendl;
  if (existKey(key, &client)){
  try{
	string owner;
	string obj_acl;
	string aclTimeStamp;
	string hosts;
	string dirty;
	string size;
	string creationTime;
	string lastAccessTime;
	string etag;
	string backendProtocol;
	string bucket_name;
	string obj_name;
	string home_location;
	string intermediate;
	string mapping_id;
	string offset;

	//fields will be filled by the redis hmget functoin
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("owner");
	fields.push_back("obj_acl");
	fields.push_back("aclTimeStamp");
	fields.push_back("hosts");
	fields.push_back("dirty");
	fields.push_back("size");
	fields.push_back("creationTime");
	fields.push_back("lastAccessTime");
	fields.push_back("etag");
	fields.push_back("backendProtocol");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("home_location");
	fields.push_back("intermediate");
	fields.push_back("mapping_id");
	fields.push_back("offset");

	ldout(cct,10) << __func__ << " key exist so read field "<< key << dendl;
	  client.hmget(key, fields, [&key, &owner, &obj_acl, &aclTimeStamp, &hosts, &dirty, &size, &creationTime, &lastAccessTime, &etag, &backendProtocol, &bucket_name, &obj_name, &home_location, &intermediate, &mapping_id, &offset, &key_exist](cpp_redis::reply& reply){
		if (reply.is_array()){
		  auto arr = reply.as_array();
		  if (!arr[0].is_null()){
			key_exist = 0;
			key = arr[0].as_string();
			owner = arr[1].as_string();
			obj_acl = arr[2].as_string();
			aclTimeStamp = arr[3].as_string();
			hosts = arr[4].as_string();
			dirty = arr[5].as_string();
			size = arr[6].as_string();
			creationTime  = arr[7].as_string();
			lastAccessTime = arr[8].as_string();
			etag = arr[9].as_string();
			backendProtocol = arr[10].as_string();
			bucket_name = arr[11].as_string();
			obj_name = arr[12].as_string();
			home_location = arr[13].as_string();
			intermediate = arr[14].as_string();
			mapping_id = arr[15].as_string();
			offset = arr[16].as_string();
		  }
		}
	  });

	  client.sync_commit(std::chrono::milliseconds(1000));
	  
	  if (key_exist < 0){
		return key_exist;
		}
	
	ldout(cct,10) << __func__ << "found the entry  "<< key << "host "<< hosts << dendl;
	stringstream sloction(hosts);
	string tmp;
	ptr->owner = owner;
	ptr->acl = obj_acl;
	
	// ptr->aclTimeStamp = stoi(aclTimeStamp);
	ptr->aclTimeStamp = stoull(aclTimeStamp);
	//host1_host2_host3_...
	while(getline(sloction, tmp, '_'))
	  ptr->hosts_list.push_back(tmp);

	ptr->dirty = StringToBool(dirty);
	ptr->intermediate = StringToBool(intermediate);
	ptr->size_in_bytes = stoull(size);
	ptr->creationTime = stoull(creationTime);
	ptr->lastAccessTime = stoull(lastAccessTime);
	ptr->etag = etag;
	ptr->backendProtocol = stringToProtocol(backendProtocol);
	ptr->bucket_name = bucket_name;
	ptr->obj_name = obj_name;
	ptr->home_location = stringToHome(home_location);
	ptr->mapping_id = mapping_id;
	ptr->offset = stoull(offset);
//	client.disconnect(true);
	}
    catch(exception &e) {
      ldout(cct,10) << __func__ << " Error" << " key " << key << dendl;
	  return -1;
    }
  }
  return key_exist;
}

int RGWBlockDirectory::getValue(cache_block *ptr, string key){
  auto start = chrono::steady_clock::now();
  cpp_redis::client client;
  findClient(key, &client);
  ldout(cct,10) << __func__ <<" block2 key:" << key <<dendl;

  if (!client.is_connected()){
	ldout(cct,10) << __func__ <<" Error key:" << key <<dendl;
	return -1;
  }
  int key_exist = -1;
  if (existKey(key, &client))
  {
//	ldout(cct,10) << __func__ <<" blokc key:" << key <<dendl;
	string hosts;
	string size;
	string bucket_name;
	string obj_name;
	string block_id;
	string access_count;
	string owner;
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("hosts");
	fields.push_back("size");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("block_id");
	fields.push_back("accessCount");
	fields.push_back("owner");
  try {
	  client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &block_id, &access_count, &owner,  &key_exist](cpp_redis::reply &reply){
		if (reply.is_array()){
		  auto arr = reply.as_array();
			if (!arr[0].is_null()) {
		 key_exist = 0;
		  key = arr[0].as_string();
		  hosts = arr[1].as_string();
		size = arr[2].as_string();
		bucket_name = arr[3].as_string();
		obj_name = arr[4].as_string();
		block_id  = arr[5].as_string();
		access_count = arr[6].as_string();
		owner = arr[7].as_string();
		}
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));
	if (key_exist < 0 ) 
	  return -1;
	

	stringstream sloction(hosts);
	string tmp;
//	ldout(cct,10) << __func__ <<" ugur1:" << key << " hosts "<< hosts<<dendl;
	ptr->c_obj.owner = owner;
	//host1_host2_host3_...
	while(getline(sloction, tmp, '_')){
      if (tmp.compare(cct->_conf->remote_cache_addr) != 0){
        if(tmp.compare("") != 0){
		  ptr->cachedOnRemote = true;
		  ptr->hosts_list.push_back(tmp);
		}
    }}
	
	ptr->size_in_bytes = stoull(size);
	ptr->c_obj.bucket_name = bucket_name;
	ptr->c_obj.obj_name = obj_name;
	ptr->block_id = stoull(block_id);
//	ldout(cct,10) << __func__ <<" ugur1:" << key <<dendl;
	ptr->access_count = stoull(access_count);
//	ldout(cct,10) << __func__ <<" ugur2:" << key <<dendl;
//	client.disconnect(true);
  }
  catch(exception &e) {
    key_exist = -1;
  }
  
  }
  auto end2 = chrono::steady_clock::now();
  ldout(cct,10) << __func__  << " ms " << chrono::duration_cast<chrono::microseconds>(end2 - start).count()  << dendl;
  return key_exist;

}


int RGWBlockDirectory::getValue(cache_block *ptr){
  int key_exist = -1;
  string key = buildIndex(ptr);
  cpp_redis::client client;
  findClient(key, &client);
  ldout(cct,10) << __func__ <<" block1 keyi0:" << key <<dendl;
  if (!client.is_connected()){
	return -1;
  }

  ldout(cct,10) << __func__ <<" key:" << key <<dendl;
  if (existKey(key, &client)){

	string hosts;
	string size;
	string bucket_name;
	string obj_name;
	string block_id;
	string access_count;
	string owner;
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("hosts");
	fields.push_back("size");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	fields.push_back("block_id");
	fields.push_back("accessCount");
	fields.push_back("owner");

	try {

	  client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &block_id, &access_count, &owner, &key_exist](cpp_redis::reply &reply){
		  if (reply.is_array()) {
			auto arr = reply.as_array();
			if (!arr[0].is_null()) {
			  key_exist = 0;
			  key = arr[0].as_string();
			  hosts = arr[1].as_string();
			  size = arr[2].as_string();
			  bucket_name = arr[3].as_string();
			  obj_name = arr[4].as_string();
			  block_id  = arr[5].as_string();
			  access_count = arr[6].as_string();
			  owner = arr[7].as_string();
		 }}
		});

	  client.sync_commit(std::chrono::milliseconds(1000));
	  if (key_exist < 0 )
		return key_exist;
	
	ldout(cct,10) << __func__ << "found the block entry "<< key << " hosts " << hosts << dendl;
	stringstream sloction(hosts);
	string tmp;
	ptr->c_obj.owner = owner;

	//host1_host2_host3_...
	while(getline(sloction, tmp, '_')){
	  if (tmp.compare(cct->_conf->remote_cache_addr) != 0){
		ptr->hosts_list.push_back(tmp);
		ptr->cachedOnRemote = true;
		}
	}

	if (ptr->hosts_list.size() <= 0)
	  return -1;
	
	ptr->size_in_bytes = stoull(size);
	ptr->c_obj.bucket_name = bucket_name;
	ptr->c_obj.obj_name = obj_name;
	ptr->block_id = stoull(block_id);
	ptr->access_count = stoull(access_count);
	}
	
	catch(exception &e) {
	  return -1;
	}
  }
  return key_exist;
}


/* returns all the keys between startTime and endTime 
 * as a vector of <bucket_name, object_name, chunk_id, owner, creationTime> vectors
 */
vector<pair<vector<string>, time_t>> RGWObjectDirectory::get_aged_keys(time_t startTime_t, time_t endTime_t){
  vector<pair<string, string>> list;
  vector<pair<vector<string>, time_t>> keys; //return aged keys
  string key;
  string time;
  int rep_size = 0;

  string startTime = to_string(startTime_t);
  string endTime = to_string(endTime_t);

  /*
	 std::string dirKey = "keyObjectDirectory";
	 client1.zrangebyscore(dirKey, startTime, endTime, true, [&key, &time, &list](cpp_redis::reply &reply){
	 auto arr = reply.as_array();
	 for (unsigned i = 0; i < reply.as_array().size(); i+=2)
	 {
	 key = arr[i].as_string();
	 time = arr[i+1].as_string();

	 list.push_back(make_pair(key, time));
	 }
  //if (reply.is_error() == false)
  });
  client1.sync_commit(std::chrono::milliseconds(1000));

  rep_size = list.size();
  for (int i=0; i < rep_size; i++){
  vector<string> vTmp;
  string owner;

  stringstream key(list[i].first);
  string sTmp;
  //bucket_object_chunkID
  while(getline(key, sTmp, '_'))
  vTmp.push_back(sTmp);

  //getting owner for the directory
  client.hget(list[i].first, "owner", [&owner](cpp_redis::reply &reply){
  auto arr = reply.as_array();
  owner = arr[0].as_string();
  });
  client.sync_commit(std::chrono::milliseconds(1000));

  vTmp.push_back(owner);

  //creationTime and return value
  keys.push_back(make_pair(vTmp, stoi(list[i].second)));

  }

*/
  return keys;
}



