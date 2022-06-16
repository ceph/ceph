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

void RGWBlockDirectory::findClient(string key, cpp_redis::client *client, int port){
  int slot = 0;
  slot = hash_slot(key.c_str(), key.size());
  
  if (client->is_connected()) 
    return;
  
  client->connect("127.0.0.1", port, nullptr);
  
  if (!client->is_connected())
  	exit(1);
}

string RGWBlockDirectory::buildIndex(cache_block *ptr){
  return ptr->c_obj.obj_name;
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
  }
  catch(exception &e) {
  
  }
  return result;
}

int RGWBlockDirectory::setValue(cache_block *ptr){
  //creating the index based on bucket_name, obj_name, and chunk_id
  string key = buildIndex(ptr);

  if (!client.is_connected()) 
    findClient(key, &client, 6379);

  string result;
  int exist = 0;
  vector<string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
  }
  catch(exception &e) {
    exist = 0;
  }
	if (!exist)
	{
	vector<pair<string, string>> list;
	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
	list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
		if (!reply.is_null())
		result = reply.as_string();
		});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	return 0;
  }
  else
  {
	string old_val;
	std::vector<std::string> fields;
	fields.push_back("hosts");
/*	try{
	client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
		if (reply.is_array()){
		auto arr = reply.as_array();
		if (!arr[0].is_null())
		old_val = arr[0].as_string();
		}
		});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	}
	catch(exception &e) {
	  return 0;
	}
*/
	string hosts;
	stringstream ss;
	bool new_cache = true;
	stringstream sloction(old_val);
	string tmp;
	while(getline(sloction, tmp, '_'))
	{
	  //if (tmp.compare(endpoint) == 0)
	  //	new_cache=false;
	}
	if(new_cache){	  
	//  if (old_val.compare("") == 0 )
		//hosts = old_val +"_"+ endpoint;
	}
	vector<pair<string, string>> list;
	list.push_back(make_pair("hosts", hosts));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
//		result = reply.as_string();
		});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	return 0;

  }
}

int RGWBlockDirectory::setValue(cache_block *ptr, int port){
  //creating the index based on bucket_name, obj_name, and chunk_id
  string key = buildIndex(ptr);

  if (!client.is_connected()) 
    findClient(key, &client, port);

  //cpp_redis::client client;
 /* if (!(client.is_connected())){
	return -1;
  }*/
  string result;
  //string endpoint=cct->_conf->remote_cache_addr;
  int exist = 0;
  vector<string> keys;
  keys.push_back(key);
  try{
    client.exists(keys, [&exist](cpp_redis::reply &reply){
	 	if (reply.is_integer()){
		  exist = reply.as_integer();
		}
	});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	
	//ldout(cct,10) <<__func__<<" update directory for block:  " << key <<  dendl;
  }
  catch(exception &e) {
    exist = 0;
  }
	if (!exist)
	{
	vector<pair<string, string>> list;
	//creating a list of key's properties
	list.push_back(make_pair("key", key));
	//list.push_back(make_pair("owner", ptr->c_obj.owner));
	//list.push_back(make_pair("hosts", endpoint));
	list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
	list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
	list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
	//list.push_back(make_pair("block_id", to_string(ptr->block_id)));
	//list.push_back(make_pair("lastAccessTime", to_string(ptr->lastAccessTime)));
	//list.push_back(make_pair("accessCount", "0"));
	//list.push_back(make_pair("accessCount", to_string(ptr->access_count)));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
		if (!reply.is_null())
		result = reply.as_string();
		});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	//if (result.find("OK") != std::string::npos)
	  //ldout(cct,10) <<__func__<<" new key res  " << result <<dendl;
	  ///////// Changes for directory testing ////////
	  //ldout(cct,5) <<__func__<<" Sam: new key res  " << result <<dendl;
	//else
	  //ldout(cct,10) <<__func__<<" else key res  " << result <<dendl; 
	  ///////// Changes for directory testing ////////
	  //ldout(cct,5) <<__func__<<" Sam: else key res  " << result <<dendl;
	return 0;
  }
  else
  {
	//ldout(cct,10) <<__func__<<" existing key  " << key <<dendl;
	string old_val;
	std::vector<std::string> fields;
	fields.push_back("hosts");
/*	try{
	client.hmget(key, fields, [&old_val](cpp_redis::reply &reply){
		if (reply.is_array()){
		auto arr = reply.as_array();
		if (!arr[0].is_null())
		old_val = arr[0].as_string();
		}
		});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	}
	catch(exception &e) {
	  return 0;
	}
*/
	string hosts;
	stringstream ss;
	bool new_cache = true;
	stringstream sloction(old_val);
	string tmp;
	while(getline(sloction, tmp, '_'))
	{
	  //if (tmp.compare(endpoint) == 0)
	  //	new_cache=false;
	}
	if(new_cache){	  
	//  if (old_val.compare("") == 0 )
		//hosts = old_val +"_"+ endpoint;
	}
	vector<pair<string, string>> list;
	list.push_back(make_pair("hosts", hosts));
	//list.push_back(make_pair("lastAccessTime", to_string(ptr->lastAccessTime)));
	client.hmset(key, list, [&result](cpp_redis::reply &reply){
//		result = reply.as_string();
		});
	//client.hincrby(key, "accessCount", ptr->access_count, [&result](cpp_redis::reply &reply){});
	//client.sync_commit(std::chrono::milliseconds(1000));
	client.sync_commit();
	//client.sync_commit(std::chrono::milliseconds(1000));
	//client.exec();
	return 0;

  }
}

int RGWBlockDirectory::getValue(cache_block *ptr){
  int key_exist = -2;
  string key = buildIndex(ptr);
  //cpp_redis::client client;
  
  if (!client.is_connected()) 
    findClient(key, &client, 6379);
  // ldout(cct,10) << __func__ <<" block1 keyi0:" << key <<dendl;
  ///////// Changes for directory testing ////////
  //ldout(cct,5) << __func__ <<" Sam: block1 keyi0:" << key <<dendl;
  /*if (!client.is_connected())
  	return -1;*/
 
  // ldout(cct,10) << __func__ <<" key:" << key <<dendl;
  ///////// Changes for directory testing ////////
  //ldout(cct,5) << __func__ <<" Sam: key:" << key <<dendl;
  if (existKey(key, &client)){

	string hosts;
	string size;
	string bucket_name;
	string obj_name;
	//string block_id;
	//string access_count;
	//string owner;
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("hosts");
	fields.push_back("size");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");
	//fields.push_back("block_id");
	//fields.push_back("accessCount");
	//fields.push_back("owner");

	try {

	  client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &key_exist](cpp_redis::reply &reply){
	  //client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &block_id, &access_count, &owner, &key_exist](cpp_redis::reply &reply){
		  if (reply.is_array()) {
			auto arr = reply.as_array();
			if (!arr[0].is_null()) {
		//	  key_exist = 0;
			  key = arr[0].as_string();
			  hosts = arr[1].as_string();
			  size = arr[2].as_string();
			  bucket_name = arr[3].as_string();
			  obj_name = arr[4].as_string();
			  //block_id  = arr[5].as_string();
			  //access_count = arr[6].as_string();
			  //owner = arr[7].as_string();
		 }}
		});

	  //client.sync_commit(std::chrono::milliseconds(1000));
	  client.sync_commit();

	  if (key_exist < 0 ) {
		return key_exist;
	  }
	
	stringstream sloction(hosts);
	string tmp;

	//host1_host2_host3_...
	//while(getline(sloction, tmp, '_')){
	 // if (tmp.compare(cct->_conf->remote_cache_addr) != 0){
	//	ptr->hosts_list.push_back(tmp);
		//ptr->cachedOnRemote = true;
	//	}
	//}

	if (ptr->hosts_list.size() <= 0) {
	  return -1;
	}

	ptr->size_in_bytes = stoull(size);
	ptr->c_obj.bucket_name = bucket_name;
	ptr->c_obj.obj_name = obj_name;
	}
	
	catch(exception &e) {
	  return -1;
	}
  }
  //client.disconnect(true);
  return key_exist;
}

int RGWBlockDirectory::getValue(cache_block *ptr, int port){
  int key_exist = -2;
  string key = buildIndex(ptr);
  //cpp_redis::client client;
  
  if (!client.is_connected()) 
    findClient(key, &client, port);
  
  if (existKey(key, &client)){

	string hosts;
	string size;
	string bucket_name;
	string obj_name;
	std::vector<std::string> fields;
	fields.push_back("key");
	fields.push_back("hosts");
	fields.push_back("size");
	fields.push_back("bucket_name");
	fields.push_back("obj_name");

	try {

	 // client.hget(key, "obj_name", [&](cpp_redis::reply &reply){
	 client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &key_exist](cpp_redis::reply &reply){
	  //client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &block_id, &access_count, &owner, &key_exist](cpp_redis::reply &reply){
		 if (reply.is_array()) {
			auto arr = reply.as_array();
			if (!arr[0].is_null()) {
			  key_exist = 0;
			  key = arr[0].as_string();
			  hosts = arr[1].as_string();
			  size = arr[2].as_string();
			  bucket_name = arr[3].as_string();
			  obj_name = arr[4].as_string();
		 }}
		
	/*	if (!reply.is_null())
	  obj_name = reply.as_string();*/

	  });
	  //client.sync_commit(std::chrono::milliseconds(1000));
	  client.sync_commit();

	  if (key_exist < 0 ) {
		return key_exist;
	  }
	
	stringstream sloction(hosts);
	string tmp;

	if (ptr->hosts_list.size() <= 0) {
	  return -1;
	}

	ptr->size_in_bytes = stoull(size);
	ptr->c_obj.bucket_name = bucket_name;
	ptr->c_obj.obj_name = obj_name;
	}
	
	catch(exception &e) {
	  return -1;
	}
  }
  return key_exist;
}
