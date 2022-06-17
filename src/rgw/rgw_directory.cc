#include <errno.h>
#include <cpp_redis/cpp_redis>
#include "rgw_directory.h"
#include <string>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <list>

void RGWBlockDirectory::findClient(string key, cpp_redis::client *client, int port) {
  if (client->is_connected()) 
    return;
  
  client->connect("127.0.0.1", port, nullptr);
  
  if (!client->is_connected())
    exit(1);
}

string RGWBlockDirectory::buildIndex(cache_block *ptr) {
  return ptr->c_obj.obj_name;
}

int RGWBlockDirectory::existKey(string key,cpp_redis::client *client) {
  int result = 0;
  vector<string> keys;
  keys.push_back(key);
  
  if (!client->is_connected()) {
    return result;
  }

  try {
    client->exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });
    
    client->sync_commit(std::chrono::milliseconds(1000));
  }
  catch(exception &e) {}

  return result;
}

int RGWBlockDirectory::setValue(cache_block *ptr) {
  //creating the index based on bucket_name, obj_name, and chunk_id
  string key = buildIndex(ptr);

  if (!client.is_connected()) { 
    findClient(key, &client, 6379);
  }

  string result;
  int exist = 0;
  vector<string> keys;
  keys.push_back(key);

  try {
    client.exists(keys, [&exist](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        exist = reply.as_integer();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
  }
  catch(exception &e) {
    exist = 0;
  }

  if (!exist) {
    vector<pair<string, string>> list;
    
    //creating a list of key's properties
    list.push_back(make_pair("key", key));
    list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
    list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
    list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
    list.push_back(make_pair("hosts", ptr->hosts_list[0]));

    client.hmset(key, list, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
	
    return 0;
  } else {
    string old_val;
    std::vector<std::string> fields;
    fields.push_back("hosts");
    
    try {
      client.hmget(key, fields, [&old_val](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();
	  
	  if (!arr[0].is_null()) {
	    old_val = arr[0].as_string();
	  }
	}
      });
      
      client.sync_commit(std::chrono::milliseconds(1000));
    }
	catch(exception &e) {
	  return 0;
	}

    string hosts;
    stringstream ss;
    stringstream sloction(old_val);
    string tmp;
    vector<pair<string, string>> list;
    
    list.push_back(make_pair("hosts", hosts));
    client.hmset(key, list, [&result](cpp_redis::reply &reply) {});
    client.sync_commit(std::chrono::milliseconds(1000));
    
    return 0;
  }
}

int RGWBlockDirectory::setValue(cache_block *ptr, int port) {
  //creating the index based on bucket_name, obj_name, and chunk_id
  string key = buildIndex(ptr);

  if (!client.is_connected()) { 
    findClient(key, &client, port);
  }

  string result;
  int exist = 0;
  vector<string> keys;
  keys.push_back(key);

  try {
    client.exists(keys, [&exist](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        exist = reply.as_integer();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
  }
  catch(exception &e) {
    exist = 0;
  }

  if (!exist) {
    vector<pair<string, string>> list;
    
    //creating a list of key's properties
    list.push_back(make_pair("key", key));
    list.push_back(make_pair("size", to_string(ptr->size_in_bytes)));
    list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
    list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
    list.push_back(make_pair("hosts", ptr->hosts_list[0]));

    client.hmset(key, list, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
	
    return 0;
  } else {
    string old_val;
    std::vector<std::string> fields;
    fields.push_back("hosts");
    
    try {
      client.hmget(key, fields, [&old_val](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();
	  
	  if (!arr[0].is_null()) {
	    old_val = arr[0].as_string();
	  }
	}
      });
      
      client.sync_commit(std::chrono::milliseconds(1000));
    }
    catch(exception &e) {
      return 0;
    }

    string hosts;
    stringstream ss;
    stringstream sloction(old_val);
    string tmp;
    vector<pair<string, string>> list;
    
    list.push_back(make_pair("hosts", hosts));
    client.hmset(key, list, [&result](cpp_redis::reply &reply) {});
    client.sync_commit(std::chrono::milliseconds(1000));
    
    return 0;
  }
}

int RGWBlockDirectory::getValue(cache_block *ptr) {
  int key_exist = -2;
  string key = buildIndex(ptr);
  
  if (!client.is_connected()) {
    findClient(key, &client, 6379);
  }
  
  if (existKey(key, &client)) {
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
      client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &key_exist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();
	      
	  if (!arr[0].is_null()) {
	    key_exist = 0;
	    key = arr[0].as_string();
	    hosts = arr[1].as_string();
	    size = arr[2].as_string();
	    bucket_name = arr[3].as_string();
	    obj_name = arr[4].as_string();
	  }
	}
      });
	  
      client.sync_commit(std::chrono::milliseconds(1000));

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

int RGWBlockDirectory::getValue(cache_block *ptr, int port) {
  int key_exist = -2;
  string key = buildIndex(ptr);
  
  if (!client.is_connected()) {
    findClient(key, &client, port);
  }
  
  if (existKey(key, &client)) {
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
      client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &key_exist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();
	      
	  if (!arr[0].is_null()) {
	    key_exist = 0;
	    key = arr[0].as_string();
	    hosts = arr[1].as_string();
	    size = arr[2].as_string();
	    bucket_name = arr[3].as_string();
	    obj_name = arr[4].as_string();
	  }
	}
      });
	  
      client.sync_commit(std::chrono::milliseconds(1000));

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
