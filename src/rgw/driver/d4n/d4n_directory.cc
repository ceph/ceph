#include "d4n_directory.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

int RGWBlockDirectory::findClient(cpp_redis::client *client) {
  if (client->is_connected())
    return 0;

  if (host == "" || port == 0) {
    dout(10) << "RGW D4N Directory: D4N directory endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(host, port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

std::string RGWBlockDirectory::buildIndex(cache_block *ptr) {
  return "rgw-object:" + ptr->c_obj.obj_name + ":directory";
}

int RGWBlockDirectory::existKey(std::string key) {
  int result = -1;
  std::vector<std::string> keys;
  keys.push_back(key);
  
  if (!client.is_connected()) {
    return result;
  }

  try {
    client.exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer(); /* Returns 1 upon success */
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {}

  return result;
}

int RGWBlockDirectory::setValue(cache_block *ptr) {
  /* Creating the index based on obj_name */
  std::string key = buildIndex(ptr);
  if (!client.is_connected()) { 
    findClient(&client);
  }

  std::string result;
  std::vector<std::string> keys;
  keys.push_back(key);

  /* Every set will be new */
  if (host == "" || port == 0) {
    dout(10) << "RGW D4N Directory: Directory endpoint not configured correctly" << dendl;
    return -1;
  }
    
  std::string endpoint = host + ":" + std::to_string(port);
  std::vector<std::pair<std::string, std::string>> list;
    
  /* Creating a list of key's properties */
  list.push_back(make_pair("key", key));
  list.push_back(make_pair("size", std::to_string(ptr->size_in_bytes)));
  list.push_back(make_pair("bucket_name", ptr->c_obj.bucket_name));
  list.push_back(make_pair("obj_name", ptr->c_obj.obj_name));
  list.push_back(make_pair("hosts", endpoint)); 

  try {
    client.hmset(key, list, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
    
    if (result != "OK") {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RGWBlockDirectory::getValue(cache_block *ptr) {
  std::string key = buildIndex(ptr);

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    int field_exist = -1;
    
    std::string hosts;
    std::string size;
    std::string bucket_name;
    std::string obj_name;
    std::vector<std::string> fields;

    fields.push_back("key");
    fields.push_back("hosts");
    fields.push_back("size");
    fields.push_back("bucket_name");
    fields.push_back("obj_name");

    try {
      client.hmget(key, fields, [&key, &hosts, &size, &bucket_name, &obj_name, &field_exist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    field_exist = 0;
	    key = arr[0].as_string();
	    hosts = arr[1].as_string();
	    size = arr[2].as_string();
	    bucket_name = arr[3].as_string();
	    obj_name = arr[4].as_string();
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (field_exist < 0) {
        return field_exist;
      }

      /* Currently, there can only be one host */
      ptr->size_in_bytes = std::stoi(size);
      ptr->c_obj.bucket_name = bucket_name;
      ptr->c_obj.obj_name = obj_name;
    } catch(std::exception &e) {
      return -1;
    }
  }

  return 0;
}

int RGWBlockDirectory::delValue(cache_block *ptr) {
  int result = 0;
  std::vector<std::string> keys;
  std::string key = buildIndex(ptr);
  keys.push_back(key);
  
  if (!client.is_connected()) {
    findClient(&client);
  }
  
  if (existKey(key)) {
    try {
      client.del(keys, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer(); /* Returns 1 upon success */
        }
      });
	
      client.sync_commit(std::chrono::milliseconds(1000));	

      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    dout(20) << "RGW D4N Directory: Block is not in directory." << dendl;
    return -2;
  }
}
