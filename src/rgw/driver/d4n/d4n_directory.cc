#include "d4n_directory.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

int BlockDirectory::find_client(cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

   if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: D4N directory endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(addr.host, addr.port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

std::string BlockDirectory::build_index(CacheBlock* block) {
  return "rgw-object:" + block->cacheObj.objName + ":directory";
}

int BlockDirectory::exist_key(std::string key) {
  int result = 0;
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

int BlockDirectory::set_value(CacheBlock* block) {
  /* Creating the index based on objName */
  std::string key = build_index(block);
  if (!client.is_connected()) { 
    find_client(&client);
  }

  std::string result;
  std::vector<std::string> keys;
  keys.push_back(key);

  /* Every set will be new */
  if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: Directory endpoint not configured correctly" << dendl;
    return -1;
  }
    
  std::string endpoint = addr.host + ":" + std::to_string(addr.port);
  std::vector< std::pair<std::string, std::string> > list;
    
  /* Creating a list of the entry's properties */
  list.push_back(make_pair("key", key));
  list.push_back(make_pair("size", std::to_string(block->size)));
  list.push_back(make_pair("globalWeight", std::to_string(block->globalWeight)));
  list.push_back(make_pair("bucketName", block->cacheObj.bucketName));
  list.push_back(make_pair("objName", block->cacheObj.objName));
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

int BlockDirectory::get_value(CacheBlock* block) {
  int keyExist = -2;
  std::string key = build_index(block);

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (existKey(key)) {
    int field_exist = -1;
    
    std::string hosts;
    std::string size;
    std::string bucketName;
    std::string objName;
    std::vector<std::string> fields;

    fields.push_back("key");
    fields.push_back("hosts");
    fields.push_back("size");
    fields.push_back("bucketName");
    fields.push_back("objName");

    try {
      client.hmget(key, fields, [&key, &hosts, &size, &bucketName, &objName, &keyExist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    keyExist = 0;
	    key = arr[0].as_string();
	    hosts = arr[1].as_string();
	    size = arr[2].as_string();
	    bucketName = arr[3].as_string();
	    objName = arr[4].as_string();
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (keyExist < 0 ) {
        return keyExist;
      }

      /* Currently, there can only be one host */
      block->size = std::stoi(size);
      block->cacheObj.bucketName = bucketName;
      block->cacheObj.objName = objName;
    } catch(std::exception &e) {
      keyExist = -1;
    }
  }

  return keyExist;
}

int BlockDirectory::copy_value(CacheBlock* block, CacheBlock* copy_block) {
  std::string result;
  std::string key = build_index(block);
  
  if (!client.is_connected()){
    find_client(&client);
  }
  
  if (exist_key(key)) {
    std::vector< std::pair<std::string, std::string> > list;
    key = build_index(copy_block); 
      
    /* Creating a list of the copy's properties */
    list.push_back(make_pair("key", key));
    list.push_back(make_pair("size", std::to_string(copy_block->size)));
    list.push_back(make_pair("globalWeight", std::to_string(copy_block->globalWeight))); // Do we want to reset global weight? -Sam
    list.push_back(make_pair("bucketName", copy_block->cacheObj.bucketName));
    list.push_back(make_pair("objName", copy_block->cacheObj.objName));
    list.push_back(make_pair("hosts", copy_block->hostsList[0])); 

    /* Set copy with new values */
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
      return -2;
    }
  }

  return 0;
}

int BlockDirectory::del_value(CacheBlock* block){
  int result = 0;
  std::vector<std::string> keys;
  std::string key = build_index(block);
  keys.push_back(key);
  
  if (!client.is_connected()){
    find_client(&client);
  }
  
  if (exist_key(key)) {
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

} } // namespace rgw::d4n
