#include "common/async/blocked_completion.h"
#include "d4n_directory.h"
#include <time.h>

namespace rgw { namespace d4n {

template <typename T>
void redis_exec(connection* conn, boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    conn->async_exec(req, resp, yield[ec]);
  } else {
    conn->async_exec(req, resp, ceph::async::use_blocked[ec]);
  }
}

int ObjectDirectory::find_client(cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

   if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: D4N directory endpoint was not configured correctly" << dendl;
    return -EDESTADDRREQ;
  }

  client->connect(addr.host, addr.port, nullptr);

  if (!client->is_connected())
    return -ECONNREFUSED;

  return 0;
}

std::string ObjectDirectory::build_index(CacheObj* object) {
  return object->bucketName + "_" + object->objName;
}

int ObjectDirectory::exist_key(std::string key) {
  int result = 0;
  std::vector<std::string> keys;
  keys.push_back(key);
  
  if (!client.is_connected()) {
    return result;
  }

  try {
    client.exists(keys, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });
    
    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {}

  return result;
}

int ObjectDirectory::set_value(CacheObj* object) {
  /* Creating the index based on objName */
  std::string result;
  std::string key = build_index(object);
  if (!client.is_connected()) { 
    find_client(&client);
  }

  /* Every set will be new */
  if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: Directory endpoint not configured correctly" << dendl;
    return -2;
  }
    
  std::string endpoint = addr.host + ":" + std::to_string(addr.port);
  std::vector< std::pair<std::string, std::string> > list;
    
  /* Creating a list of the entry's properties */
  list.push_back(make_pair("key", key));
  list.push_back(make_pair("objName", object->objName));
  list.push_back(make_pair("bucketName", object->bucketName));
  list.push_back(make_pair("creationTime", std::to_string(object->creationTime)));
  list.push_back(make_pair("dirty", std::to_string(object->dirty)));
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

int ObjectDirectory::get_value(CacheObj* object) {
  int keyExist = -2;
  std::string key = build_index(object);

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    std::string key;
    std::string objName;
    std::string bucketName;
    std::string creationTime;
    std::string dirty;
    std::string hosts;
    std::vector<std::string> fields;

    fields.push_back("key");
    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("hosts");

    try {
      client.hmget(key, fields, [&key, &objName, &bucketName, &creationTime, &dirty, &hosts, &keyExist](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    keyExist = 0;
	    key = arr[0].as_string();
	    objName = arr[1].as_string();
	    bucketName = arr[2].as_string();
	    creationTime = arr[3].as_string();
	    dirty = arr[4].as_string();
	    hosts = arr[5].as_string();
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (keyExist < 0) {
        return keyExist;
      }

      /* Currently, there can only be one host */
      object->objName = objName;
      object->bucketName = bucketName;

      struct std::tm tm;
      std::istringstream(creationTime) >> std::get_time(&tm, "%T");
      strptime(creationTime.c_str(), "%T", &tm); // Need to check formatting -Sam
      object->creationTime = mktime(&tm);

      std::istringstream(dirty) >> object->dirty;
    } catch(std::exception &e) {
      keyExist = -1;
    }
  }

  return keyExist;
}

int ObjectDirectory::del_value(CacheObj* object) {
  int result = 0;
  std::vector<std::string> keys;
  std::string key = build_index(object);
  keys.push_back(key);
  
  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key)) {
    try {
      client.del(keys, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });
	
      client.sync_commit(std::chrono::milliseconds(1000));	
      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }
}

int BlockDirectory::find_client(cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

   if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW D4N Directory: D4N directory endpoint was not configured correctly" << dendl;
    return -EDESTADDRREQ;
  }

  client->connect(addr.host, addr.port, nullptr);

  if (!client->is_connected())
    return -ECONNREFUSED;

  return 0;
}

std::string BlockDirectory::build_index(CacheBlock* block) {
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + boost::lexical_cast<std::string>(block->version);
}

int BlockDirectory::exist_key(std::string key, optional_yield y) {
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec(conn, ec, req, resp, y);

    if ((bool)ec)
      return false;
  } catch(std::exception &e) {}

  return std::get<0>(resp).value();
}

int BlockDirectory::set_value(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);
    
  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  std::string endpoint = addr.host + ":" + std::to_string(addr.port);
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("version");
  redisValues.push_back(std::to_string(block->version));
  redisValues.push_back("size");
  redisValues.push_back(std::to_string(block->size));
  redisValues.push_back("globalWeight");
  redisValues.push_back(std::to_string(block->globalWeight));
  redisValues.push_back("blockHosts");
  redisValues.push_back(endpoint); // Set in filter -Sam

  redisValues.push_back("objName");
  redisValues.push_back(block->cacheObj.objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(block->cacheObj.bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(std::to_string(block->cacheObj.creationTime)); 
  redisValues.push_back("dirty");
  redisValues.push_back(std::to_string(block->cacheObj.dirty));
  redisValues.push_back("objHosts");
  redisValues.push_back(endpoint); // Set in filter -Sam

  try {
    boost::system::error_code ec;
    response<std::string> resp;

    request req;
    req.push_range("HMSET", key, redisValues);

    redis_exec(conn, ec, req, resp, y);

    if (std::get<0>(resp).value() != "OK" || (bool)ec) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int BlockDirectory::get_value(CacheBlock* block, optional_yield y) {
  //int keyExist = -2;
  std::string key = build_index(block);

/*
  if (!client.is_connected()) {
    find_client(&client);
  }*/

  if (exist_key(key, y)) {
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
/*
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
      }*/

      /* Currently, there can only be one host */ // update -Sam
      block->size = std::stoi(size);
      block->cacheObj.bucketName = bucketName;
      block->cacheObj.objName = objName;
    } catch(std::exception &e) {
      //keyExist = -1;
    }
  }

  return 0;//keyExist;
}

int BlockDirectory::del_value(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);

  if (exist_key(key, y)) {
    try {
      boost::system::error_code ec;
      response<int> resp;
      request req;
      req.push("DEL", key);

      redis_exec(conn, ec, req, resp, y);
      conn->cancel();

      if (ec)
        return -1;

      return std::get<0>(resp).value() - 1; 
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return 0; /* No delete was necessary */
  }

/*  int result = 0;
  std::vector<std::string> keys;
  std::string key = build_index(block);
  keys.push_back(key);
  
  if (!client.is_connected()) {
    find_client(&client);
  }
  
  if (exist_key(key, y)) {
    try {
      client.del(keys, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });
	
      client.sync_commit(std::chrono::milliseconds(1000));	
      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2; // update logic -Sam
  }*/
}

int BlockDirectory::update_field(CacheBlock* block, std::string field, std::string value) { // represent in cache block too -Sam
  std::string result;
  std::string key = build_index(block);

  if (!client.is_connected()) {
    find_client(&client);
  }
  
//  if (exist_key(key)) {
    if (field == "hostsList") {
      /* Append rather than overwrite */
      std::string hosts;

      try {
        client.hget(key, "hostsList", [&hosts](cpp_redis::reply& reply) {
          if (!reply.is_null()) {
            hosts = reply.as_string();
          }
        });

        client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
        return -1;
      }
      
      value += "_";
      value += hosts;
    }

    /* Update cache block */ // Remove ones that aren't used -Sam
    if (field == "size")
      block->size = std::stoi(value);
    else if (field == "bucketName")
      block->cacheObj.bucketName = value;
    else if (field == "objName")
      block->cacheObj.objName = value;
    else if (field == "hostsList")
      block->hostsList.push_back(value);

    std::vector< std::pair<std::string, std::string> > list;
    list.push_back(std::make_pair(field, value));

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
 // }

  return 0;
}

} } // namespace rgw::d4n
