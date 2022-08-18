#include "rgw_d4n_datacache.h"
#include "rgw_sal_d4n.h"
#include <iostream>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

std::vector< std::pair<std::string, std::string> > RGWD4NCache::buildObject(rgw::sal::Attrs* baseBinary, rgw::sal::Attrs* newBinary) {
  std::vector< std::pair<std::string, std::string> > values;
  rgw::sal::Attrs::iterator attrs;
 
  // Convert to vector
  if (baseBinary != NULL && newBinary != NULL) {
    for (attrs = baseBinary->begin(); attrs != baseBinary->end(); ++attrs) {
      values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
    }

    // Update attributes
    if (!newBinary->empty()) {
      for (attrs = newBinary->begin(); attrs != newBinary->end(); ++attrs) {
        int index = 0;

        for (const auto& pair : values) {
          if (pair.first == attrs->first) {
            break;
          }
      
          index++;
        }
    
        if (index != (int)values.size()) {
          values[index] = std::make_pair(attrs->first, attrs->second.to_str());
        } else {
          values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
	}
      }
    }
  } else if (newBinary != NULL) {
    // Update attributes
    if (!newBinary->empty()) {
      for (attrs = newBinary->begin(); attrs != newBinary->end(); ++attrs) {
        int index = 0;

        for (const auto& pair : values) {
          if (pair.first == attrs->first) {
            break;
          }
      
          index++;
        }
    
        if (index != (int)values.size()) {
          values[index] = std::make_pair(attrs->first, attrs->second.to_str());
        } else {
          values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
	}

	return values;
      }
    }
  } 

  return values; 
}

void RGWD4NCache::findClient(cpp_redis::client *client) { 
  if (client->is_connected())
    return;

  client->connect(host, port, nullptr);

  if (!client->is_connected())
    exit(1);
}

int RGWD4NCache::existKey(std::string key) { 
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

int RGWD4NCache::setObject(rgw::sal::Attrs baseAttrs, rgw::sal::Attrs* newAttrs, std::string oid) {
  // Creating the index based on obj_name
  std::string key = "rgw-object:" + oid + ":cache";
  if (!client.is_connected()) {
    findClient(&client);
  }

  std::string result;
  std::vector<std::string> keys;
  keys.push_back(key);

  // Every set will be new
  if (host == "" || port == 0) {
    dout(10) << "RGW D4N Cache: D4N cache endpoint not configured correctly" << dendl;
    return -1;
  }

  try {
    std::vector< std::pair<std::string, std::string> > redisObject = buildObject(&baseAttrs, newAttrs);
      
    if (redisObject.empty()) {
      return -1;
    }
      
    client.hmset(key, redisObject, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RGWD4NCache::getObject(rgw::sal::Object* source) {
  int key_exist = -2;
  rgw::sal::Attrs newAttrs;
  std::string key = "rgw-object:" + source->get_name() + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    rgw::sal::Attrs::iterator it;
    rgw::sal::Attrs newAttrs;
    std::vector<std::string> fields;

    for (it = source->get_attrs().begin(); it != source->get_attrs().end(); ++it) {
      fields.push_back(it->first);
    }

    
    try {
      client.hmget(key, fields, [&key_exist, &newAttrs, &fields](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    key_exist = 0;

            for (int i = 0; i < static_cast<int>(fields.size()); ++i) {
	      std::string tmp = arr[i].as_string();
              buffer::list bl;
	      bl.append(tmp.data(), std::strlen(tmp.data()));
	      newAttrs.insert({fields[i], bl});
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (key_exist < 0 ) {
        dout(20) << "RGW D4N Cache: Object is not in cache." << dendl;
        return key_exist;
      }
   
      // Update the zipper object's attributes 
      int set_attrs_return = source->set_attrs(newAttrs);

      if (set_attrs_return < 0) {
        dout(20) << "RGW D4N Cache: Zipper object's attributes were not set." << dendl;
      }
    } catch(std::exception &e) {
      return -1;
    }
  }
 
  return key_exist;
}

int RGWD4NCache::delObject(rgw::sal::Object* source) {
  int result = 0;
  std::vector<std::string> keys;
  std::string key = "rgw-object:" + source->get_name() + ":cache";
  keys.push_back(key);

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    try {
      client.del(keys, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
      return result-1;
    } catch(std::exception &e) {
      return -1;
    }
  }
    
  dout(20) << "RGW D4N Cache: Object is not in cache." << dendl;
  return -2;
}

int RGWD4NCache::updateAttrs(std::string oid, rgw::sal::Attrs* updateAttrs) {
  int result = 0;
  std::string key = "rgw-object:" + oid + ":cache";
 
  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    try {
      std::vector< std::pair<std::string, std::string> > redisObject = buildObject(NULL, updateAttrs);
      
      if (redisObject.empty()) {
        return -1;
      }

      client.hmset(key, redisObject, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  }

  dout(20) << "RGW D4N Cache: Object is not in cache." << dendl;
  return -2;
}

int RGWD4NCache::delAttrs(std::string oid, std::vector<std::string> fields) {
  int result = 0;
  std::string key = "rgw-object:" + oid + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    try {
      client.hdel(key, fields, [&result](cpp_redis::reply &reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  } 
  
  dout(20) << "RGW D4N Cache: Object is not in cache." << dendl;
  return -2;
}
