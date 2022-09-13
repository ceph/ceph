#include "rgw_d4n_datacache.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

std::vector< std::pair<std::string, std::string> > RGWD4NCache::buildObject(rgw::sal::Attrs* baseBinary, rgw::sal::Attrs* newBinary) {
  std::vector< std::pair<std::string, std::string> > values;
  rgw::sal::Attrs::iterator attrs;
 
  /* Convert to vector */
  if (baseBinary != NULL) {
    for (attrs = baseBinary->begin(); attrs != baseBinary->end(); ++attrs) {
      values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
    }

    /* Update attributes */
    if (!newBinary->empty()) {
      for (attrs = newBinary->begin(); attrs != newBinary->end(); ++attrs) {
        long unsigned int index = 0;

	/* Find if attribute already exists */
        for (const auto& pair : values) {
          if (pair.first == attrs->first) {
            break;
          }
      
          index++;
        }
    
        if (index != values.size()) {
          values[index] = std::make_pair(attrs->first, attrs->second.to_str());
        } else {
	  /* If not, append it to existing attributes */
          values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
	}
      }
    }
  } else if (newBinary != NULL) {
    /* Update attributes */
    if (!newBinary->empty()) {
      for (attrs = newBinary->begin(); attrs != newBinary->end(); ++attrs) {
        long unsigned int index = 0;

        for (const auto& pair : values) {
          if (pair.first == attrs->first) {
            break;
          }
      
          index++;
        }
    
        if (index != values.size()) {
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

  if (host == "" || port == 0) { 
    dout(10) << "RGW D4N Cache: D4N cache endpoint not configured correctly" << dendl;
    exit(-1);
  }

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

int RGWD4NCache::setObject(std::string oid, rgw::sal::Attrs* baseAttrs, rgw::sal::Attrs* newAttrs) {
  /* Creating the index based on obj_name */
  std::string key = "rgw-object:" + oid + ":cache";
  std::string result;

  if (!client.is_connected()) {
    findClient(&client);
  }

  /* Every set will be treated as new */
  try {
    std::vector< std::pair<std::string, std::string> > redisObject = buildObject(baseAttrs, newAttrs);
      
    if (redisObject.empty()) {
      return -1;
    }
      
    client.hmset(key, redisObject, [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != "OK") {
      return -1;
    }

    return 0;
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RGWD4NCache::getObject(std::string oid, rgw::sal::Attrs* baseAttrs, rgw::sal::Attrs* newAttrs) {
  int key_exist = -2;
  std::string key = "rgw-object:" + oid + ":cache";
  std::vector<std::string> values;
  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    rgw::sal::Attrs::iterator it;
    std::vector<std::string> fields;

    for (it = baseAttrs->begin(); it != baseAttrs->end(); ++it) {
      fields.push_back(it->first);
    }

    try {
      client.hmget(key, fields, [&key_exist, &newAttrs, &fields, &values](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    key_exist = 0;

            for (long unsigned int i = 0; i < fields.size(); ++i) {
	      std::string tmp = arr[i].as_string();
              buffer::list bl;
	      bl.append(tmp.data(), std::strlen(tmp.data()));
	      newAttrs->insert({fields[i], bl});
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));

      if (key_exist < 0) {
        dout(20) << "RGW D4N Cache: Object was not retrievable." << dendl;
        return -1;
      }
    } catch(std::exception &e) {
      exit(-1);
    }
  } else {
    return -2;
  }

  return 0;
}

int RGWD4NCache::delObject(std::string oid) {
  int result = 0;
  std::vector<std::string> keys;
  std::string key = "rgw-object:" + oid + ":cache";
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
      
      return result - 1;
    } catch(std::exception &e) {
      return -1;
    }
  }

  dout(20) << "RGW D4N Cache: Object is not in cache." << dendl;
  return -2;
}

int RGWD4NCache::delAttrs(std::string oid, std::vector<std::string>& baseFields, std::vector<std::string>& deleteFields) {
  int result = 0;
  std::string key = "rgw-object:" + oid + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    /* Find if attribute doesn't exist */
    for (const auto& delField : deleteFields) {
      if (std::find(baseFields.begin(), baseFields.end(), delField) == baseFields.end()) {
        deleteFields.erase(std::find(deleteFields.begin(), deleteFields.end(), delField));
      }
    }

    try {
      client.hdel(key, deleteFields, [&result](cpp_redis::reply &reply) {
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
