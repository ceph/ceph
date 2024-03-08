#include "d4n_datacache.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

/* Base metadata and data fields should remain consistent */
std::vector<std::string> baseFields {
  "mtime",
  "object_size",
  "accounted_size",
  "epoch",
  "version_id",
  "source_zone_short_id",
  "bucket_count",
  "bucket_size",
  "user_quota.max_size",
  "user_quota.max_objects",
  "max_buckets",
  "data"};

std::vector< std::pair<std::string, std::string> > RGWD4NCache::buildObject(rgw::sal::Attrs* binary) {
  std::vector< std::pair<std::string, std::string> > values;
  rgw::sal::Attrs::iterator attrs;
 
  /* Convert to vector */
  if (binary != NULL) {
    for (attrs = binary->begin(); attrs != binary->end(); ++attrs) {
      values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
    }
  } 

  return values; 
}

int RGWD4NCache::findClient(cpp_redis::client *client) { 
  if (client->is_connected())
    return 0;

  if (host == "" || port == 0) { 
    dout(10) << "RGW D4N Cache: D4N cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(host, port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

int RGWD4NCache::existKey(std::string key) { 
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

int RGWD4NCache::setObject(std::string oid, rgw::sal::Attrs* attrs) {
  /* Creating the index based on oid */
  std::string key = "rgw-object:" + oid + ":cache";
  std::string result;

  if (!client.is_connected()) {
    findClient(&client);
  }

  /* Every set will be treated as new */
  try {
    std::vector< std::pair<std::string, std::string> > redisObject = buildObject(attrs);
      
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
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RGWD4NCache::getObject(std::string oid, 
    rgw::sal::Attrs* newAttrs, 
    std::vector< std::pair<std::string, std::string> >* newMetadata) 
{
  std::string result;
  std::string key = "rgw-object:" + oid + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    int field_exist = -1;
    
    rgw::sal::Attrs::iterator it;
    std::vector< std::pair<std::string, std::string> > redisObject;
    std::vector<std::string> getFields;

    /* Retrieve existing fields from cache */
    try {
      client.hgetall(key, [&getFields](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      getFields.push_back(arr[i].as_string());
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    /* Only data exists */
    if (getFields.size() == 1 && getFields[0] == "data")
      return 0;

    /* Ensure all metadata, attributes, and data has been set */
    for (const auto& field : baseFields) { 
      auto it = std::find_if(getFields.begin(), getFields.end(),
        [&](const auto& comp) { return comp == field; });

      if (it != getFields.end()) {
	int index = std::distance(getFields.begin(), it);
	getFields.erase(getFields.begin() + index);
      } else {
        return -1;
      }
    }

    /* Get attributes from cache */
    try {
      client.hmget(key, getFields, [&field_exist, &newAttrs, &getFields](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    field_exist = 0;

            for (long unsigned int i = 0; i < getFields.size(); ++i) {
	      std::string tmp = arr[i].as_string();
              buffer::list bl;
	      bl.append(tmp);
	      newAttrs->insert({getFields[i], bl});
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
    
    if (field_exist == 0) {
      field_exist = -1;

      getFields.clear();
      getFields.insert(getFields.begin(), baseFields.begin(), baseFields.end());
      getFields.pop_back(); /* Do not query for data field */

      /* Get metadata from cache */
      try {
	client.hmget(key, getFields, [&field_exist, &newMetadata, &getFields](cpp_redis::reply &reply) {
	  if (reply.is_array()) {
	    auto arr = reply.as_array();

	    if (!arr[0].is_null()) {
	      field_exist = 0;

	      for (long unsigned int i = 0; i < getFields.size(); ++i) {
		newMetadata->push_back({getFields[i], arr[i].as_string()});
	      }
	    }
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
	return -1;
      }
    } else {
      return -1;
    }
  } else {
    dout(20) << "RGW D4N Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RGWD4NCache::copyObject(std::string original_oid, std::string copy_oid, rgw::sal::Attrs* attrs) {
  std::string result;
  std::vector< std::pair<std::string, std::string> > redisObject;
  std::string key = "rgw-object:" + original_oid + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }

  /* Read values from cache */
  if (existKey(key)) {
    try {
      client.hgetall(key, [&redisObject](cpp_redis::reply &reply) {
        if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
            for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      redisObject.push_back({arr[i].as_string(), arr[i + 1].as_string()});
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2; 
  }

  /* Build copy with updated values */
  if (!redisObject.empty()) {
    rgw::sal::Attrs::iterator attr;
    
    for (attr = attrs->begin(); attr != attrs->end(); ++attr) {
      auto it = std::find_if(redisObject.begin(), redisObject.end(),
        [&](const auto& pair) { return pair.first == attr->first; });

      if (it != redisObject.end()) {
	int index = std::distance(redisObject.begin(), it);
	redisObject[index] = {attr->first, attr->second.to_str()};
      } else {
	redisObject.push_back(std::make_pair(attr->first, attr->second.to_str()));
      }
    }
  } else {
    return -1;
  }

  /* Set copy with new values */
  key = "rgw-object:" + copy_oid + ":cache";

  try {
    client.hmset(key, redisObject, [&result](cpp_redis::reply &reply) {
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
  } else {
    dout(20) << "RGW D4N Cache: Object is not in cache." << dendl;
    return -2;
  }
}

int RGWD4NCache::updateAttr(std::string oid, rgw::sal::Attrs* attr) {
  std::string result;
  std::string key = "rgw-object:" + oid + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }
  
  if (existKey(key)) { 
    try {
      std::vector< std::pair<std::string, std::string> > redisObject;
      auto it = attr->begin();
      redisObject.push_back({it->first, it->second.to_str()});

      client.hmset(key, redisObject, [&result](cpp_redis::reply &reply) {
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
  } else {
    return -2;
  }

  return 0;
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

int RGWD4NCache::appendData(std::string oid, buffer::list& data) {
  std::string result;
  std::string value = "";
  std::string key = "rgw-object:" + oid + ":cache";

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    try {
      client.hget(key, "data", [&value](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  value = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  }

  try {
    /* Append to existing value or set as new value */
    std::string temp = value + data.to_str();
    std::vector< std::pair<std::string, std::string> > field;
    field.push_back({"data", temp});

    client.hmset(key, field, [&result](cpp_redis::reply &reply) {
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

int RGWD4NCache::deleteData(std::string oid) {
  int result = 0;
  std::string key = "rgw-object:" + oid + ":cache";
  std::vector<std::string> deleteField;
  deleteField.push_back("data");

  if (!client.is_connected()) {
    findClient(&client);
  }

  if (existKey(key)) {
    int field_exist = -1;

    try {
      client.hget(key, "data", [&field_exist](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  field_exist = 0;
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    if (field_exist == 0) {
      try {
	client.hdel(key, deleteField, [&result](cpp_redis::reply &reply) {
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
      return -1;
    }
  } else {
    return 0; /* No delete was necessary */
  }
}
