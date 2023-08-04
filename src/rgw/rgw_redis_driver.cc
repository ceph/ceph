#include "rgw_redis_driver.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { 

/* Base metadata and data fields should remain consistent */
std::vector<std::string> baseFields{
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

std::vector< std::pair<std::string, std::string> > build_attrs(rgw::sal::Attrs* binary) {
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

int RedisDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  if (client.is_connected())
    return 0;

  /*
  if (addr.host == "" || addr.port == 0) {
    dout(10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }*/

  client.connect("127.0.0.1", 6379, nullptr);

  if (!client.is_connected()) 
    return ECONNREFUSED;

  return 0;
}

int RedisDriver::insert_entry(const DoutPrefixProvider* dpp, std::string key, off_t offset, uint64_t len) 
{
  auto ret = entries.emplace(key, Entry(key, offset, len));
  return ret.second;
}

std::optional<Entry> RedisDriver::get_entry(const DoutPrefixProvider* dpp, std::string key) 
{
  auto iter = entries.find(key);

  if (iter != entries.end()) {
    return iter->second;
  }

  return std::nullopt;
}

int RedisDriver::remove_entry(const DoutPrefixProvider* dpp, std::string key) 
{
  return entries.erase(key);
}

int RedisDriver::add_partition_info(Partition& info)
{
  std::string key = info.name + info.type;
  auto ret = partitions.emplace(key, info);

  return ret.second;
}

int RedisDriver::remove_partition_info(Partition& info)
{
  std::string key = info.name + info.type;
  return partitions.erase(key);
}

auto RedisDriver::redis_exec(boost::system::error_code ec, boost::redis::request req, boost::redis::response<std::string>& resp, optional_yield y) {
  assert(y); 
  auto yield = y.get_yield_context();

  return conn.async_exec(req, resp, yield[ec]);
}

bool RedisDriver::key_exists(const DoutPrefixProvider* dpp, const std::string& key) 
{
  int result;
  std::string entry = partition_info.location + key;
  std::vector<std::string> keys;
  keys.push_back(key);

  if (!client.is_connected()) 
    return ECONNREFUSED;

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

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) {
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  /* Every set will be treated as new */
  try {
    /* Set data field */
    int result; 

    client.hset(entryName, "data", bl.to_str(), [&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    if (result != 0) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}
*/

std::optional<Partition> RedisDriver::get_partition_info(const DoutPrefixProvider* dpp, const std::string& name, const std::string& type)
{
  std::string key = name + type;

  auto iter = partitions.find(key);
  if (iter != partitions.end())
    return iter->second;

  return std::nullopt;
}

std::vector<Partition> RedisDriver::list_partitions(const DoutPrefixProvider* dpp)
{
  std::vector<Partition> partitions_v;

  for (auto& it : partitions)
    partitions_v.emplace_back(it.second);

  return partitions_v;
}

/* Currently an attribute but will also be part of the Entry metadata once consistency is guaranteed -Sam
int RedisDriver::update_local_weight(const DoutPrefixProvider* dpp, std::string key, int localWeight) 
{
  auto iter = entries.find(key);

  if (iter != entries.end()) {
    iter->second.localWeight = localWeight;
    return 0;
  }

  return -1;
}
*/

int RedisDriver::initialize(CephContext* cct, const DoutPrefixProvider* dpp) 
{
  namespace net = boost::asio;
  using boost::redis::config;

  this->cct = cct;

  if (partition_info.location.back() != '/') {
    partition_info.location += "/";
  }

  // remove
  addr.host = cct->_conf->rgw_d4n_host; // change later -Sam
  addr.port = cct->_conf->rgw_d4n_port;

  if (addr.host == "" || addr.port == 0) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  config cfg;
  cfg.addr.host = addr.host;
  cfg.addr.port = std::to_string(addr.port);
  
  conn.async_run(cfg, {}, net::detached);

/*  client.connect("127.0.0.1", 6379, nullptr);

  if (!client.is_connected()) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Could not connect to redis cache endpoint." << dendl;
    return ECONNREFUSED;
  }*/

  return 0;
}

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  try {
    /* Set attribute fields */
    std::string result; 
    std::vector< std::pair<std::string, std::string> > redisAttrs = build_attrs(&attrs);

    client.hmset(entryName, redisAttrs, [&result](cpp_redis::reply &reply) {
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

int RedisDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  
  if (!client.is_connected()) 
    return ECONNREFUSED;
    
  if (key_exists(dpp, entryName)) {
    rgw::sal::Attrs::iterator it;
    std::vector< std::pair<std::string, std::string> > redisAttrs;
    std::vector<std::string> getFields;

    /* Retrieve existing values from cache */
    try {
      client.hgetall(entryName, [&bl, &attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() == "data")
                bl.append(arr[i + 1].as_string());
	      else {
	        buffer::list temp;
		temp.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), temp});
		temp.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    dout(20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) 
{
  std::string value;
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, entryName)) {
    try {
      client.hget(entryName, "data", [&value](cpp_redis::reply &reply) {
        if (!reply.is_null()) {
          value = reply.as_string();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -2;
    }
  }

  try {
    /* Append to existing value or set as new value */
    std::string temp = value + bl_data.to_str();
    std::vector< std::pair<std::string, std::string> > field;
    field.push_back({"data", temp});

    client.hmset(entryName, field, [&result](cpp_redis::reply &reply) {
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

  return 0;
}

int RedisDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, entryName)) {
    try {
    client.hdel(entryName, deleteField, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
    return -2;
    }
  } else {
    return 0; /* No delete was necessary */
  }

  return result - 1;
}

int RedisDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, key)) {
    try {
      client.hgetall(entry, [&attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) { 
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() != "data") {
	        buffer::list bl_value;
		bl_value.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), bl_value});
		bl_value.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  if (attrs.empty())
    return -1;
      
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    /* Every attr set will be treated as new */
    try {
      std::string result;
      auto redisAttrs = build_attrs(&attrs);
	
      client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
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
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    try {
      std::string result;
      auto redisAttrs = build_attrs(&attrs);

      client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
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
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  if (key_exists(dpp, key)) {
    std::vector<std::string> getFields;

    /* Retrieve existing values from cache */
    try {
      client.hgetall(entryName, [&getFields](cpp_redis::reply &reply) {
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

    /* Ensure all metadata, attributes, and data has been set */
    for (const auto& field : baseFields) {
      auto it = std::find_if(getFields.begin(), getFields.end(),
	[&](const auto& comp) { return comp == field; });

      if (it == getFields.end()) {
	return -1;
      }
    }

    getFields.pop_back(); /* Do not query for data field */
    
    /* Get attributes from cache */
    try {
      client.hmget(entryName, getFields, [&exists, &attrs, &getFields](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();

	  if (!arr[0].is_null()) {
	    exists = 0;

	    for (long unsigned int i = 0; i < getFields.size(); ++i) {
	      std::string tmp = arr[i].as_string();
	      buffer::list bl;
	      bl.append(tmp);
	      attrs.insert({getFields[i], bl});
	    }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      exit(-1);
    }

    if (exists < 0) {
      dout(20) << "RGW Redis Cache: Object was not retrievable." << dendl;
      return -2;
    }
  }

  return 0;
}

int RedisDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) {
  std::string result;
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, entryName)) {
    try {
      std::vector< std::pair<std::string, std::string> > redisAttrs;
      for (const auto& it : attrs) {
        redisAttrs.push_back({it.first, it.second.to_str()});
      }

      client.hmset(entryName, redisAttrs, [&result](cpp_redis::reply &reply) {
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
  } else {
    return -2;
  }

  return 0;
}

int RedisDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) {
  int result = 0;
  std::string entryName = "rgw-object:" + key + ":cache";

  if (!client.is_connected()) 
    return ECONNREFUSED;

  if (key_exists(dpp, entryName)) {
    std::vector<std::string> getFields;

    /* Retrieve existing values from cache */
    try {
      client.hgetall(entryName, [&getFields](cpp_redis::reply &reply) {
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

    std::vector< std::pair<std::string, std::string> > redisAttrs = build_attrs(&del_attrs);
    std::vector<std::string> redisFields;

    std::transform(begin(redisAttrs), end(redisAttrs), std::back_inserter(redisFields),
                          [](auto const& pair) { return pair.first; });

    /* Only delete attributes that have been stored */
    for (const auto& it : redisFields) {
      if (std::find(getFields.begin(), getFields.end(), it) == getFields.end()) {
        redisFields.erase(std::find(redisFields.begin(), redisFields.end(), it)); // Should I return here instead? -Sam
      }
    }

    try {
      client.hdel(entryName, redisFields, [&result](cpp_redis::reply &reply) {
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

  dout(20) << "RGW Redis Cache: Object is not in cache." << dendl;
  return -2;
}

std::string RedisDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  std::string attrValue;

  if (!client.is_connected()) 
    return {};

  if (key_exists(dpp, entryName)) {
    std::string getValue;

    /* Ensure field was set */
    try {
      client.hexists(entryName, attr_name, [&exists](cpp_redis::reply& reply) {
	if (!reply.is_null()) {
	  exists = reply.as_integer();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return {};
    }
    
    if (!exists) {
      dout(20) << "RGW Redis Cache: Attribute was not set." << dendl;
      return {};
    }

    /* Retrieve existing value from cache */
    try {
      client.hget(entryName, attr_name, [&exists, &attrValue](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  exists = 0;
	  attrValue = reply.as_string();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return {};
    }

    if (exists < 0) {
      dout(20) << "RGW Redis Cache: Object was not retrievable." << dendl;
      return {};
    }
  }

  return attrValue;
}

int RedisDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attrVal, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  int result = 0;
    
  if (!client.is_connected()) 
    return ECONNREFUSED;
    
  /* Every set will be treated as new */
  try {
    client.hset(entryName, attr_name, attrVal, [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

} // namespace rgw::sal
