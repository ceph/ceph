#include "d4n_policy.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

int CachePolicy::find_client(cpp_redis::client *client) {
  if (client->is_connected())
    return 0;

  if (get_addr().host == "" || get_addr().port == 0) {
    dout(10) << "RGW D4N Cache: D4N cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client->connect(get_addr().host, get_addr().port, nullptr);

  if (!client->is_connected())
    return ECONNREFUSED;

  return 0;
}

int CachePolicy::exist_key(std::string key) {
  int result = -1;
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

int LFUDAPolicy::set_age(int age) {
  int result = -1;

  try {
    client.hset("lfuda", "age", std::to_string(age), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

int LFUDAPolicy::get_age() {
  int ret = 0;
  int age = -1;

  try {
    client.hexists("lfuda", "age", [&ret](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        ret = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  if (!ret) {
    ret = set_age(0); /* Initialize age */
    return ret; // Test return value -Sam
  }

  try {
    client.hget("lfuda", "age", [&age](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        age = std::stoi(reply.as_string());
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return age;
}

int LFUDAPolicy::set_global_weight(std::string key, int weight) {
  int result = -1;

  try {
    client.hset(key, "globalWeight", std::to_string(weight), [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

  return result;
}

int LFUDAPolicy::get_global_weight(std::string key) {
  int weight = -1;

  try {
    client.hget(key, "globalWeight", [&weight](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	weight = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return weight;
}

int LFUDAPolicy::set_min_avg_weight(size_t weight, std::string cacheLocation) {
  int result = -1;

  try {
    client.hset("lfuda", "minAvgWeight:cache", cacheLocation, [&result](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
	result = reply.as_integer();
      }
    }); 

    client.sync_commit();
  } catch(std::exception &e) {
    return -1;
  }

  if (result) {
    result = -1;
    try {
      client.hset("lfuda", "minAvgWeight:weight", std::to_string(weight), [&result](cpp_redis::reply& reply) {
	if (!reply.is_null()) {
	  result = reply.as_integer();
	}
      }); 

      client.sync_commit();
    } catch(std::exception &e) {
      return -1;
    }
  }

  return result;
}

int LFUDAPolicy::get_min_avg_weight() {
  int ret = 0;
  int weight = -1;

  try {
    client.hexists("lfuda", "minAvgWeight:cache", [&ret](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        ret = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  if (!ret) {
    ret = set_min_avg_weight(INT_MAX, "initial"); /* Initialize minimum average weight */
    return ret; // Test return value -Sam
  }

  try {
    client.hget("lfuda", "minAvgWeight:weight", [&weight](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        weight = std::stoi(reply.as_string());
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return weight;
}

int LFUDAPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, CacheDriver* cacheNode) {
  std::string key = "rgw-object:" + block->cacheObj.objName + ":directory";
  int localWeight = std::stoi(cacheNode->get_attr(dpp, block->cacheObj.objName, "localWeight")); // change to block name eventually -Sam

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    int age = get_age();

    if (cacheNode->key_exists(dpp, block->cacheObj.objName)) { /* Local copy */
      localWeight += age;
    } else {
      uint64_t freeSpace = cacheNode->partition_info.get_free_space();

      while (freeSpace < block->size) {
	int newSpace = eviction(dpp, cacheNode);
	
	if (newSpace > 0)
	  freeSpace += newSpace;
      }

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

      if (hosts.length() > 0) { /* Remote copy */
	int globalWeight = get_global_weight(key);
	globalWeight += age;
	
	if (set_global_weight(key, globalWeight))
	  return -2;
      } else { /* No remote copy */
	localWeight += age;
      }
    }
  } else {
    return -1; 
  } 

  int ret = cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(localWeight));
  return ret;
}

uint64_t LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, CacheDriver* cacheNode) {
  CacheBlock* victim = NULL; // find victim
  std::string key = "rgw-object:" + victim->cacheObj.objName + ":directory";
  std::string hosts;
  int globalWeight = get_global_weight(key);
  int localWeight = std::stoi(cacheNode->get_attr(dpp, victim->cacheObj.objName, "localWeight")); // change to block name eventually -Sam
  int avgWeight;

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

  if (!hosts.length()) { /* Last copy */
    if (globalWeight > 0) {
      localWeight += globalWeight;
      int ret = cacheNode->set_attr(dpp, victim->cacheObj.objName, "localWeight", std::to_string(localWeight));

      if (!ret) // check return value -Sam
        ret = set_global_weight(key, 0);
      else
        return -1;

      if (ret)
	return -2;
    }

    avgWeight = get_min_avg_weight();

    if (avgWeight < 0)
      return -2;

    if (localWeight < avgWeight) {
      // push block to remote cache
    }
  }

  globalWeight += localWeight;
  int ret = cacheNode->delete_data(dpp, victim->cacheObj.objName);

  if (!ret)
    ret = set_min_avg_weight(avgWeight - (localWeight/cacheNode->get_num_entries(dpp)), ""/*local cache location*/); // Where else must this be set? -Sam
  else
    return -1;

  if (ret)
    return -2;

  int age = get_age();
  age = std::max(localWeight, age);
  ret = set_age(age);
  
  if (ret)
    return -2;

  return victim->size;
}

int PolicyDriver::init() { // Add "none" option? -Sam
  CacheDriver::Partition partition_info;
  cacheDriver = new RedisDriver(partition_info, "127.0.0.1", 6379); // hardcoded for now

  if (policyName == "lfuda") {
    cachePolicy = new LFUDAPolicy();
    return 0;
  } else
    return -1;
}

} } // namespace rgw::d4n
