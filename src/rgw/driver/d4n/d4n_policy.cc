#include "d4n_policy.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

int CachePolicy::find_client(const DoutPrefixProvider* dpp, cpp_redis::client* client) {
  if (client->is_connected())
    return 0;

  if (get_addr().host == "" || get_addr().port == 0) {
    ldpp_dout(dpp, 10) << "RGW D4N Policy: D4N policy endpoint was not configured correctly" << dendl;
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
  int result = 0;

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

  return result - 1;
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

    if (!ret) {
      return 0; /* Success */
    } else {
      return -1;
    };
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
  int result = 0;

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

  return result - 1;
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

int LFUDAPolicy::set_min_avg_weight(int weight, std::string cacheLocation) {
  int result = 0;

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

  if (result == 1) {
    result = 0;
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

  return result - 1;
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
    ret = set_min_avg_weight(INT_MAX, ""/* local cache location or keep empty? */); /* Initialize minimum average weight */

    if (!ret) {
      return INT_MAX; /* Success */
    } else {
      return -1;
    };
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

CacheBlock LFUDAPolicy::find_victim(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode) {
  std::vector<rgw::cache::Entry> entries = cacheNode->list_entries(dpp);
  std::string victimName;
  int minWeight = INT_MAX;

  for (auto it = entries.begin(); it != entries.end(); ++it) {
    std::string localWeightStr = cacheNode->get_attr(dpp, it->key, "localWeight"); // should represent block -Sam

    if (!std::stoi(localWeightStr)) { // maybe do this in some sort of initialization procedure instead of here? -Sam
      /* Local weight hasn't been set */
      int ret = cacheNode->set_attr(dpp, it->key, "localWeight", std::to_string(get_age())); 

      if (ret < 0)
	return {};
    } else if (std::stoi(localWeightStr) < minWeight) {
      minWeight = std::stoi(localWeightStr);
      victimName = it->key;
    }
  }

  /* Get victim cache block */
  CacheBlock victimBlock;
  victimBlock.cacheObj.objName = victimName;
  BlockDirectory blockDir;
  blockDir.init(cct);

  int ret = blockDir.get_value(&victimBlock);

  if (ret < 0)
    return {};

  return victimBlock;
}

int LFUDAPolicy::get_block(const DoutPrefixProvider* dpp, CacheBlock* block, rgw::cache::CacheDriver* cacheNode) {
/*  std::string key = "rgw-object:" + block->cacheObj.objName + ":directory";
  std::string localWeightStr = cacheNode->get_attr(dpp, block->cacheObj.objName, "localWeight"); // change to block name eventually -Sam
  int localWeight = -1;

  if (!client.is_connected())
    find_client(dpp, &client);

  if (localWeightStr.empty()) { // figure out where to set local weight -Sam
    int ret = cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(get_age())); 
    localWeight = get_age();

    if (ret < 0)
      return -1;
  } else {
    localWeight = std::stoi(localWeightStr);
  }

  int age = get_age();

  if (cacheNode->key_exists(dpp, block->cacheObj.objName)) { 
    localWeight += age;
  } else {
    std::string hosts;
    uint64_t freeSpace = cacheNode->get_free_space(dpp);

    while (freeSpace < block->size)
      freeSpace += eviction(dpp, cacheNode);

    if (exist_key(key)) {
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
    } else {
      return -2; 
    }

    // should not hold local cache IP if in this else statement -Sam
    if (hosts.length() > 0) {
      int globalWeight = get_global_weight(key);
      globalWeight += age;
      
      if (set_global_weight(key, globalWeight))
	return -1;
    } else { 
      // do I need to add the block to the local cache here? -Sam
      // update hosts list for block as well? check read workflow -Sam
      localWeight += age;
      return cacheNode->set_attr(dpp, block->cacheObj.objName, "localWeight", std::to_string(localWeight));
    }
  }*/ 
}

uint64_t LFUDAPolicy::eviction(const DoutPrefixProvider* dpp, rgw::cache::CacheDriver* cacheNode) {
  CacheBlock victim = find_victim(dpp, cacheNode);

  if (victim.cacheObj.objName.empty()) {
    ldpp_dout(dpp, 10) << "RGW D4N Policy: Could not find victim block" << dendl;
    return -1;
  }

  std::string key = "rgw-object:" + victim.cacheObj.objName + ":directory";
  std::string hosts;
  int globalWeight = get_global_weight(key);
  int localWeight = std::stoi(cacheNode->get_attr(dpp, victim.cacheObj.objName, "localWeight")); // change to block name eventually -Sam
  int avgWeight = get_min_avg_weight();

  if (exist_key(key)) {
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
  } else {
    return -2; 
  }

  if (hosts.empty()) { /* Last copy */
    if (globalWeight > 0) {
      localWeight += globalWeight;
      int ret = cacheNode->set_attr(dpp, victim.cacheObj.objName, "localWeight", std::to_string(localWeight));

      if (!ret)
        ret = set_global_weight(key, 0);
      else
        return -1;

      if (ret)
	return -1;
    }

    if (avgWeight < 0)
      return -1;

    if (localWeight > avgWeight) {
      // push block to remote cache
    }
  }

  globalWeight += localWeight;

  if (set_global_weight(key, globalWeight))
    return -2;

  ldpp_dout(dpp, 10) << "RGW D4N Policy: Block " << victim.cacheObj.objName << " has been evicted." << dendl;
  int ret = cacheNode->delete_data(dpp, victim.cacheObj.objName);

  if (!ret) {
    ret = set_min_avg_weight(avgWeight - (localWeight/cacheNode->get_num_entries(dpp)), ""/*local cache location*/); // Where else must this be set? -Sam

    if (!ret) {
      int age = get_age();
      age = std::max(localWeight, age);
      ret = set_age(age);
      
      if (ret)
	return -1;
    } else {
      return -1;
    }
  } else {
    return -1;
  }

  return victim.size;
}

int PolicyDriver::init() {
  rgw::cache::Partition partition_info;
  cacheDriver = new rgw::cache::RedisDriver(partition_info, "127.0.0.1", 6379); // hardcoded for now -Sam

  if (policyName == "lfuda") {
    cachePolicy = new LFUDAPolicy();
    return 0;
  } else
    return -1;
}

} } // namespace rgw::d4n
