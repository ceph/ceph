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

int LFUDAPolicy::get_block(CacheBlock* block/*, CacheDriver* cacheNode*/) {
  int result = -1;
  std::string key = "rgw-object:" + block->cacheObj.objName + ":cache";

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    /* Check if data is cached */
    try {
      client.hexists(key, "data", [&result](cpp_redis::reply& reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    if (result) {
      std::string location;

      try {
	client.hget(key, "location", [&location](cpp_redis::reply& reply) {
	  if (!reply.is_null()) {
	    location = reply.as_string();
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
	return -1;
      }

      /* Check local cache */
      if (location == (get_addr().host + ":" + std::to_string(get_addr().port))) {
	  // get local weight from cache
	  // add age to local weight
	  // update in cache
      } else {
	uint64_t freeSpace = 0; // find free space

	while (freeSpace < block->size) {
	  freeSpace += eviction();
	}

	/* Check remote cache */
	if (location.length() > 0) {
	  int globalWeight;

	  try {
	    client.hget(key, "globalWeight", [&globalWeight](cpp_redis::reply& reply) {
	      if (!reply.is_null()) {
		globalWeight = reply.as_integer();
	      }
	    });

	    client.sync_commit(std::chrono::milliseconds(1000));
	  } catch(std::exception &e) {
	    return -1;
	  }

	  // add age to global weight
	  
	  result = 0;
	  try {
	    client.hset(key, "globalWeight", std::to_string(globalWeight), [&result](cpp_redis::reply& reply) {
	      if (!reply.is_null()) {
	        result = reply.as_integer();
	      }
	    }); 

	    client.sync_commit();
	  } catch(std::exception &e) {
	    return -1;
	  }
	  
	  if (result)
	    return -2;
	} else { // make less repetitive -Sam
	  // get local weight from data lake
	  // add age to local weight
	  // update in cache
	}
      }
    }
  } else {
    return -1; 
  } 

  return 0;
}

int LFUDAPolicy::eviction(/*CacheDriver* cacheNode*/) {
  CacheBlock* victim = NULL; // find victim
  int result = -1;
  std::string response;
  std::string key = "rgw-object:" + victim->cacheObj.objName + ":cache";

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    /* Check if data is cached */
    try {
      client.hexists(key, "data", [&result](cpp_redis::reply& reply) {
        if (reply.is_integer()) {
          result = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    if (result) {
      std::string location;

      try {
	client.hget(key, "location", [&location](cpp_redis::reply& reply) {
	  if (!reply.is_null()) {
	    location = reply.as_string();
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));
      } catch(std::exception &e) {
	return -1;
      }

      /* Check if block is not in remote cache */
      if (!location.length()) {
	// get local weight from cache
	int globalWeight;

	try {
	  client.hget(key, "globalWeight", [&globalWeight](cpp_redis::reply& reply) {
	    if (!reply.is_null()) {
	      globalWeight = reply.as_integer();
	    }
	  });

	  client.sync_commit(std::chrono::milliseconds(1000));
	} catch(std::exception &e) {
	  return -1;
	}

	if (globalWeight != 0) {
	  std::vector< std::pair<std::string, std::string> > updatedWeights;
	  // localWeight += globalWeight;
	  globalWeight = 0;

	  updatedWeights.push_back({"globalWeight", std::to_string(globalWeight)});

	  /*
	  try {
	    client.hset(key, [&response](cpp_redis::reply& reply) {
	      if (!reply.is_null()) {
	        response = reply.as_string();
	      }
	    }); 

	    client.sync_commit();
	  } catch(std::exception &e) {
	    return -1;
	  }
	  
	  if (response != "OK")
	    return -2;
	  }
	  */
	}
      }

      // get cache node with minimum average weight
      //int avgWeight = 0; //cache node's avg weight

      /*
      if (localWeight < avgWeight) {
        // push block to remote cache
      }
      */
    }

    //globalWeight += localWeight;
    // remove victim from local cache
    // update age
    // update in cache
  }

  // return victim size
  return 0;
}

int PolicyDriver::set_policy() { // Add "none" option? -Sam
  if (policyName == "lfuda") {
    cachePolicy = new LFUDAPolicy();
    return 0;
  } else
    return -1;
}

int PolicyDriver::delete_policy() {
  delete cachePolicy;
  return 0;
}

} } // namespace rgw::d4n
