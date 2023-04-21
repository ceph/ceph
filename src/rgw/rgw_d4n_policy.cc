#include "rgw_d4n_policy.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

int RGWD4NPolicy::find_client(cpp_redis::client *client) {
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

int RGWD4NPolicy::exist_key(std::string key) {
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

int RGWD4NPolicy::update_gw(CacheBlock* block) {
  std::string result;
  std::string key = "rgw-object:" + block->cacheObj.objName + ":directory"; // should have "build_index" method -Sam
  int globalWeight;

  if (!client.is_connected()) {
    find_client(&client);
  }

  if (exist_key(key)) {
    try {
      client.hget(key, "globalWeight", [&globalWeight](cpp_redis::reply &reply) {
        if (!reply.is_null()) {
          globalWeight = reply.as_integer();
        }
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -2;
    }
  } else {
    return -1;
  }

  try {
    int age;
    // get local cache age
	
    /* Update global weight */
    globalWeight += age;
    
    client.hset(key, "globalWeight", std::to_string(globalWeight), [&result](cpp_redis::reply &reply) {
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

int RGWD4NPolicy::gwf_get_block(CacheBlock* block) {
  int result = -1;
  std::string response;
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
      if (location == (host + ":" + std::to_string(port))) {
	  // get local weight from cache
	  // add age to local weight
	  // update in cache
      } else {
	int freeSpace = 0; // find free space

	while (freeSpace < block->size) {
	  freeSpace += gwf_eviction();
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
	  
	  try {
	    client.hset(key, "globalWeight", std::to_string(globalWeight), [&response](cpp_redis::reply& reply) {
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

int RGWD4NPolicy::gwf_eviction() {
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

bool RGWD4NPolicy::should_cache(int objSize, int minSize) {
  if (objSize < minSize)
    return true;
  else
    return false;
}

bool RGWD4NPolicy::should_cache(std::string uploadType) {
  if (uploadType == "PUT")
    return true;
  else if (uploadType == "MULTIPART")
    return false;
  else /* Should not reach here */
    return false;
}
