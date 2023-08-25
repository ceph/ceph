#include <boost/asio/consign.hpp>
#include "common/async/blocked_completion.h"
#include "d4n_directory.h"
#include <time.h>

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;
  boost::redis::request req;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, Response& resp)
  {
    conn->async_exec(req, resp, boost::asio::consign(std::move(handler), conn));
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn), req}, token, resp);
}

template <typename T>
void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
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

int ObjectDirectory::set(CacheObj* object) {
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

int ObjectDirectory::get(CacheObj* object) {
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

int ObjectDirectory::del(CacheObj* object) {
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

int BlockDirectory::set(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);
    
  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  std::string endpoint = cct->_conf->rgw_d4n_host + ":" + std::to_string(cct->_conf->rgw_d4n_port);
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
    request req;
    req.push_range("HMSET", key, redisValues);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (std::get<0>(resp).value() != "OK" || (bool)ec) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int BlockDirectory::get(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);

  if (exist_key(key, y)) {
    std::vector<std::string> fields;

    fields.push_back("version");
    fields.push_back("size");
    fields.push_back("globalWeight");
    fields.push_back("blockHosts");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("objHosts");

    try {
      boost::system::error_code ec;
      request req;
      req.push_range("HMGET", key, fields);
      response< std::vector<std::string> > resp;

      redis_exec(conn, ec, req, resp, y);

      if (!std::get<0>(resp).value().size() || (bool)ec) {
	return -1;
      }

      block->version = boost::lexical_cast<uint64_t>(std::get<0>(resp).value()[0]);
      block->size = boost::lexical_cast<uint64_t>(std::get<0>(resp).value()[1]);
      block->globalWeight = boost::lexical_cast<int>(std::get<0>(resp).value()[2]);

      {
        std::stringstream ss(boost::lexical_cast<std::string>(std::get<0>(resp).value()[3]));

	while (!ss.eof()) {
          std::string host;
	  std::getline(ss, host, '_');
	  block->hostsList.push_back(host);
	}
      }

      block->cacheObj.objName = std::get<0>(resp).value()[4];
      block->cacheObj.bucketName = std::get<0>(resp).value()[5];
      block->cacheObj.creationTime = boost::lexical_cast<time_t>(std::get<0>(resp).value()[6]);
      block->cacheObj.dirty = boost::lexical_cast<bool>(std::get<0>(resp).value()[7]);

      {
        std::stringstream ss(boost::lexical_cast<std::string>(std::get<0>(resp).value()[8]));

	while (!ss.eof()) {
          std::string host;
	  std::getline(ss, host, '_');
	  block->hostsList.push_back(host);
	}
      }
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }

  return 0;
}

int BlockDirectory::copy(CacheBlock* block, std::string copyName, std::string copyBucketName, optional_yield y) {
  std::string key = build_index(block);
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName } };
  std::string copyKey = build_index(&copyBlock);

  if (exist_key(key, y)) {
    try {
      response<int> resp;
     
      {
	boost::system::error_code ec;
	request req;
	req.push("COPY", key, copyKey);

	redis_exec(conn, ec, req, resp, y);

	if ((bool)ec) {
	  return -1;
	}
      }

      {
	boost::system::error_code ec;
	request req;
	req.push("HMSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
	response<std::string> res;

	redis_exec(conn, ec, req, res, y);

        if (std::get<0>(res).value() != "OK" || (bool)ec) {
          return -1;
        }
      }

      return std::get<0>(resp).value() - 1; 
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }
}

int BlockDirectory::del(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);

  if (exist_key(key, y)) {
    try {
      boost::system::error_code ec;
      request req;
      req.push("DEL", key);
      response<int> resp;

      redis_exec(conn, ec, req, resp, y);

      if ((bool)ec)
        return -1;

      return std::get<0>(resp).value() - 1; 
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

int BlockDirectory::update_field(CacheBlock* block, std::string field, std::string value, optional_yield y) {
  std::string key = build_index(block);

  if (exist_key(key, y)) {
    try {
      /* Ensure field exists */
      {
	boost::system::error_code ec;
	request req;
	req.push("HEXISTS", key, field);
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value() || (bool)ec)
	  return -1;
      }

      if (field == "blockHosts" || field == "objHosts") {
	/* Append rather than overwrite */
	boost::system::error_code ec;
	request req;
	req.push("HGET", key, field);
	response<std::string> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value().size() || (bool)ec)
	  return -1;

	std::get<0>(resp).value() += "_";
	std::get<0>(resp).value() += value;
	value = std::get<0>(resp).value();
      }

      {
	boost::system::error_code ec;
	request req;
	req.push_range("HSET", key, std::map<std::string, std::string>{{field, value}});
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if ((bool)ec) {
	  return -1;
	}

	return std::get<0>(resp).value(); /* Zero fields added since it is an update of an existing field */ 
      }
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }
}

void BlockDirectory::shutdown()
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
}

} } // namespace rgw::d4n
