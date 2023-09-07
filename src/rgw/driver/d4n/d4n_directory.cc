#include <boost/asio/consign.hpp>
#include "common/async/blocked_completion.h"
#include "d4n_directory.h"

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

std::string ObjectDirectory::build_index(CacheObj* object) {
  return object->bucketName + "_" + object->objName;
}

int ObjectDirectory::exist_key(CacheObj* object, optional_yield y) {
  std::string key = build_index(object);
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

void ObjectDirectory::shutdown() // generalize -Sam
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
}

int ObjectDirectory::set(CacheObj* object, optional_yield y) {
  std::string key = build_index(object);
    
  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  std::string endpoint = cct->_conf->rgw_d4n_host + ":" + std::to_string(cct->_conf->rgw_d4n_port);
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("objName");
  redisValues.push_back(object->objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(object->bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(std::to_string(object->creationTime)); 
  redisValues.push_back("dirty");
  redisValues.push_back(std::to_string(object->dirty));
  redisValues.push_back("objHosts");
  redisValues.push_back(endpoint); 

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

int ObjectDirectory::get(CacheObj* object, optional_yield y) {
  std::string key = build_index(object);

  if (exist_key(object, y)) {
    std::vector<std::string> fields;

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

      object->objName = std::get<0>(resp).value()[0];
      object->bucketName = std::get<0>(resp).value()[1];
      object->creationTime = boost::lexical_cast<time_t>(std::get<0>(resp).value()[2]);
      object->dirty = boost::lexical_cast<bool>(std::get<0>(resp).value()[3]);

      {
        std::stringstream ss(boost::lexical_cast<std::string>(std::get<0>(resp).value()[4]));

	while (!ss.eof()) {
          std::string host;
	  std::getline(ss, host, '_');
	  object->hostsList.push_back(host);
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

int ObjectDirectory::copy(CacheObj* object, std::string copyName, std::string copyBucketName, optional_yield y) {
  std::string key = build_index(object);
  auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };
  std::string copyKey = build_index(&copyObj);

  if (exist_key(object, y)) {
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

int ObjectDirectory::del(CacheObj* object, optional_yield y) {
  std::string key = build_index(object);

  if (exist_key(object, y)) {
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

std::string BlockDirectory::build_index(CacheBlock* block) {
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID);
}

int BlockDirectory::exist_key(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);
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

void BlockDirectory::shutdown()
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
}

int BlockDirectory::set(CacheBlock* block, optional_yield y) {
  std::string key = build_index(block);
    
  /* Every set will be treated as new */
  std::string endpoint;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("blockID");
  redisValues.push_back(std::to_string(block->blockID));
  redisValues.push_back("version");
  redisValues.push_back(block->version);
  redisValues.push_back("size");
  redisValues.push_back(std::to_string(block->size));
  redisValues.push_back("globalWeight");
  redisValues.push_back(std::to_string(block->globalWeight));
  redisValues.push_back("blockHosts");
  
  for (auto const& host : block->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  endpoint.pop_back();
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
  
  endpoint.clear();
  for (auto const& host : block->cacheObj.hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  endpoint.pop_back();
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

  if (exist_key(block, y)) {
    std::vector<std::string> fields;

    fields.push_back("blockID");
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

      block->blockID = boost::lexical_cast<uint64_t>(std::get<0>(resp).value()[0]);
      block->version = std::get<0>(resp).value()[1];
      block->size = boost::lexical_cast<uint64_t>(std::get<0>(resp).value()[2]);
      block->globalWeight = boost::lexical_cast<int>(std::get<0>(resp).value()[3]);

      {
        std::stringstream ss(boost::lexical_cast<std::string>(std::get<0>(resp).value()[4]));
	block->hostsList.clear();

	while (!ss.eof()) {
          std::string host;
	  std::getline(ss, host, '_');
	  block->hostsList.push_back(host);
	}
      }

      block->cacheObj.objName = std::get<0>(resp).value()[5];
      block->cacheObj.bucketName = std::get<0>(resp).value()[6];
      block->cacheObj.creationTime = boost::lexical_cast<time_t>(std::get<0>(resp).value()[7]);
      block->cacheObj.dirty = boost::lexical_cast<bool>(std::get<0>(resp).value()[8]);

      {
        std::stringstream ss(boost::lexical_cast<std::string>(std::get<0>(resp).value()[9]));
	block->cacheObj.hostsList.clear();

	while (!ss.eof()) {
          std::string host;
	  std::getline(ss, host, '_');
	  block->cacheObj.hostsList.push_back(host);
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
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  if (exist_key(block, y)) {
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

  if (exist_key(block, y)) {
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

  if (exist_key(block, y)) {
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

      if (field == "blockHosts") { // Need one for object hosts? -Sam
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

int BlockDirectory::remove_host(CacheBlock* block, std::string delValue, optional_yield y) {
  std::string key = build_index(block);

  if (exist_key(block, y)) {
    try {
      /* Ensure field exists */
      {
	boost::system::error_code ec;
	request req;
	req.push("HEXISTS", key, "blockHosts");
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value() || (bool)ec)
	  return -1;
      }

      {
	boost::system::error_code ec;
	request req;
	req.push("HGET", key, "blockHosts");
	response<std::string> resp;

	redis_exec(conn, ec, req, resp, y);

	if (!std::get<0>(resp).value().size() || (bool)ec)
	  return -1;

	if (std::get<0>(resp).value().find("_") == std::string::npos) /* Last host, delete entirely */
          return del(block, y);

        std::string result = std::get<0>(resp).value();
        auto it = result.find(delValue);
        if (it != std::string::npos) 
          result.erase(result.begin() + it, result.begin() + it + delValue.size());
        else
          return -1;

        if (result[0] == '_')
          result.erase(0, 1);

	delValue = result;
      }

      {
	boost::system::error_code ec;
	request req;
	req.push_range("HSET", key, std::map<std::string, std::string>{{"blockHosts", delValue}});
	response<int> resp;

	redis_exec(conn, ec, req, resp, y);

	if ((bool)ec) {
	  return -1;
	}

	return std::get<0>(resp).value();
      }
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return -2;
  }
}

} } // namespace rgw::d4n
