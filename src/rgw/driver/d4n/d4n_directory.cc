#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory.h"

namespace rgw { namespace d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = boost::asio::consign(std::move(handler), conn);
    return boost::asio::dispatch(get_executor(),
        [c = conn, &req, &resp, h = std::move(h)] () mutable {
            return c->async_exec(req, resp, std::move(h));
    });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename... Types>
void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int check_bool(std::string str) {
  if (str == "true" || str == "1") {
    return 1;
  } else if (str == "false" || str == "0") {
    return 0;
  } else {
    return -EINVAL;
  }
}

std::string ObjectDirectory::build_index(CacheObj* object) 
{
  return object->bucketName + "_" + object->objName;
}

int ObjectDirectory::exist_key(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

int ObjectDirectory::set(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  /* For existing keys, call get method beforehand. 
     Sets completely overwrite existing values. */
  std::string key = build_index(object);

  std::string endpoint;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("objName");
  redisValues.push_back(object->objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(object->bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(object->creationTime); 
  redisValues.push_back("dirty");
  int ret = -1;
  if ((ret = check_bool(std::to_string(object->dirty))) != -EINVAL) {
    object->dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(object->dirty));
  redisValues.push_back("hosts");

  for (auto const& host : object->hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint); 

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push_range("HSET", key, redisValues);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::get(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);
  std::vector<std::string> fields;

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");

  try {
    boost::system::error_code ec;
    response< std::vector<std::string> > resp;
    request req;
    req.push_range("HMGET", key, fields);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "(): No values returned." << dendl;
      return -ENOENT;
    }

    object->objName = std::get<0>(resp).value()[0];
    object->bucketName = std::get<0>(resp).value()[1];
    object->creationTime = std::get<0>(resp).value()[2];
    object->dirty = (std::stoi(std::get<0>(resp).value()[3]) != 0);
    boost::split(object->hostsList, std::get<0>(resp).value()[4], boost::is_any_of("_"));
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
int ObjectDirectory::copy(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  std::string key = build_index(object);
  auto copyObj = CacheObj{ .objName = copyName, .bucketName = copyBucketName };
  std::string copyKey = build_index(&copyObj);

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>> 
    > resp;
    request req;
    req.push("MULTI");
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int ObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y) 
{
  std::string key = build_index(object);

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push("DEL", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0; 
}

int ObjectDirectory::update_field(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& field, std::string& value, optional_yield y)
{
  std::string key = build_index(object);

  if (exist_key(dpp, object, y)) {
    try {
      if (field == "hosts") {
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "ObjectDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response<std::string> resp;
	request req;
	req.push("HGET", key, field);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	/* If entry exists, it should have at least one host */
	std::get<0>(resp).value() += "_";
	std::get<0>(resp).value() += value;
	value = std::get<0>(resp).value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
	  bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
}

int BlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  response<int> resp;
  ldpp_dout(dpp, 10) << __func__ << "(): index is: " << key << dendl;
  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return false;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

int BlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  /* For existing keys, call get method beforehand. 
     Sets completely overwrite existing values. */
  std::string key = build_index(block);
    
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
  redisValues.push_back("objName");
  redisValues.push_back(block->cacheObj.objName);
  redisValues.push_back("bucketName");
  redisValues.push_back(block->cacheObj.bucketName);
  redisValues.push_back("creationTime");
  redisValues.push_back(block->cacheObj.creationTime); 
  redisValues.push_back("dirty");
  int ret = -1;
  if ((ret = check_bool(std::to_string(block->cacheObj.dirty))) != -EINVAL) {
    block->cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block->cacheObj.dirty));
  redisValues.push_back("hosts");
  
  endpoint.clear();
  for (auto const& host : block->cacheObj.hostsList) {
    if (endpoint.empty())
      endpoint = host + "_";
    else
      endpoint = endpoint + host + "_";
  }

  if (!endpoint.empty())
    endpoint.pop_back();

  redisValues.push_back(endpoint);

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push_range("HSET", key, redisValues);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  std::vector<std::string> fields;

  ldpp_dout(dpp, 10) << __func__ << "(): index is: " << key << dendl;

  fields.push_back("blockID");
  fields.push_back("version");
  fields.push_back("size");
  fields.push_back("globalWeight");

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");

  try {
    boost::system::error_code ec;
    response< std::optional<std::vector<std::string>> > resp;
    request req;
    req.push_range("HMGET", key, fields);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
      return -ENOENT;
    } 

    block->blockID = std::stoull(std::get<0>(resp).value().value()[0]);
    block->version = std::get<0>(resp).value().value()[1];
    block->size = std::stoull(std::get<0>(resp).value().value()[2]);
    block->globalWeight = std::stoull(std::get<0>(resp).value().value()[3]);
    block->cacheObj.objName = std::get<0>(resp).value().value()[4];
    block->cacheObj.bucketName = std::get<0>(resp).value().value()[5];
    block->cacheObj.creationTime = std::get<0>(resp).value().value()[6];
    block->cacheObj.dirty =(std::stoi(std::get<0>(resp).value().value()[7]) != 0);
    boost::split(block->cacheObj.hostsList, std::get<0>(resp).value().value()[8], boost::is_any_of("_"));
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
int BlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  std::string key = build_index(block);
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>> 
    > resp;
    request req;
    req.push("MULTI");
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int BlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    request req;
    req.push("DEL", key);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0; 
}

int BlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  std::string key = build_index(block);

  if (exist_key(dpp, block, y)) {
    try {
      if (field == "hosts") { 
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "BlockDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response< std::optional<std::string> > resp;
	request req;
	req.push("HGET", key, field);

	redis_exec(conn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	/* If entry exists, it should have at least one host */
	std::get<0>(resp).value().value() += "_";
	std::get<0>(resp).value().value() += value;
	value = std::get<0>(resp).value().value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
          bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else {
    return -ENOENT;
  }
}

int BlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
{
  std::string key = build_index(block);

  try {
    {
      boost::system::error_code ec;
      response< std::optional<std::string> > resp;
      request req;
      req.push("HGET", key, "hosts");

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      if (std::get<0>(resp).value().value().empty()) {
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values returned." << dendl;
	return -ENOENT;
      }

      std::string result = std::get<0>(resp).value().value();
      auto it = result.find(value);
      if (it != std::string::npos) { 
	result.erase(result.begin() + it, result.begin() + it + value.size());
      } else {
	return -ENOENT;
      }

      if (result[0] == '_') {
	result.erase(0, 1);
      } else if (result.length() && result[result.length() - 1] == '_') {
	result.erase(result.length() - 1, 1);
      }

      if (result.length() == 0) /* Last host, delete entirely */
	return del(dpp, block, y); 

  value = result;
    }

    {
      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, "hosts", value);

      redis_exec(conn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

} } // namespace rgw::d4n
