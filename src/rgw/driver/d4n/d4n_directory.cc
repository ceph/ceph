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

void redis_exec(std::shared_ptr<connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::generic_response& resp, optional_yield y)
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

int BucketDirectory::zadd(const DoutPrefixProvider* dpp, const std::string& bucket_id, double score, const std::string& member, optional_yield y, bool multi)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZADD", bucket_id, "CH", std::to_string(0), member);

    response<std::string> resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "BucketDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}

int BucketDirectory::zrem(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, optional_yield y, bool multi)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", bucket_id, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "BucketDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BucketDirectory::zrange(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    if (offset == 0 && count == 0) {
      req.push("ZRANGE", bucket_id, start, stop, "bylex");
    } else {
      req.push("ZRANGE", bucket_id, start, stop, "bylex", "LIMIT", offset, count);
    }

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "BucketDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BucketDirectory::zscan(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t cursor, const std::string& pattern, uint64_t count, std::vector<std::string>& members, uint64_t next_cursor, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;

    req.push("ZSCAN", bucket_id, cursor, "MATCH", pattern, "COUNT", count);

    boost::redis::generic_response resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    std::vector<boost::redis::resp3::basic_node<std::__cxx11::basic_string<char> > > root_array;
    if (resp.has_value()) {
      root_array = resp.value();
      ldpp_dout(dpp, 20) << "BucketDirectory::" << __func__ << "() aggregate size is: " << root_array.size() << dendl;
      auto size = root_array.size();
      if (size >= 2) {
        //Nothing of interest at index 0, index 1 has the next cursor value
        next_cursor = std::stoull(root_array[1].value);

        //skip the first 3 values to get the actual member, score
        for (uint64_t i = 3; i < size; i = i+2) {
          members.emplace_back(root_array[i].value);
          ldpp_dout(dpp, 20) << "BucketDirectory::" << __func__ << "() member is: " << root_array[i].value << dendl;
        }
      }
    } else {
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BucketDirectory::zrank(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& member, uint64_t& rank, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;

    req.push("ZRANK", bucket_id, member);

    response<int> resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    rank = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
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
  redisValues.push_back("etag");
  redisValues.push_back(object->etag);
  redisValues.push_back("objSize");
  redisValues.push_back(std::to_string(object->size));
  redisValues.push_back("userId");
  redisValues.push_back(object->user_id);
  redisValues.push_back("displayName");
  redisValues.push_back(object->display_name);

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
  ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");
  fields.push_back("etag");
  fields.push_back("objSize");
  fields.push_back("userId");
  fields.push_back("displayName");

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
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values returned." << dendl;
      return -ENOENT;
    }

    using Fields = rgw::d4n::ObjectFields;
    object->objName = std::get<0>(resp).value()[std::size_t(Fields::ObjName)];
    object->bucketName = std::get<0>(resp).value()[std::size_t(Fields::BucketName)];
    object->creationTime = std::get<0>(resp).value()[std::size_t(Fields::CreationTime)];
    object->dirty = (std::stoi(std::get<0>(resp).value()[std::size_t(Fields::Dirty)]) != 0);
    boost::split(object->hostsList, std::get<0>(resp).value()[std::size_t(Fields::Hosts)], boost::is_any_of("_"));
    object->etag = std::get<0>(resp).value()[std::size_t(Fields::Etag)];
    object->size = std::stoull(std::get<0>(resp).value()[std::size_t(Fields::ObjSize)]);
    object->user_id = std::get<0>(resp).value()[std::size_t(Fields::UserID)];
    object->display_name = std::get<0>(resp).value()[std::size_t(Fields::DisplayName)];
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
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values copied." << dendl;
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
  ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    response<int> resp;
    request req;
    req.push("DEL", key);

    redis_exec(conn, ec, req, resp, y);

    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): No values deleted." << dendl;
      return -ENOENT;
    }

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
  int ret = -1;
  std::string key = build_index(object);

  if ((ret = exist_key(dpp, object, y))) {
    try {
      if (field == "hosts") {
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "ObjectDirectory::" << __func__ << "(): Appending to hosts list." << dendl;

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
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): Object does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
}

int ObjectDirectory::zadd(const DoutPrefixProvider* dpp, CacheObj* object, double score, const std::string& member, optional_yield y, bool multi)
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZADD", key, "CH", std::to_string(score), member);

    response<std::string> resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}

int ObjectDirectory::zrange(const DoutPrefixProvider* dpp, CacheObj* object, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& start, const std::string& stop, std::vector<std::string>& members, optional_yield y)
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", key, start, stop);

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::zrem(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, optional_yield y, bool multi)
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", key, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp, CacheObj* object, double min, double max, optional_yield y, bool multi)
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREMRANGEBYSCORE", key, std::to_string(min), std::to_string(max));
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!multi) {
      if (std::get<0>(resp).value() == "0") {
        ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "() No element removed!" << dendl;
        return -ENOENT;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int ObjectDirectory::incr(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  std::string key = build_index(object);
  key = key + "_versioned_epoch";
  uint64_t value;
  try {
    boost::system::error_code ec;
    request req;
    req.push("INCR", key);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    value = std::stoull(std::get<0>(resp).value());

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return value;
}

int ObjectDirectory::zrank(const DoutPrefixProvider* dpp, CacheObj* object, const std::string& member, std::string& index, optional_yield y)
{
  std::string key = build_index(object);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANK", key, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    index = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "ObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
}

int BlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  response<int> resp;

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
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;
    
  std::string hosts;
  std::list<std::string> redisValues;
    
  /* Creating a redisValues of the entry's properties */
  redisValues.push_back("blockID");
  redisValues.push_back(std::to_string(block->blockID));
  redisValues.push_back("version");
  redisValues.push_back(block->version);
  redisValues.push_back("deleteMarker");
  int ret = -1;
  if ((ret = check_bool(std::to_string(block->deleteMarker))) != -EINVAL) {
    block->deleteMarker = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value for delete marker" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block->deleteMarker));
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
  if ((ret = check_bool(std::to_string(block->cacheObj.dirty))) != -EINVAL) {
    block->cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues.push_back(std::to_string(block->cacheObj.dirty));
  redisValues.push_back("hosts");
  
  hosts.clear();
  for (auto const& host : block->cacheObj.hostsList) {
    if (hosts.empty())
    hosts = host + "_";
    else
    hosts = hosts + host + "_";
  }

  if (!hosts.empty())
  hosts.pop_back();

  redisValues.push_back(hosts);
  redisValues.push_back("etag");
  redisValues.push_back(block->cacheObj.etag);
  redisValues.push_back("objSize");
  redisValues.push_back(std::to_string(block->cacheObj.size));
  redisValues.push_back("userId");
  redisValues.push_back(block->cacheObj.user_id);
  redisValues.push_back("displayName");
  redisValues.push_back(block->cacheObj.display_name);

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

template<typename T, typename Seq>
struct expander;

template<typename T, std::size_t... Is>
struct expander<T, std::index_sequence<Is...>> {
template<typename E, std::size_t>
using elem = E;

using type = boost::redis::response<elem<T, Is>...>;
};

template <size_t N, class Type>
struct redis_response
{
  using type = typename expander<Type, std::make_index_sequence<N>>::type;
};

template <typename Integer, Integer ...I, typename F>
constexpr void constexpr_for_each(std::integer_sequence<Integer, I...>, F &&func)
{
    (func(std::integral_constant<Integer, I>{}) , ...);
}

template <auto N, typename F>
constexpr void constexpr_for(F &&func)
{
    if constexpr (N > 0)
    {
        constexpr_for_each(std::make_integer_sequence<decltype(N), N>{}, std::forward<F>(func));
    }
}

template <typename T>
void parse_response(T t, std::vector<std::vector<std::string>>& responses)
{
    constexpr_for<std::tuple_size_v<T>>([&](auto index)
    {
      std::vector<std::string> empty_vector;
      constexpr auto i = index.value;
      if (std::get<i>(t).value().has_value()) {
        if (std::get<i>(t).value().value().empty()) {
          responses.emplace_back(empty_vector);
        } else {
          responses.emplace_back(std::get<i>(t).value().value());
        }
      } else {
        responses.emplace_back(empty_vector);
      }
    });
}

int BlockDirectory::get(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  request req;
  redis_response<100, std::optional<std::vector<std::string>>>::type resp;
  for (auto block : blocks) {
    std::string key = build_index(&block);
    std::vector<std::string> fields;
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    fields.push_back("blockID");
    fields.push_back("version");
    fields.push_back("deleteMarker");
    fields.push_back("size");
    fields.push_back("globalWeight");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("hosts");
    fields.push_back("etag");
    fields.push_back("objSize");
    fields.push_back("userId");
    fields.push_back("displayName");

    try {
      req.push_range("HMGET", key, fields);
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } //end - for

  try {
    boost::system::error_code ec;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  std::vector<std::vector<std::string>> responses;
  parse_response<decltype(resp)>(resp, responses);

  for (size_t i = 0; i < blocks.size(); i++) {
    CacheBlock* block = &blocks[i];
    auto vec = responses[i];
    if (vec.empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): No values returned for key=" << build_index(block) << dendl;
      continue;
    }

    using Fields = rgw::d4n::BlockFields;
    block->blockID = std::stoull(vec[std::size_t(Fields::BlockID)]);
    block->version = vec[std::size_t(Fields::Version)];
    block->deleteMarker = (std::stoi(vec[std::size_t(Fields::DeleteMarker)]) != 0);
    block->size = std::stoull(vec[std::size_t(Fields::Size)]);
    block->globalWeight = std::stoull(vec[std::size_t(Fields::GlobalWeight)]);
    block->cacheObj.objName = vec[std::size_t(Fields::ObjName)];
    block->cacheObj.bucketName = vec[std::size_t(Fields::BucketName)];
    block->cacheObj.creationTime = vec[std::size_t(Fields::CreationTime)];
    block->cacheObj.dirty = (std::stoi(vec[std::size_t(Fields::Dirty)]) != 0);
    boost::split(block->cacheObj.hostsList, vec[std::size_t(Fields::Hosts)], boost::is_any_of("_"));
    block->cacheObj.etag = vec[std::size_t(Fields::Etag)];
    block->cacheObj.size = std::stoull(vec[std::size_t(Fields::ObjSize)]);
    block->cacheObj.user_id = vec[std::size_t(Fields::UserID)];
    block->cacheObj.display_name = vec[std::size_t(Fields::DisplayName)];
  }

  return 0;
}

int BlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  std::vector<std::string> fields;
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  fields.push_back("blockID");
  fields.push_back("version");
  fields.push_back("deleteMarker");
  fields.push_back("size");
  fields.push_back("globalWeight");

  fields.push_back("objName");
  fields.push_back("bucketName");
  fields.push_back("creationTime");
  fields.push_back("dirty");
  fields.push_back("hosts");
  fields.push_back("etag");
  fields.push_back("objSize");
  fields.push_back("userId");
  fields.push_back("displayName");

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
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values returned for key=" << key << dendl;
      return -ENOENT;
    } 

    block->blockID = std::stoull(std::get<0>(resp).value().value()[0]);
    block->version = std::get<0>(resp).value().value()[1];
    block->deleteMarker = (std::stoi(std::get<0>(resp).value().value()[2]) != 0);
    block->size = std::stoull(std::get<0>(resp).value().value()[3]);
    block->globalWeight = std::stoull(std::get<0>(resp).value().value()[4]);
    block->cacheObj.objName = std::get<0>(resp).value().value()[5];
    block->cacheObj.bucketName = std::get<0>(resp).value().value()[6];
    block->cacheObj.creationTime = std::get<0>(resp).value().value()[7];
    block->cacheObj.dirty = (std::stoi(std::get<0>(resp).value().value()[8]) != 0);
    boost::split(block->cacheObj.hostsList, std::get<0>(resp).value().value()[9], boost::is_any_of("_"));
    block->cacheObj.etag = std::get<0>(resp).value().value()[10];
    block->cacheObj.size = std::stoull(std::get<0>(resp).value().value()[11]);
    block->cacheObj.user_id = std::get<0>(resp).value().value()[12];
    block->cacheObj.display_name = std::get<0>(resp).value().value()[13];
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
      ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values copied." << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int BlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, bool multi) 
{
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    request req;
    req.push("DEL", key);
    if (!multi) {
      response<int> resp;
      redis_exec(conn, ec, req, resp, y);
      if (!std::get<0>(resp).value()) {
        ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): No values deleted for key=" << key << dendl;
        return -ENOENT;
      }
    } else { //if delete is called as part of a transaction, the command will be queued, hence the response will be a string
      response<std::string> resp;
      redis_exec(conn, ec, req, resp, y);
    }
    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << std::endl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0; 
}

int BlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  int ret = -1;
  std::string key = build_index(block);

  if ((ret = exist_key(dpp, block, y))) {
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
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
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
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Host was not found." << dendl;
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

int BlockDirectory::zadd(const DoutPrefixProvider* dpp, CacheBlock* block, double score, const std::string& member, optional_yield y, bool multi)
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZADD", key, "CH", std::to_string(score), member);

    response<std::string> resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
    if (!multi) {
      if (std::get<0>(resp).value() != "1") {
        ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
        return -EINVAL;
      }
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;

}

int BlockDirectory::zrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Empty response" << dendl;
      return -EINVAL;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::zrevrange(const DoutPrefixProvider* dpp, CacheBlock* block, int start, int stop, std::vector<std::string>& members, optional_yield y)
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREVRANGE", key, std::to_string(start), std::to_string(stop));

    response<std::vector<std::string> > resp;
    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Empty response" << dendl;
      return -EINVAL;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::zrem(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& member, optional_yield y)
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("ZREM", key, member);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::watch(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
{
  std::string key = build_index(block);
  try {
    boost::system::error_code ec;
    request req;
    req.push("WATCH", key);
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::exec(const DoutPrefixProvider* dpp, std::vector<std::string>& responses, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("EXEC");
    boost::redis::generic_response resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << std::endl;
      return -ec.value();
    }

    for (uint64_t i = 0; i < resp.value().size(); i++) {
      ldpp_dout(dpp, 20) << "BlockDirectory::" << __func__ << "() MULTI: " << resp.value().front().value << dendl;
      responses.emplace_back(resp.value().front().value);
      boost::redis::consume_one(resp);
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    std::cout << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::multi(const DoutPrefixProvider* dpp, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("MULTI");
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::discard(const DoutPrefixProvider* dpp, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("DISCARD");
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int BlockDirectory::unwatch(const DoutPrefixProvider* dpp, optional_yield y)
{
  try {
    boost::system::error_code ec;
    request req;
    req.push("UNWATCH");
    response<std::string> resp;

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "OK") {
      ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -EINVAL;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

} } // namespace rgw::d4n
