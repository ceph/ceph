#include <boost/algorithm/string.hpp>
#include <boost/redis/src.hpp>
#include <boost/asio/detached.hpp>

#include "common/dout.h" 
#include "common/async/blocked_completion.h"
#include "rgw_redis_driver.h"

namespace rgw { namespace cache {

std::list<std::string> build_attrs(const rgw::sal::Attrs& binary) 
{
  std::list<std::string> values;

  /* Convert to vector */
  if (!binary.empty()) {
    for (auto attrs = binary.begin(); attrs != binary.end(); ++attrs) {
      values.push_back(attrs->first);
      values.push_back(attrs->second.to_str());
    }
  }

  return values;
}

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
        [c=conn, &req, &resp, h=std::move(h)] {
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

template <typename T>
void redis_exec(std::shared_ptr<connection> conn, boost::system::error_code& ec, boost::redis::request& req, boost::redis::response<T>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

int RedisDriver::initialize(const DoutPrefixProvider* dpp)
{
  if (partition_info.location.back() != '/') {
    partition_info.location += "/";
  }

  std::string address = dpp->get_cct()->_conf->rgw_local_cache_address;

  config cfg;
  cfg.addr.host = address.substr(0, address.find(":"));
  cfg.addr.port = address.substr(address.find(":") + 1, address.length());
  cfg.clientname = "RedisDriver";

  if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
    return -EDESTADDRREQ;
  }

  conn->async_run(cfg, {}, net::consign(net::detached, conn));

  return 0;
}

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  /* Every set will be treated as new */
  try {
    boost::system::error_code ec;
    response<std::string> resp;
    auto redisAttrs = build_attrs(attrs);

    if (bl.length()) {
      redisAttrs.push_back("data");
      redisAttrs.push_back(bl.to_str());
    }

    request req;
    req.push_range("HMSET", entry, redisAttrs);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  this->free_space -= bl.length();
  return 0;
}

int RedisDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  
  /* Retrieve existing values from cache */
  try {
    boost::system::error_code ec;
    response< std::map<std::string, std::string> > resp;
    request req;
    req.push("HGETALL", entry);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }

    for (auto const& it : std::get<0>(resp).value()) {
      if (it.first == "data") {
	bl.append(it.second);
      } else {
	buffer::list bl_value;
	bl_value.append(it.second);
	attrs.insert({it.first, bl_value});
	bl_value.clear();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisDriver::del(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  std::string entry = partition_info.location + key;
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", entry, "data");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  if (std::get<0>(resp).value()) {
    response<std::string> data;
    response<int> ret;

    try {
      boost::system::error_code ec;
      request req;
      req.push("HGET", entry, "data");

      redis_exec(conn, ec, req, data, y);

      if (ec) {
	return -ec.value();
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
      return -EINVAL;
    }

    try {
      boost::system::error_code ec;
      request req;
      req.push("DEL", entry);

      redis_exec(conn, ec, req, ret, y);

      if (!std::get<0>(ret).value()) {
	return -ENOENT;
      } else if (ec) {
	return -ec.value();
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
      return -EINVAL;
    }

    this->free_space += std::get<0>(data).value().length();
  }

  return 0; 
}

int RedisDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, const bufferlist& bl_data, optional_yield y) 
{
  response<int> exists;
  std::string value;
  std::string entry = partition_info.location + key;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", entry, "data");

    redis_exec(conn, ec, req, exists, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  if (!std::get<0>(exists).value()) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): Data field was not found." << dendl;
    return -ENOENT;
  }

  try {
    boost::system::error_code ec;
    response<std::string> resp;
    request req;
    req.push("HGET", entry, "data");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }

    value = std::get<0>(resp).value();
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  try {
    /* Append to existing value or set as new value */
    boost::system::error_code ec;
    response<std::string> resp;
    std::string newVal = value + bl_data.to_str();

    request req;
    req.push("HMSET", entry, "data", newVal);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  this->free_space -= bl_data.length();
  return 0;
}

int RedisDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", entry, "data");

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  if (std::get<0>(resp).value()) {
    response<std::string> data;
    response<int> ret;

    try {
      boost::system::error_code ec;
      request req;
      req.push("HGET", entry, "data");

      redis_exec(conn, ec, req, data, y);

      if (ec) {
	return -ec.value();
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
      return -EINVAL;
    }

    try {
      boost::system::error_code ec;
      request req;
      req.push("HDEL", entry, "data");

      redis_exec(conn, ec, req, ret, y);

      if (!std::get<0>(ret).value()) {
	return -ENOENT;
      } else if (ec) {
	return -ec.value();
      }
    } catch (std::exception &e) {
      ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
      return -EINVAL;
    }

    this->free_space += std::get<0>(data).value().length();
  }

  return 0;
}

int RedisDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  try {
    boost::system::error_code ec;
    response< std::map<std::string, std::string> > resp;
    request req;
    req.push("HGETALL", entry);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }

    for (auto const& it : std::get<0>(resp).value()) {
      if (it.first != "data") {
	buffer::list bl_value;
	bl_value.append(it.second);
	attrs.insert({it.first, bl_value});
	bl_value.clear();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) 
{
  if (attrs.empty())
    return -EINVAL;
      
  std::string entry = partition_info.location + key;

  /* Every attr set will be treated as new */
  try {
    boost::system::error_code ec;
    response<std::string> resp;
    std::string result;
    std::list<std::string> redisAttrs = build_attrs(attrs);

    request req;
    req.push_range("HMSET", entry, redisAttrs);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, const rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  try {
    boost::system::error_code ec;
    response<std::string> resp;
    auto redisAttrs = build_attrs(attrs);

    request req;
    req.push_range("HMSET", entry, redisAttrs);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  try {
    boost::system::error_code ec;
    response<int> resp;
    auto redisAttrs = build_attrs(del_attrs);

    request req;
    req.push_range("HDEL", entry, redisAttrs);

    redis_exec(conn, ec, req, resp, y);

    if (!std::get<0>(resp).value()) {
      return -ENOENT;
    } else if (ec) {
      return -ec.value();
    }

    return std::get<0>(resp).value(); 
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int RedisDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, std::string& attr_val, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  response<std::string> value;
  response<int> resp;
  attr_val = "";

  /* Ensure field was set */
  try {
    boost::system::error_code ec;
    request req;
    req.push("HEXISTS", entry, attr_name);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  
  if (!std::get<0>(resp).value()) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): Attribute was not found." << dendl;
    return -ENOENT;
  }

  /* Retrieve existing value from cache */
  try {
    boost::system::error_code ec;
    request req;
    req.push("HGET", entry, attr_name);

    redis_exec(conn, ec, req, value, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  
  if (!std::get<0>(resp).value()) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): Attribute was not found." << dendl;
    return -ENOENT;
  }

  /* Retrieve existing value from cache */
  try {
    boost::system::error_code ec;
    request req;
    req.push("HGET", entry, attr_name);

    redis_exec(conn, ec, req, value, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  attr_val = std::get<0>(value).value();
  return 0;
}

int RedisDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  response<int> resp;
    
  /* Every attr set will be treated as new */
  try {
    boost::system::error_code ec;
    request req;
    req.push("HSET", entry, attr_name, attr_val);

    redis_exec(conn, ec, req, resp, y);

    if (ec) {
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 10) << "RedisDriver::" << __func__ << "(): ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

Aio::OpFunc RedisDriver::redis_read_op(optional_yield y, std::shared_ptr<connection> conn,
                                 off_t read_ofs, off_t read_len, const std::string& key)
{
  return [y, conn, &key] (Aio* aio, AioResult& r) mutable {
    using namespace boost::asio;
    spawn::yield_context yield = y.get_yield_context();
    async_completion<spawn::yield_context, void()> init(yield);
    auto ex = get_associated_executor(init.completion_handler);

    // TODO: Make unique pointer once support is added
    auto s = std::make_shared<RedisDriver::redis_response>();
    auto& resp = s->resp;
    auto& req = s->req;
    req.push("HGET", key, "data");

    conn->async_exec(req, resp, bind_executor(ex, RedisDriver::redis_aio_handler{aio, r, s}));
  };
}

Aio::OpFunc RedisDriver::redis_write_op(optional_yield y, std::shared_ptr<connection> conn,
                                 const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, const std::string& key)
{
  return [y, conn, &bl, &len, &attrs, &key] (Aio* aio, AioResult& r) mutable {
    using namespace boost::asio;
    spawn::yield_context yield = y.get_yield_context();
    async_completion<spawn::yield_context, void()> init(yield);
    auto ex = get_associated_executor(init.completion_handler);

    auto redisAttrs = build_attrs(attrs);

    if (bl.length()) {
      redisAttrs.push_back("data");
      redisAttrs.push_back(bl.to_str());
    }

    // TODO: Make unique pointer once support is added
    auto s = std::make_shared<RedisDriver::redis_response>();
    auto& resp = s->resp;
    auto& req = s->req;
    req.push_range("HMSET", key, redisAttrs);

    conn->async_exec(req, resp, bind_executor(ex, RedisDriver::redis_aio_handler{aio, r, s}));
  };
}

rgw::AioResultList RedisDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) 
{
  std::string entry = partition_info.location + key;
  rgw_raw_obj r_obj;
  r_obj.oid = key;

  return aio->get(r_obj, redis_read_op(y, conn, ofs, len, entry), cost, id);
}

rgw::AioResultList RedisDriver::put_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, const bufferlist& bl, uint64_t len, const rgw::sal::Attrs& attrs, uint64_t cost, uint64_t id) {
  std::string entry = partition_info.location + key;
  rgw_raw_obj r_obj;
  r_obj.oid = key;

  return aio->get(r_obj, redis_write_op(y, conn, bl, len, attrs, entry), cost, id);
} 

void RedisDriver::shutdown()
{
  // call cancel() on the connection's executor
  boost::asio::dispatch(conn->get_executor(), [c = conn] { c->cancel(); });
}
} } // namespace rgw::cache
