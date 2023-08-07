#include <boost/algorithm/string.hpp>
#include "rgw_redis_driver.h"
#include <boost/redis/src.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace cache { 

std::unordered_map<std::string, Partition> RedisDriver::partitions;

std::list<std::string> build_attrs(rgw::sal::Attrs* binary) 
{
  std::list<std::string> values;
  rgw::sal::Attrs::iterator attrs;

  /* Convert to vector */
  if (binary != NULL) {
    for (attrs = binary->begin(); attrs != binary->end(); ++attrs) {
      values.push_back(attrs->first);
      values.push_back(attrs->second.to_str());
    }
  }

  return values;
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

template <typename T>
auto RedisDriver::redis_exec(boost::system::error_code ec, boost::redis::request req, boost::redis::response<T>& resp, optional_yield y) {
  assert(y); 
  auto yield = y.get_yield_context();

  return conn.async_exec(req, resp, yield[ec]);
}

bool RedisDriver::key_exists(const DoutPrefixProvider* dpp, const std::string& key) 
{
  std::string entry = partition_info.location + key;
  response<int> resp;

  try {
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", entry);

    redis_exec(ec, req, resp, y);

    if (ec)
      return false;
  } catch(std::exception &e) {}

  return std::get<0>(resp).value();
}

std::vector<Entry> RedisDriver::list_entries(const DoutPrefixProvider* dpp) 
{
  std::vector<Entry> result;

  for (auto it = entries.begin(); it != entries.end(); ++it) 
    result.push_back(it->second);

  return result;
}

size_t RedisDriver::get_num_entries(const DoutPrefixProvider* dpp) 
{
  return entries.size();
}

/*
uint64_t RedisDriver::get_free_space(const DoutPrefixProvider* dpp) 
{
  int result = -1;

  if (!client.is_connected()) 
    find_client(dpp);

  try {
    client.info([&result](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        int usedMem = -1;
	int maxMem = -1;

        std::istringstream iss(reply.as_string());
	std::string line;    
        while (std::getline(iss, line)) {
	  size_t pos = line.find_first_of(":");
	  if (pos != std::string::npos) {
	    if (line.substr(0, pos) == "used_memory") {
	      usedMem = std::stoi(line.substr(pos + 1, line.length() - pos - 2));
	    } else if (line.substr(0, line.find_first_of(":")) == "maxmemory") {
	      maxMem = std::stoi(line.substr(pos + 1, line.length() - pos - 2));
	    } 
	  }
        }

	if (usedMem > -1 && maxMem > -1)
	  result = maxMem - usedMem;
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
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
  this->cct = cct;

  if (partition_info.location.back() != '/') {
    partition_info.location += "/";
  }

  config cfg;
  cfg.addr.host = cct->_conf->rgw_d4n_host; // TODO: Replace with cache address
  cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);

  if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return -EDESTADDRREQ;
  }

  conn.async_run(cfg, {}, net::detached);

  return 0;
}

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  try {
    boost::system::error_code ec;
    response<std::string> resp;
    auto redisAttrs = build_attrs(&attrs);

    if (bl.length()) {
      redisAttrs.push_back("data");
      redisAttrs.push_back(bl.to_str());
    }

    request req;
    req.push_range("HMSET", entry, redisAttrs);

    redis_exec(ec, req, resp, y);

    if (std::get<0>(resp).value() != "OK" || ec) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return insert_entry(dpp, key, 0, len); // why is offset necessarily 0? -Sam
}

int RedisDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  
  if (key_exists(dpp, key)) {
    /* Retrieve existing values from cache */
    try {
      boost::system::error_code ec;
      response< std::map<std::string, std::string> > resp;
      request req;
      req.push("HGETALL", entry);

      redis_exec(ec, req, resp, y);

      if (ec)
	return -1;

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
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object was not retrievable." << dendl;
    return -2;
  }

  return 0;
}

int RedisDriver::del(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y)
{
  std::string entry = partition_info.location + key;

  if (key_exists(dpp, key)) {
    try {
      boost::system::error_code ec;
      response<int> resp;
      request req;
      req.push("DEL", entry);

      redis_exec(ec, req, resp, y);

      if (ec)
	return -1;

      return std::get<0>(resp).value() - 1; 
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

int RedisDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) 
{
  std::string value;
  std::string entry = partition_info.location + key;

  if (key_exists(dpp, key)) {
    try {
      boost::system::error_code ec;
      response<std::string> resp;
      request req;
      req.push("HGET", entry, "data");

      redis_exec(ec, req, resp, y);

      if (ec)
	return -1;

      value = std::get<0>(resp).value();
    } catch(std::exception &e) {
      return -1;
    }
  }

  try { // do we want key check here? -Sam
    /* Append to existing value or set as new value */
    boost::system::error_code ec;
    response<std::string> resp;
    std::string newVal = value + bl_data.to_str();

    request req;
    req.push("HMSET", entry, "data", newVal);

    redis_exec(ec, req, resp, y);

    if (std::get<0>(resp).value() != "OK" || ec) {
      return -1;
    }
  } catch(std::exception &e) {
    return -1;
  }

  return 0;
}

int RedisDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (key_exists(dpp, key)) {
    response<int> resp;

    try {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", entry, "data");

      redis_exec(ec, req, resp, y);

      if (ec)
	return -1;
    } catch(std::exception &e) {
      return -1;
    }

    if (std::get<0>(resp).value()) {
      try {
	boost::system::error_code ec;
	request req;
	req.push("HDEL", entry, "data");

	redis_exec(ec, req, resp, y);

	if (!std::get<0>(resp).value() || ec) {
	  return -1;
	} else {
	  return remove_entry(dpp, key);
	}
      } catch(std::exception &e) {
	return -1;
      }
    } else {
      return 0; /* No delete was necessary */
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

int RedisDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  std::string entry = partition_info.location + key;

  if (key_exists(dpp, key)) {
    try {
      boost::system::error_code ec;
      response< std::map<std::string, std::string> > resp;
      request req;
      req.push("HGETALL", entry);

      redis_exec(ec, req, resp, y);

      if (ec)
	return -1;

      for (auto const& it : std::get<0>(resp).value()) {
	if (it.first != "data") {
	  buffer::list bl_value;
	  bl_value.append(it.second);
	  attrs.insert({it.first, bl_value});
	  bl_value.clear();
	}
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

int RedisDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) 
{
  if (attrs.empty())
    return -1;
      
  std::string entry = partition_info.location + key;

  if (key_exists(dpp, key)) {
    /* Every attr set will be treated as new */
    try {
      boost::system::error_code ec;
      response<std::string> resp;
      std::string result;
      std::list<std::string> redisAttrs = build_attrs(&attrs);

      request req;
      req.push_range("HMSET", entry, redisAttrs);

      redis_exec(ec, req, resp, y);

      if (std::get<0>(resp).value() != "OK" || ec) {
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

  if (key_exists(dpp, key)) {
    try {
      boost::system::error_code ec;
      response<std::string> resp;
      auto redisAttrs = build_attrs(&attrs);

      request req;
      req.push_range("HMSET", entry, redisAttrs);

      redis_exec(ec, req, resp, y);

      if (std::get<0>(resp).value() != "OK" || ec) {
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

  if (key_exists(dpp, key)) {
    try {
      boost::system::error_code ec;
      response<int> resp;
      auto redisAttrs = build_attrs(&del_attrs);

      request req;
      req.push_range("HDEL", entry, redisAttrs);

      redis_exec(ec, req, resp, y);

      if (ec)
	return -1;

      return std::get<0>(resp).value() - del_attrs.size();  /* Returns number of fields deleted */
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    return 0; /* No delete was necessary */
  }
}

std::string RedisDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  response<std::string> value;

  if (key_exists(dpp, key)) {
    response<int> resp;

    /* Ensure field was set */
    try {
      boost::system::error_code ec;
      request req;
      req.push("HEXISTS", entry, attr_name);

      redis_exec(ec, req, resp, y);

      if (ec)
	return {};
    } catch(std::exception &e) {
      return {};
    }
    
    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 20) << "RGW Redis Cache: Attribute was not set." << dendl;
      return {};
    }

    /* Retrieve existing value from cache */
    try {
      boost::system::error_code ec;
      request req;
      req.push("HGET", entry, attr_name);

      redis_exec(ec, req, resp, y);

      if (ec)
	return {};
    } catch(std::exception &e) {
      return {};
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object is not in cache." << dendl;
    return {};
  }

  return std::get<0>(value).value();
}

int RedisDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) 
{
  std::string entry = partition_info.location + key;
  response<int> resp;
    
  if (key_exists(dpp, key)) {
    /* Every attr set will be treated as new */
    try {
      boost::system::error_code ec;
      request req;
      req.push("HSET", entry, attr_name, attr_val);

      redis_exec(ec, req, resp, y);

      if (ec)
	return {};
    } catch(std::exception &e) {
      return -1;
    }
  } else {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Object is not in cache." << dendl;
    return -2; 
  }

  return std::get<0>(resp).value() - 1; /* Returns number of fields set */
}

static Aio::OpFunc redis_read_op(optional_yield y, boost::redis::connection& conn,
                                 off_t read_ofs, off_t read_len, const std::string& key)
{
  return [y, &conn, read_ofs, read_len, key] (Aio* aio, AioResult& r) mutable {
    using namespace boost::asio;
    async_completion<yield_context, void()> init(y.get_yield_context());
    auto ex = get_associated_executor(init.completion_handler);

    boost::redis::request req;
    req.push("HGET", key, "data");

    // TODO: Make unique pointer once support is added
    auto s = std::make_shared<RedisDriver::redis_response>();
    auto& resp = s->resp;

    conn.async_exec(req, resp, bind_executor(ex, RedisDriver::redis_aio_handler{aio, r, s}));
  };
}

rgw::AioResultList RedisDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) 
{
  std::string entry = partition_info.location + key;
  rgw_raw_obj r_obj;
  r_obj.oid = key;

  return aio->get(r_obj, redis_read_op(y, conn, ofs, len, entry), cost, id);
}

} } // namespace rgw::cache
