#include <boost/algorithm/string.hpp>
#include "rgw_redis_driver.h"
//#include "rgw_ssd_driver.h" // fix -Sam
//#include <aedis/src.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace cache {

std::unordered_map<std::string, Partition> RedisDriver::partitions;

std::vector< std::pair<std::string, std::string> > build_attrs(rgw::sal::Attrs* binary) 
{
  std::vector< std::pair<std::string, std::string> > values;
  rgw::sal::Attrs::iterator attrs;

  /* Convert to vector */
  if (binary != NULL) {
    for (attrs = binary->begin(); attrs != binary->end(); ++attrs) {
      values.push_back(std::make_pair(attrs->first, attrs->second.to_str()));
    }
  }

  return values;
}

int RedisDriver::find_client(const DoutPrefixProvider* dpp) 
{
  if (client.is_connected())
    return 0;

  if (addr.host == "" || addr.port == 0) { 
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client.connect(addr.host, addr.port, nullptr);

  if (!client.is_connected())
    return ECONNREFUSED;

  return 0;
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

  addr.host = cct->_conf->rgw_d4n_host; // change later -Sam
  addr.port = cct->_conf->rgw_d4n_port;

  if (partition_info.location.back() != '/') {
    partition_info.location += "/";
  }

  if (addr.host == "" || addr.port == 0) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Redis cache endpoint was not configured correctly" << dendl;
    return EDESTADDRREQ;
  }

  client.connect("127.0.0.1", 6379, nullptr);

  if (!client.is_connected()) {
    ldpp_dout(dpp, 10) << "RGW Redis Cache: Could not connect to redis cache endpoint." << dendl;
    return ECONNREFUSED;
  }

  return 0;
}

int RedisDriver::put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  /* Every set will be treated as new */ // or maybe, if key exists, simply return? -Sam
  try {
    std::string result; 
    auto redisAttrs = build_attrs(&attrs);
    redisAttrs.push_back({"data", bl.to_str()});

    client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
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

  return 0; // why is offset necessarily 0? -Sam
}

int RedisDriver::get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs) 
{
  std::string entry = partition_info.location + key;
  
  if (!client.is_connected()) 
    find_client(dpp);
    
    /* Retrieve existing values from cache */
  try {
      client.hgetall(entry, [&bl, &attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) {
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() == "data") {
                bl.append(arr[i + 1].as_string());
	      } else {
	        buffer::list bl_value;
		bl_value.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), bl_value});
		bl_value.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }


  return 0;
}

int RedisDriver::append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data) 
{
  std::string value;
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  try {
    client.hget(entry, "data", [&value](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        value = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  try { // do we want key check here? -Sam
    /* Append to existing value or set as new value */
    std::string newVal = value + bl_data.to_str();
    std::vector< std::pair<std::string, std::string> > field;
    field.push_back({"data", newVal});
    std::string result;

    client.hmset(entry, field, [&result](cpp_redis::reply &reply) {
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

int RedisDriver::delete_data(const DoutPrefixProvider* dpp, const::std::string& key) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  int exists = -2;

  try {
      client.hexists(entry, "data", [&exists](cpp_redis::reply &reply) {
	if (!reply.is_null()) {
	  exists = reply.as_integer();
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

    if (exists) {
      try {
	int result;
	std::vector<std::string> deleteField;
	deleteField.push_back("data");

	client.hdel(entry, deleteField, [&result](cpp_redis::reply &reply) {
	  if (reply.is_integer()) {
	    result = reply.as_integer(); 
	  }
	});

	client.sync_commit(std::chrono::milliseconds(1000));

	if (!result) {
	  return -1;
	}
      } catch(std::exception &e) {
	return -1;
      }
    } else {
      return 0; /* No delete was necessary */
    }
  return 0;
}

int RedisDriver::get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  try {
      client.hgetall(entry, [&attrs](cpp_redis::reply &reply) {
	if (reply.is_array()) { 
	  auto arr = reply.as_array();
    
	  if (!arr[0].is_null()) {
    	    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
	      if (arr[i].as_string() != "data") {
	        buffer::list bl_value;
		bl_value.append(arr[i + 1].as_string());
                attrs.insert({arr[i].as_string(), bl_value});
		bl_value.clear();
	      }
            }
	  }
	}
      });

      client.sync_commit(std::chrono::milliseconds(1000));
    } catch(std::exception &e) {
      return -1;
    }

  return 0;
}

int RedisDriver::set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) 
{
  if (attrs.empty())
    return -1;
      
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  /* Every attr set will be treated as new */
  try {
    std::string result;
    auto redisAttrs = build_attrs(&attrs);

    client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
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

int RedisDriver::update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  try {
    std::string result;
    auto redisAttrs = build_attrs(&attrs);

    client.hmset(entry, redisAttrs, [&result](cpp_redis::reply &reply) {
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

int RedisDriver::delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs) 
{
  std::string entry = partition_info.location + key;

  if (!client.is_connected()) 
    find_client(dpp);

  std::vector<std::string> getFields;

  try {
    client.hgetall(entry, [&getFields](cpp_redis::reply &reply) {
if (reply.is_array()) {
  auto arr = reply.as_array();
  
  if (!arr[0].is_null()) {
    for (long unsigned int i = 0; i < arr.size() - 1; i += 2) {
      getFields.push_back(arr[i].as_string());
    }
  }
}
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  auto redisAttrs = build_attrs(&del_attrs);
  std::vector<std::string> redisFields;

  std::transform(begin(redisAttrs), end(redisAttrs), std::back_inserter(redisFields),
    [](auto const& pair) { return pair.first; });

  /* Only delete attributes that have been stored */
  for (const auto& it : redisFields) {
    if (std::find(getFields.begin(), getFields.end(), it) == getFields.end()) {
      redisFields.erase(std::find(redisFields.begin(), redisFields.end(), it));
    }
  }

  try {
    int result = 0;

    client.hdel(entry, redisFields, [&result](cpp_redis::reply &reply) {
      if (reply.is_integer()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));

    return result - 1;
  } catch(std::exception &e) {
    return -1;
  }

  ldpp_dout(dpp, 20) << "RGW Redis Cache: Object is not in cache." << dendl;
  return -2;
}

std::string RedisDriver::get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name) 
{
  std::string entry = partition_info.location + key;
  std::string attrValue;

  if (!client.is_connected()) 
    find_client(dpp);

  int exists = -2;
  std::string getValue;

  /* Ensure field was set */
  try {
    client.hexists(entry, attr_name, [&exists](cpp_redis::reply& reply) {
      if (!reply.is_null()) {
        exists = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return {};
  }
  
  if (!exists) {
    ldpp_dout(dpp, 20) << "RGW Redis Cache: Attribute was not set." << dendl;
    return {};
  }

  /* Retrieve existing value from cache */
  try {
    client.hget(entry, attr_name, [&exists, &attrValue](cpp_redis::reply &reply) {
      if (!reply.is_null()) {
        attrValue = reply.as_string();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return {};
  }

  return attrValue;
}

int RedisDriver::set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attrVal) 
{
  std::string entry = partition_info.location + key;
  int result = 0;
    
  if (!client.is_connected()) 
    find_client(dpp);
    
  /* Every attr set will be treated as new */
  try {
    client.hset(entry, attr_name, attrVal, [&result](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
        result = reply.as_integer();
      }
    });

    client.sync_commit(std::chrono::milliseconds(1000));
  } catch(std::exception &e) {
    return -1;
  }

  return result - 1;
}
#if 0
std::unique_ptr<CacheAioRequest> RedisDriver::get_cache_aio_request_ptr(const DoutPrefixProvider* dpp) 
{
  return std::make_unique<RedisCacheAioRequest>(this);  
}
#endif
rgw::AioResultList RedisDriver::get_async(const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) 
{
  rgw_raw_obj r_obj;
  r_obj.oid = key;

  return aio->get(r_obj, rgw::Aio::cache_read_op(dpp, y, this, ofs, len, key), cost, id);
}

int RedisDriver::AsyncReadOp::init(const DoutPrefixProvider *dpp, CephContext* cct, const std::string& file_path, off_t read_ofs, off_t read_len, void* arg)
{
  ldpp_dout(dpp, 20) << "RedisCache: " << __func__ << "(): file_path=" << file_path << dendl;
  aio_cb.reset(new struct aiocb);
  memset(aio_cb.get(), 0, sizeof(struct aiocb));
  aio_cb->aio_fildes = TEMP_FAILURE_RETRY(::open(file_path.c_str(), O_RDONLY|O_CLOEXEC|O_BINARY));

  if (aio_cb->aio_fildes < 0) {
      int err = errno;
      ldpp_dout(dpp, 1) << "ERROR: RedisCache: " << __func__ << "(): can't open " << file_path << " : " << " error: " << err << dendl;
      return -err;
  }

  if (cct->_conf->rgw_d3n_l1_fadvise != POSIX_FADV_NORMAL) {
      posix_fadvise(aio_cb->aio_fildes, 0, 0, g_conf()->rgw_d3n_l1_fadvise);
  }

  bufferptr bp(read_len);
  aio_cb->aio_buf = bp.c_str();
  result.append(std::move(bp));

  aio_cb->aio_nbytes = read_len;
  aio_cb->aio_offset = read_ofs;
  aio_cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  aio_cb->aio_sigevent.sigev_notify_function = libaio_cb_aio_dispatch;
  aio_cb->aio_sigevent.sigev_notify_attributes = nullptr;
  aio_cb->aio_sigevent.sigev_value.sival_ptr = arg;

  return 0;
}

void RedisDriver::AsyncReadOp::libaio_cb_aio_dispatch(sigval sigval)
{
  auto p = std::unique_ptr<Completion>{static_cast<Completion*>(sigval.sival_ptr)};
  auto op = std::move(p->user_data);
  const int ret = -aio_error(op.aio_cb.get());
  boost::system::error_code ec;
  if (ret < 0) {
      ec.assign(-ret, boost::system::system_category());
  }

  ceph::async::dispatch(std::move(p), ec, std::move(op.result));
}

template <typename Executor1, typename CompletionHandler>
auto RedisDriver::AsyncReadOp::create(const Executor1& ex1, CompletionHandler&& handler)
{
  auto p = Completion::create(ex1, std::move(handler));
  return p;
}

template <typename ExecutionContext, typename CompletionToken>
auto RedisDriver::get_async(const DoutPrefixProvider *dpp, ExecutionContext& ctx, const std::string& key,
                off_t read_ofs, off_t read_len, CompletionToken&& token)
{
  std::string location = partition_info.location + key;
  ldpp_dout(dpp, 20) << "RedisCache: " << __func__ << "(): location=" << location << dendl;

  using Op = AsyncReadOp;
  using Signature = typename Op::Signature;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto p = Op::create(ctx.get_executor(), init.completion_handler);
  auto& op = p->user_data;

  int ret = op.init(dpp, cct, location, read_ofs, read_len, p.get());
  if (0 == ret) {
    ret = ::aio_read(op.aio_cb.get());
  }
//  ldpp_dout(dpp, 20) << "SSDCache: " << __func__ << "(): ::aio_read(), ret=" << ret << dendl;
 /* if(ret < 0) {
      auto ec = boost::system::error_code{-ret, boost::system::system_category()};
      ceph::async::post(std::move(p), ec, bufferlist{});
  } else {
      (void)p.release();
  }*/
  //return init.result.get();
}
#if 0
void RedisCacheAioRequest::cache_aio_read(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, off_t ofs, uint64_t len, rgw::Aio* aio, rgw::AioResult& r)
{
  using namespace boost::asio;
  async_completion<yield_context, void()> init(y.get_yield_context());
  auto ex = get_associated_executor(init.completion_handler);

  ldpp_dout(dpp, 20) << "RedisCache: " << __func__ << "(): key=" << key << dendl;
  cache_driver->get_async(dpp, y.get_io_context(), key, ofs, len, bind_executor(ex, RedisDriver::libaio_handler{aio, r}));
}

void RedisCacheAioRequest::cache_aio_write(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, bufferlist& bl, uint64_t len, rgw::Aio* aio, rgw::AioResult& r)
{
}
#endif
int RedisDriver::put_async(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs)
{
  return 0;
}
} } // namespace rgw::cache
