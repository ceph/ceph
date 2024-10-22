#include "rgw_redis_queue.h"

namespace rgw {
namespace redisqueue {

// Add the queue to the hashmap of 2pc_queues
int queue_init(connection* conn, const std::string& name, uint64_t size,
               optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;
  boost::system::error_code ec;

  std::string HASHMAP_NAME = "2pc_queues";

  try {
    req.push("HSET", HASHMAP_NAME, name, std::to_string(size));
    rgw::redis::redis_exec(conn, ec, req, resp, y);
    if (ec) {
      std::cerr << "RGW Redis Queue:: " << __func__
                << "(): ERROR: " << ec.message() << std::endl;
      return -ec.value();
    }
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): Exception: " << e.what() << std::endl;
    return -EINVAL;
  }
}

// FIXME: Perhaps return the queue length in calls to reserve, commit, abort
// etc and do not use this function explicitly?
int queue_status(connection* conn, const std::string& name,
                 std::tuple<uint32_t, uint32_t>& res, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<uint32_t, uint32_t> resp;
  boost::system::error_code ec;

  try {
    req.push("LLEN", "reserve:" + name);
    req.push("LLEN", "queue:" + name);

    rgw::redis::redis_exec(conn, ec, req, resp, y);
    if (ec) {
      std::cerr << "RGW Redis Queue:: " << __func__
                << "(): ERROR: " << ec.message() << std::endl;
      return -ec.value();
    }
    res = std::make_tuple(std::get<0>(resp).value(), std::get<1>(resp).value());
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): Exception: " << e.what() << std::endl;
    return -EINVAL;
  }
}

int queue_stats(connection* conn, const std::string& name,
                std::tuple<uint64_t, uint32_t>& res, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<std::optional<uint64_t>, uint32_t> resp;
  boost::system::error_code ec;

  try {
    req.push("MEMORY", "USAGE", "reserve:" + name);
    req.push("LLEN", "reserve:" + name);

    rgw::redis::redis_exec(conn, ec, req, resp, y);
    if (ec) {
      std::cerr << "RGW Redis Queue:: " << __func__
                << "(): ERROR: " << ec.message() << std::endl;
      return -ec.value();
    }
    uint64_t reserveSize;
    try {
      reserveSize = std::get<0>(resp).value().value();
    } catch (const std::bad_optional_access& e) {
      // Empty queue
      reserveSize = 0;
    }
    res = std::make_tuple(reserveSize, std::get<1>(resp).value());
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): Exception: " << e.what() << std::endl;
    return -EINVAL;
  }
}

int reserve(connection* conn, const std::string name,
            const std::size_t reserve_size, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "reserve", 1, name, reserve_size);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int commit(connection* conn, const std::string& name, const std::string& data,
           optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "commit", 1, name, data);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int abort(connection* conn, const std::string& name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "abort", 1, name);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int read(connection* conn, const std::string& name, std::string& res,
         optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "read", 1, name);
  rgw::redis::RedisResponse ret =
      rgw::redis::do_redis_func(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    res = ret.data;
  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
    res = "";
  }

  return ret.errorCode;
}

int locked_read(connection* conn, const std::string& name,
                const std::string& lock_cookie, std::string& res,
                optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_read", 1, name, lock_cookie);
  rgw::redis::RedisResponse ret =
      rgw::redis::do_redis_func(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    res = ret.data;
  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
    res = "";
  }

  return ret.errorCode;
}

int locked_read(connection* conn, const std::string& name,
                const std::string& lock_cookie, std::string& res,
                const int count, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_read_multi", 1, name, lock_cookie, count);
  rgw::redis::RedisResponse ret =
      rgw::redis::do_redis_func(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    res = ret.data;
  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
  }

  return ret.errorCode;
}

int redis_queue_parse_result(const std::string& data,
                             std::vector<rgw_queue_entry>& entries,
                             bool* truncated) {
  // Parse the JSON data
  boost::json::value v = boost::json::parse(data);

  if (!v.is_object()) {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: JSON value is not an object" << std::endl;
    return -EINVAL;
  }

  boost::json::object jdata = v.as_object();
  boost::json::array jentries = jdata.at("values").as_array();
  *truncated = jdata.at("isTruncated").as_bool();

  for (std::size_t i = 0; i < jentries.size(); i++) {
    rgw_queue_entry entry;
    entry.marker = std::to_string(i);
    entry.data = jentries[i].as_string().c_str();
    entries.push_back(entry);
  }

  return 0;
}

int ack(connection* conn, const std::string& name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "ack", 1, name);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int locked_ack(connection* conn, const std::string& name,
               const std::string& lock_cookie, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_ack", 1, name, lock_cookie);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int locked_ack(connection* conn, const std::string& name,
               const std::string& lock_cookie, const int count,
               optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_ack_multi", 1, name, lock_cookie, count);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

int cleanup_stale_reservations(connection* conn, const std::string& name,
                               int stale_timeout, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "cleanup", 1, name, stale_timeout);
  return rgw::redis::do_redis_func(conn, req, resp, __func__, y).errorCode;
}

}  // namespace redisqueue
}  // namespace rgw
