#include "rgw_redis_queue.h"

namespace rgw {
namespace redisqueue {

int initQueue(boost::asio::io_context& io, connection* conn, config* cfg,
              optional_yield y) {
  return rgw::redis::loadLuaFunctions(io, conn, cfg, y);
}

// FIXME: Perhaps return the queue length in calls to reserve, commit, abort
// etc and do not use this function explicitly?
int queueStatus(connection* conn, const std::string& name,
                std::tuple<int, int>& res, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int, int> resp;
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

int reserve(connection* conn, const std::string name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  int reserveSize = 120;
  req.push("FCALL", "reserve", 1, name, reserveSize);
  return rgw::redis::doRedisFunc(conn, req, resp, __func__, y).errorCode;
}

int commit(connection* conn, const std::string& name, const std::string& data,
           optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "commit", 1, name, data);
  return rgw::redis::doRedisFunc(conn, req, resp, __func__, y).errorCode;
}

int abort(connection* conn, const std::string& name, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "abort", 1, name);
  return rgw::redis::doRedisFunc(conn, req, resp, __func__, y).errorCode;
}

int read(connection* conn, const std::string& name, std::string& res,
         optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "read", 1, name);
  rgw::redis::RedisResponse ret =
      rgw::redis::doRedisFunc(conn, req, resp, __func__, y);
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
                std::string& lock_cookie, std::string& res, optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "locked_read", 1, name, lock_cookie);
  rgw::redis::RedisResponse ret =
      rgw::redis::doRedisFunc(conn, req, resp, __func__, y);
  if (ret.errorCode == 0) {
    res = ret.data;
  } else {
    std::cerr << "RGW Redis Queue:: " << __func__
              << "(): ERROR: " << ret.errorMessage << std::endl;
    res = "";
  }

  return ret.errorCode;
}

  int ret = rgw::redis::doRedisFunc(conn, req, resp, __func__, y).errorCode;
  // if (ret == 0) {
  //   res = std::get<0>(resp).value();
  // }
  return ret;
}

}  // namespace redisqueue
}  // namespace rgw