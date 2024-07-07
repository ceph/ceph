#include "rgw_redis_queue.h"

namespace rgw {
namespace redisqueue {

int initQueue(boost::asio::io_context& io, connection* conn, config* cfg,
              optional_yield y) {
  return rgw::redis::loadLuaFunctions(io, conn, cfg, y);
}

int queueStatus(connection* conn, const std::string& name, int& res,
                optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "queue_status", 1, name);
  int ret = rgw::redis::doRedisFunc(conn, req, resp, y);
  if (ret == 0) {
    res = std::get<0>(resp).value();
  }
  return ret;
}

int reserve(connection* conn, const std::string name, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "reserve", 1, name);
  return rgw::redis::doRedisFunc(conn, req, resp, y);
}

int commit(connection* conn, const std::string& name, const std::string& data,
           optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "commit", 1, name, data);
  return rgw::redis::doRedisFunc(conn, req, resp, y);
}

int abort(connection* conn, const std::string& name, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "abort", 1, name);
  return rgw::redis::doRedisFunc(conn, req, resp, y);
}

int read(connection* conn, const std::string& name, int& res,
         optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "read", 1, name);
  int ret = rgw::redis::doRedisFunc(conn, req, resp, y);
  if (ret == 0) {
    res = std::get<0>(resp).value();
  }
  return ret;
}

int locked_read(connection* conn, const std::string& name, int& res,
                std::string& lock_cookie, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "locked_read", 1, name, lock_cookie);
  int ret = rgw::redis::doRedisFunc(conn, req, resp, y);
  if (ret == 0) {
    res = std::get<0>(resp).value();
  }
  return ret;
}

}  // namespace redisqueue
}  // namespace rgw