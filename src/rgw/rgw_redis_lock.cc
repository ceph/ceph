#include "rgw_redis_lock.h"

namespace rgw {
namespace redislock {

int initLock(boost::asio::io_context& io, connection* conn, config* cfg,
             optional_yield y) {
  return rgw::redis::loadLuaFunctions(io, conn, cfg, y);
}

int lock(connection* conn, const std::string name, const std::string cookie,
         const int duration, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  std::string expiration_time = std::to_string(duration);
  req.push("FCALL", "lock", 1, name, cookie, expiration_time);
  return rgw::redis::doRedisFunc(conn, req, resp, y);
}

int unlock(connection* conn, const std::string& name, const std::string& cookie,
           optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "unlock", 1, name, cookie);
  return rgw::redis::doRedisFunc(conn, req, resp, y);
}

int assert_locked(connection* conn, const std::string& name,
                  const std::string& cookie, optional_yield y) {
  boost::redis::request req;
  boost::redis::response<int> resp;

  req.push("FCALL", "assert_lock", 1, name, cookie);
  return rgw::redis::doRedisFunc(conn, req, resp, y);
}

}  // namespace redislock
}  // namespace rgw
