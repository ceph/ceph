#include "rgw_redis_lock.h"

namespace rgw {
namespace redislock {

int lock(const DoutPrefixProvider* dpp, connection* conn,
         const std::string& name, const std::string& cookie, int duration,
         optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  std::string expiration_time = std::to_string(duration);
  req.push("FCALL", "lock", 1, name, cookie, expiration_time);
  return rgw::redis::do_redis_func(dpp, conn, req, resp, __func__, y).errorCode;
}

int unlock(const DoutPrefixProvider* dpp, connection* conn,
           const std::string& name, const std::string& cookie,
           optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "unlock", 1, name, cookie);
  return rgw::redis::do_redis_func(dpp, conn, req, resp, __func__, y).errorCode;
}

int assert_locked(const DoutPrefixProvider* dpp, connection* conn,
                  const std::string& name, const std::string& cookie,
                  optional_yield y) {
  boost::redis::request req;
  rgw::redis::RedisResponseMap resp;

  req.push("FCALL", "assert_lock", 1, name, cookie);
  return rgw::redis::do_redis_func(dpp, conn, req, resp, __func__, y).errorCode;
}

}  // namespace redislock
}  // namespace rgw
