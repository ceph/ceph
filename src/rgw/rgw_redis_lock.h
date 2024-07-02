#pragma once

#include "boost/redis/connection.hpp"
#include "common/async/yield_context.h"

namespace rgw {
namespace redislock {

using boost::redis::config;
using boost::redis::connection;

// Needed if we implement tags and description for the lock
// struct redis_lock {
//   std::string name;
//   std::string cookie;
//   std::string description;
//   std::string tag;
// };

int initLock(boost::asio::io_context& io, connection* conn, config* cfg,
             optional_yield y);
int lock(connection* conn, const std::string name, const std::string cookie,
         const int duration, optional_yield y);
int unlock(connection* conn, const std::string& name, const std::string& cookie,
           optional_yield y);
int assert_locked(connection* conn, const std::string& name,
                  const std::string& cookie, optional_yield y);
// int get_lock_info(connection* conn, const std::string& name,
//                   const redis_lock& lock, optional_yield y);
}  // namespace redislock
}  // namespace rgw