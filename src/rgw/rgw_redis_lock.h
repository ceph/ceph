#pragma once

#include <memory>

#include "rgw_redis_common.h"

namespace rgw {
namespace redislock {

using boost::redis::config;
using boost::redis::connection;

int lock(connection* conn, const std::string& name, const std::string& cookie,
         int duration, optional_yield y);
int unlock(connection* conn, const std::string& name, const std::string& cookie,
           optional_yield y);
int assert_locked(connection* conn, const std::string& name,
                  const std::string& cookie, optional_yield y);

}  // namespace redislock
}  // namespace rgw
