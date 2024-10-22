#pragma once

#include <memory>

#include "rgw_redis_common.h"

namespace rgw {
namespace redisqueue {

using boost::redis::config;
using boost::redis::connection;

struct rgw_queue_entry {
  std::string marker;
  std::string data;
};

int queue_init(connection* conn, const std::string& name, uint64_t size,
               optional_yield y);

int redis_queue_parse_result(const std::string& data,
                             std::vector<rgw_queue_entry>& entries,
                             bool* truncated);

int queue_status(connection* conn, const std::string& name,
                 std::tuple<int, int>& res, optional_yield y);

int queue_stats(connection* conn, const std::string& name,
                std::tuple<uint64_t, uint32_t>& res, optional_yield y);

int reserve(connection* conn, const std::string name, const std::size_t reserve_size, optional_yield y);

int commit(connection* conn, const std::string& name, const std::string& data,
           optional_yield y);

int abort(connection* conn, const std::string& name, optional_yield y);

int read(connection* conn, const std::string& name, std::string& res,
         optional_yield y);

int locked_read(connection* conn, const std::string& name,
                const std::string& lock_cookie, std::string& res,
                optional_yield y);

int locked_read(connection* conn, const std::string& name,
                const std::string& lock_cookie, std::string& res,
                const int count, optional_yield y);

int ack(connection* conn, const std::string& name, optional_yield y);

int locked_ack(connection* conn, const std::string& name,
               const std::string& lock_cookie, optional_yield y);

int locked_ack(connection* conn, const std::string& name,
               const std::string& lock_cookie, const int count,
               optional_yield y);

int cleanup_stale_reservations(connection* conn, const std::string& name,
                               int stale_timeout, optional_yield y);

}  // namespace redisqueue
}  // namespace rgw
