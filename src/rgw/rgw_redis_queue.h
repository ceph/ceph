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

int queue_init(const DoutPrefixProvider* dpp, connection* conn,
               const std::string& name, uint64_t size, optional_yield y);

int queue_remove(const DoutPrefixProvider* dpp, connection* conn,
                 const std::string& name, optional_yield y);

int parse_read_result(const DoutPrefixProvider* dpp, const std::string& data,
                      std::vector<rgw_queue_entry>& entries, bool* truncated);

int parse_reserve_result(const DoutPrefixProvider* dpp, const std::string& data,
                         std::uint32_t& res_id);

int queue_status(const DoutPrefixProvider* dpp, connection* conn,
                 const std::string& name, std::tuple<uint32_t, uint32_t>& res,
                 optional_yield y);

int queue_stats(const DoutPrefixProvider* dpp, connection* conn,
                const std::string& name, std::tuple<uint64_t, uint32_t>& res,
                optional_yield y);

int reserve(const DoutPrefixProvider* dpp, connection* conn,
            const std::string name, const std::size_t reserve_size,
            std::string& res, optional_yield y);

int commit(const DoutPrefixProvider* dpp, connection* conn,
           const std::string& name, const std::string& data, optional_yield y);

int abort(const DoutPrefixProvider* dpp, connection* conn,
          const std::string& name, const std::uint32_t& res_id,
          optional_yield y);

int read(const DoutPrefixProvider* dpp, connection* conn,
         const std::string& name, std::string& res, optional_yield y);

int locked_read(const DoutPrefixProvider* dpp, connection* conn,
                const std::string& name, const std::string& lock_cookie,
                std::string& res, optional_yield y);

int locked_read(const DoutPrefixProvider* dpp, connection* conn,
                const std::string& name, const std::string& lock_cookie,
                std::string& res, const int count, optional_yield y);

int ack(const DoutPrefixProvider* dpp, connection* conn,
        const std::string& name, optional_yield y);

int locked_ack(const DoutPrefixProvider* dpp, connection* conn,
               const std::string& name, const std::string& lock_cookie,
               optional_yield y);

int locked_ack(const DoutPrefixProvider* dpp, connection* conn,
               const std::string& name, const std::string& lock_cookie,
               const int count, optional_yield y);

int cleanup_stale_reservations(const DoutPrefixProvider* dpp, connection* conn,
                               const std::string& name, int stale_timeout,
                               optional_yield y);

}  // namespace redisqueue
}  // namespace rgw
