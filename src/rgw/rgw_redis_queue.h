#pragma once

#include <memory>

#include "rgw_redis_common.h"

namespace rgw {
namespace redisqueue {

using boost::redis::config;
using boost::redis::connection;

int queue_status(std::unique_ptr<connection>& conn, const std::string& name,
                 std::tuple<int, int>& res, optional_yield y);

int reserve(std::unique_ptr<connection>& conn, const std::string name,
            optional_yield y);

int commit(std::unique_ptr<connection>& conn, const std::string& name,
           const std::string& data, optional_yield y);

int abort(std::unique_ptr<connection>& conn, const std::string& name,
          optional_yield y);

int read(std::unique_ptr<connection>& conn, const std::string& name,
         std::string& res, optional_yield y);

int locked_read(std::unique_ptr<connection>& conn, const std::string& name,
                std::string& lock_cookie, std::string& res, optional_yield y);

int ack_read(std::unique_ptr<connection>& conn, const std::string& name,
             const std::string& lock_cookie, optional_yield y);

int cleanup(std::unique_ptr<connection>& conn, const std::string& name,
            optional_yield y);

}  // namespace redisqueue
}  // namespace rgw
