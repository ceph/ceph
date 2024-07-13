#include "rgw_redis_common.h"

#include <iostream>

#include "rgw_redis_scripts.h"

namespace rgw {
namespace redis {

using boost::redis::config;
using boost::redis::connection;

int loadLuaFunctions(boost::asio::io_context& io, connection* conn, config* cfg,
                     optional_yield y) {
  conn->async_run(*cfg, {}, boost::asio::detached);

  boost::redis::request req;
  boost::redis::response<std::string> resp;
  boost::system::error_code ec;

  req.push("FUNCTION", "LOAD", "REPLACE", RGW_LUA_SCRIPT);
  rgw::redis::redis_exec(conn, ec, req, resp, y);

  if (ec) {
    std::cerr << "EC Message: " << ec.message() << std::endl;
    return ec.value();
  }
  if (std::get<0>(resp).value() != "rgwlib") return -EINVAL;

  return 0;
}

}  // namespace redis
}  // namespace rgw