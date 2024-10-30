#include "rgw_redis_common.h"

#include <iostream>

#include "rgw_redis_scripts.h"

namespace rgw {
namespace redis {

using boost::redis::config;
using boost::redis::connection;

int load_lua_rgwlib(boost::asio::io_context& io, connection* conn, config* cfg,
                    optional_yield y) {
  conn->async_run(*cfg, {}, boost::asio::detached);

  boost::redis::request req;
  boost::redis::response<std::string> resp;
  boost::system::error_code ec;

  req.push("FUNCTION", "LOAD", "REPLACE", RGW_LUA_SCRIPT);
  rgw::redis::redis_exec(conn, ec, req, resp, y);

  if (ec) {
    return ec.value();
  }
  if (std::get<0>(resp).value() != "rgwlib") return -EINVAL;
  return 0;
}

RedisResponse do_redis_func(const DoutPrefixProvider* dpp, connection* conn,
                            boost::redis::request& req, RedisResponseMap& resp,
                            std::string func_name, optional_yield y) {
  try {
    boost::system::error_code ec;
    rgw::redis::redis_exec(conn, ec, req, resp, y);
    if (ec) {
      ldpp_dout(dpp, 1) << "RGW RedisLock:: " << func_name
                        << "(): ERROR: " << ec.message() << dendl;
      return RedisResponse(-ec.value(), ec.message());
    }
    return RedisResponse(resp);

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 1) << "RGW RedisLock:: " << func_name
                      << "(): Exception: " << e.what() << dendl;
    return RedisResponse(-EINVAL, e.what());
  }
}

RGWRedis::RGWRedis(boost::asio::io_context& io)
    : io(io),
      conn(std::make_unique<connection>(io)),
      cfg(std::make_unique<config>()) {}

RGWRedis::~RGWRedis() = default;

connection* RGWRedis::get_conn() { return conn.get(); }
config* RGWRedis::get_cfg() { return cfg.get(); }

}  // namespace redis
}  // namespace rgw
