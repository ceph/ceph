#include <algorithm>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_redis.h"

namespace rgw::d4n {

// initiate a call to async_exec() on the connection's executor
struct initiate_exec {
  std::shared_ptr<boost::redis::connection> conn;

  using executor_type = boost::redis::connection::executor_type;
  executor_type get_executor() const noexcept { return conn->get_executor(); }

  template <typename Handler, typename Response>
  void operator()(Handler handler, const boost::redis::request& req, Response& resp)
  {
    auto h = boost::asio::consign(std::move(handler), conn);
    return boost::asio::dispatch(get_executor(),
        [c = conn, &req, &resp, h = std::move(h)] () mutable {
            return c->async_exec(req, resp, std::move(h));
    });
  }
};

template <typename Response, typename CompletionToken>
auto async_exec(std::shared_ptr<boost::redis::connection> conn,
                const boost::redis::request& req,
                Response& resp, CompletionToken&& token)
{
  return boost::asio::async_initiate<CompletionToken,
         void(boost::system::error_code, std::size_t)>(
      initiate_exec{std::move(conn)}, token, req, resp);
}

template <typename... Types>
void redis_exec(std::shared_ptr<boost::redis::connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

template <typename... Types>
void redis_exec_cp(const DoutPrefixProvider* dpp,
                std::shared_ptr<rgw::d4n::RedisPool> pool,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::response<Types...>& resp,
		optional_yield y)
{
//purpose: Execute a Redis command using a connection from the pool
	std::shared_ptr<boost::redis::connection> conn = pool->acquire(dpp);
	try {

  		if (y) {
    		auto yield = y.get_yield_context();
    		async_exec(conn, req, resp, yield[ec]);
  		} else {
    		async_exec(conn, req, resp, ceph::async::use_blocked[ec]);
  		}
	} catch (const std::exception& e) {
		//release the connection upon exception
    		pool->release(conn);
    		throw;
	}
	//release the connection back to the pool after execution
	pool->release(conn);
}

void redis_exec(std::shared_ptr<boost::redis::connection> conn,
                boost::system::error_code& ec,
                const boost::redis::request& req,
    boost::redis::generic_response& resp, optional_yield y)
{
  if (y) {
    auto yield = y.get_yield_context();
    async_exec(std::move(conn), req, resp, yield[ec]);
  } else {
    async_exec(std::move(conn), req, resp, ceph::async::use_blocked[ec]);
  }
}

void redis_exec_cp(const DoutPrefixProvider* dpp,
                std::shared_ptr<rgw::d4n::RedisPool> pool,
                boost::system::error_code& ec,
                const boost::redis::request& req,
                boost::redis::generic_response& resp, optional_yield y)
{
	//purpose: Execute a Redis command using a connection from the pool
	std::shared_ptr<boost::redis::connection> conn = pool->acquire(dpp);

	try {
  		if (y) {
    			auto yield = y.get_yield_context();
    			async_exec(conn, req, resp, yield[ec]);
  		} else {
    			async_exec(conn, req, resp, ceph::async::use_blocked[ec]);
  		}	
	} catch (const std::exception& e) {
    			pool->release(conn);
    			throw;
	}
	//release the connection back to the pool after execution
	pool->release(conn);
}

void redis_exec_connection_pool(const DoutPrefixProvider* dpp,
				std::shared_ptr<RedisPool> redis_pool,
				std::shared_ptr<boost::redis::connection> conn,
				boost::system::error_code& ec,
				const boost::redis::request& req,
				boost::redis::generic_response& resp,
				optional_yield y)
{
    if(!redis_pool)[[unlikely]]
    {
	redis_exec(conn, ec, req, resp, y);
	ldpp_dout(dpp, 0) << "Directory::" << __func__ << " not using connection-pool, it's using the shared connection " << dendl;
    }
    else[[likely]]
    	redis_exec_cp(dpp, redis_pool, ec, req, resp, y);
}

template <typename... Types>
void redis_exec_connection_pool(const DoutPrefixProvider* dpp,
				std::shared_ptr<RedisPool> redis_pool,
				std::shared_ptr<boost::redis::connection> conn,
				boost::system::error_code& ec,
				const boost::redis::request& req,
				boost::redis::response<Types...>& resp,
				optional_yield y)
{
    if(!redis_pool)[[unlikely]]
    {
	redis_exec(conn, ec, req, resp, y);
	ldpp_dout(dpp, 0) << "Directory::" << __func__ << " not using connection-pool, it's using the shared connection " << dendl;
    }
    else[[likely]]
    	redis_exec_cp(dpp, redis_pool, ec, req, resp, y);
}

// RedisLease implementation

namespace {

constexpr std::string_view lease_prefix{"d4n:leases:"};

// Helper: Convert nanoseconds to milliseconds for Redis PEXPIRE
uint64_t nanoseconds_to_milliseconds(uint64_t nanoseconds) {
  // Convert nanoseconds to milliseconds, round up to avoid zero TTL
  uint64_t milliseconds = (nanoseconds + 999999ULL) / 1000000ULL;
  return std::max<uint64_t>(milliseconds, 1);  // Ensure at least 1ms
}

// Lease metadata structure
struct LeaseData {
  uint64_t expiry = 0;          // Expiry in nanoseconds since epoch
  std::string holder_id;
  std::string token;
  uint64_t tick_count = 0;
  std::string last_renewal_id;  // ID of last renewal - for replay detection

  // Serialize to format: "expiry|holder_id|token|tick_count|last_renewal_id"
  std::string serialize() const {
    return std::to_string(expiry) + "|" +
           url_encode(holder_id, true) + "|" +
           url_encode(token, true) + "|" +
           std::to_string(tick_count) + "|" +
           url_encode(last_renewal_id, true);
  }

  // Deserialize from format: "expiry|holder_id|token|tick_count|last_renewal_id"
  static bool deserialize(const std::string& value, LeaseData& data) {
    std::vector<std::string> parts;
    boost::split(parts, value, boost::is_any_of("|"));

    // Support old formats: 3 parts (no tick), 4 parts (no renewal_id), 5 parts (current)
    if (parts.size() < 3 || parts.size() > 5) {
      return false;
    }

    try {
      data.expiry = std::stoull(parts[0]);
    } catch (...) {
      return false;
    }

    data.holder_id = url_decode(parts[1]);
    data.token = url_decode(parts[2]);

    // Parse tick_count if present (parts[3])
    if (parts.size() >= 4) {
      try {
        data.tick_count = std::stoull(parts[3]);
      } catch (...) {
        data.tick_count = 0;
      }
    } else {
      data.tick_count = 0;
    }

    // Parse last_renewal_id if present (parts[4])
    if (parts.size() >= 5) {
      data.last_renewal_id = url_decode(parts[4]);
    } else {
      data.last_renewal_id = "";
    }

    return true;
  }

  bool is_active(uint64_t now) const {
    return expiry > now;
  }
};

std::string make_lease_key(const std::string& resource_name)
{
  return std::string{lease_prefix} + resource_name;
}

uint64_t current_time_nanoseconds()
{
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return static_cast<uint64_t>(std::max<int64_t>(now, 0));
}

} // anonymous namespace

int RedisLease::acquire(const DoutPrefixProvider* dpp,
                               const std::string& resource_name,
                               const std::string& holder_id,
                               const std::string& token,
                               uint64_t ttl_nanoseconds)
{
  if (ttl_nanoseconds == 0) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " invalid TTL=0 for resource=" << resource_name << dendl;
    return -EINVAL;
  }

  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  const auto redis_key = make_lease_key(resource_name);
  LeaseData new_lease{
    .expiry = current_time_nanoseconds() + ttl_nanoseconds,
    .holder_id = holder_id,
    .token = token,
    .tick_count = 0,
    .last_renewal_id = ""  // No renewals yet
  };

  // Convert nanoseconds to milliseconds for Redis (PX option)
  const uint64_t ttl_ms = nanoseconds_to_milliseconds(ttl_nanoseconds);

  response<std::optional<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;
    // SET key value NX PX ttl_ms
    // NX = only set if key doesn't exist
    // PX = expire in ttl_ms milliseconds (better precision than EX seconds)
    req.push("SET", redis_key, new_lease.serialize(), "NX", "PX", std::to_string(ttl_ms));

    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, ec, req, resp, null_yield);
    } else {
      redis_exec(REDISconn, ec, req, resp, null_yield);
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " Redis error for resource=" << resource_name
                        << ": " << ec.message() << dendl;
      return -EIO;
    }

    const auto& result = std::get<0>(resp).value();
    if (!result.has_value() || result->empty()) {
      // SET NX failed - resource already has an active lease
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " resource already leased: " << resource_name << dendl;
      return -EBUSY;
    }

    ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                       << " acquired lease: resource=" << resource_name
                       << " holder=" << holder_id
                       << " expiry_ns=" << new_lease.expiry << dendl;
    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                      << " exception for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return -EIO;
  }
}

int RedisLease::renew(const DoutPrefixProvider* dpp,
                             const std::string& resource_name,
                             const std::string& holder_id,
                             const std::string& token,
                             uint64_t ttl_nanoseconds,
                             uint64_t max_ticks)
{
  if (ttl_nanoseconds == 0) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " invalid TTL=0 for resource=" << resource_name << dendl;
    return -EINVAL;
  }

  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  // Generate unique renewal_id BEFORE any Redis operations
  // This ensures the same ID is used if we retry
  const uint64_t renewal_timestamp = current_time_nanoseconds();
  const std::string renewal_id = std::to_string(renewal_timestamp) + ":" +
                                  holder_id + ":" +
                                  std::to_string(std::hash<std::string>{}(token));

  const auto redis_key = make_lease_key(resource_name);

  response<std::optional<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;

    // Get current lease data
    req.push("GET", redis_key);

    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, ec, req, resp, null_yield);
    } else {
      redis_exec(REDISconn, ec, req, resp, null_yield);
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " Redis error for resource=" << resource_name
                        << ": " << ec.message() << dendl;
      return -EIO;
    }

    const auto& get_result = std::get<0>(resp).value();
    if (!get_result.has_value() || get_result->empty()) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " lease not found: resource=" << resource_name << dendl;
      return -ENOENT;
    }

    // Parse existing lease
    LeaseData existing_lease;
    if (!LeaseData::deserialize(*get_result, existing_lease)) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " failed to parse lease data for resource=" << resource_name << dendl;
      return -EINVAL;
    }

    // REPLAY/RETRY DETECTION: Check if this exact renewal was already applied
    if (existing_lease.last_renewal_id == renewal_id) {
      ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                         << " renewal already applied (retry detected)"
                         << " renewal_id=" << renewal_id
                         << " resource=" << resource_name << dendl;
      return 0;  // Idempotent - this renewal already happened
    }

    // Check if lease is still active
    const auto now = current_time_nanoseconds();
    if (!existing_lease.is_active(now)) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " lease expired: resource=" << resource_name << dendl;
      // Clean up expired lease
      request del_req;
      del_req.push("DEL", redis_key);
      response<int> del_resp;
      boost::system::error_code del_ec;
      if (redis_pool) {
        redis_exec_cp(dpp, redis_pool, del_ec, del_req, del_resp, null_yield);
      } else {
        redis_exec(REDISconn, del_ec, del_req, del_resp, null_yield);
      }
      return -ENOENT;
    }

    // Validate ownership
    if (existing_lease.holder_id != holder_id || existing_lease.token != token) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " ownership validation failed: resource=" << resource_name
                         << " expected_holder=" << existing_lease.holder_id
                         << " provided_holder=" << holder_id << dendl;
      return -EACCES;
    }

    // Check if max_ticks limit reached
    if (max_ticks > 0 && existing_lease.tick_count >= max_ticks) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " max_ticks limit reached: resource=" << resource_name
                         << " tick_count=" << existing_lease.tick_count
                         << " max_ticks=" << max_ticks << dendl;
      return -EINVAL;  // Lease still held - caller decides what to do
    }

    // Renew the lease with incremented tick count and this renewal_id
    LeaseData renewed_lease{
      .expiry = current_time_nanoseconds() + ttl_nanoseconds,
      .holder_id = holder_id,
      .token = token,
      .tick_count = existing_lease.tick_count + 1,
      .last_renewal_id = renewal_id  // Mark this renewal as applied
    };

    // Convert nanoseconds to milliseconds for Redis (PX option)
    const uint64_t ttl_ms = nanoseconds_to_milliseconds(ttl_nanoseconds);

    request set_req;
    set_req.push("SET", redis_key, renewed_lease.serialize(), "PX", std::to_string(ttl_ms));
    response<std::optional<std::string>> set_resp;
    boost::system::error_code set_ec;
    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, set_ec, set_req, set_resp, null_yield);
    } else {
      redis_exec(REDISconn, set_ec, set_req, set_resp, null_yield);
    }

    if (set_ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " failed to update lease for resource=" << resource_name
                        << ": " << set_ec.message() << dendl;
      return -EIO;
    }

    ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                       << " renewed lease: resource=" << resource_name
                       << " holder=" << holder_id
                       << " new_expiry_ns=" << renewed_lease.expiry
                       << " tick_count=" << renewed_lease.tick_count
                       << " renewal_id=" << renewal_id << dendl;
    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                      << " exception for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return -EIO;
  }
}

int RedisLease::release(const DoutPrefixProvider* dpp,
                               const std::string& resource_name,
                               const std::string& holder_id,
                               const std::string& token)
{
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  const auto redis_key = make_lease_key(resource_name);

  response<std::optional<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;

    // Get current lease data
    req.push("GET", redis_key);

    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, ec, req, resp, null_yield);
    } else {
      redis_exec(REDISconn, ec, req, resp, null_yield);
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " Redis error for resource=" << resource_name
                        << ": " << ec.message() << dendl;
      return -EIO;
    }

    const auto& get_result = std::get<0>(resp).value();
    if (!get_result.has_value() || get_result->empty()) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " lease not found: resource=" << resource_name << dendl;
      return -ENOENT;
    }

    // Parse existing lease
    LeaseData existing_lease;
    if (!LeaseData::deserialize(*get_result, existing_lease)) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " failed to parse lease data for resource=" << resource_name << dendl;
      return -ENOENT;
    }

    // Validate ownership
    if (existing_lease.holder_id != holder_id || existing_lease.token != token) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " ownership validation failed: resource=" << resource_name
                         << " expected_holder=" << existing_lease.holder_id
                         << " provided_holder=" << holder_id << dendl;
      return -EACCES;
    }

    // Release the lease
    request del_req;
    del_req.push("DEL", redis_key);
    response<int> del_resp;
    boost::system::error_code del_ec;
    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, del_ec, del_req, del_resp, null_yield);
    } else {
      redis_exec(REDISconn, del_ec, del_req, del_resp, null_yield);
    }

    if (del_ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " failed to delete lease for resource=" << resource_name
                        << ": " << del_ec.message() << dendl;
      return -EIO;
    }

    ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                       << " released lease: resource=" << resource_name
                       << " holder=" << holder_id << dendl;
    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                      << " exception for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return -EIO;
  }
}

LeaseCheckResult RedisLease::any_active(const DoutPrefixProvider* dpp,
                                        const std::string& resource_prefix)
{
  if (resource_prefix.empty()) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " empty resource_prefix" << dendl;
    return {.active = false, .error = -EINVAL};
  }

  // Build pattern for scanning all leases matching this resource prefix
  const auto pattern = make_lease_key(resource_prefix) + "*";

  response<std::vector<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;
    // Use KEYS pattern to find all matching lease keys
    // Note: KEYS can be slow on large datasets, but lease counts should be small
    req.push("KEYS", pattern);

    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, ec, req, resp, null_yield);
    } else {
      redis_exec(REDISconn, ec, req, resp, null_yield);
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " Redis error for resource_prefix=" << resource_prefix
                        << ": " << ec.message() << dendl;
      return {.active = false, .error = -EIO};
    }

    const auto& keys = std::get<0>(resp).value();
    if (keys.empty()) {
      ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                         << " no leases found for resource prefix: " << resource_prefix << dendl;
      return {.active = false, .error = 0};
    }

    // Check if any of the found keys contain active leases
    // Opportunistically delete expired leases we encounter
    const auto now = current_time_nanoseconds();
    int total_leases = keys.size();
    int active_leases = 0;
    std::vector<std::string> expired_keys;

    for (const auto& lease_key : keys) {
      response<std::optional<std::string>> get_resp;
      request get_req;
      get_req.push("GET", lease_key);
      boost::system::error_code get_ec;

      if (redis_pool) {
        redis_exec_cp(dpp, redis_pool, get_ec, get_req, get_resp, null_yield);
      } else {
        redis_exec(REDISconn, get_ec, get_req, get_resp, null_yield);
      }

      if (!get_ec) {
        const auto& val = std::get<0>(get_resp).value();
        if (val.has_value() && !val->empty()) {
          LeaseData lease;
          if (LeaseData::deserialize(*val, lease)) {
            if (lease.is_active(now)) {
              active_leases++;
              ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                                 << " found active lease: holder=" << lease.holder_id
                                 << " expiry_ns=" << lease.expiry << dendl;
            } else {
              // Lease expired but not yet cleaned up by Redis - delete it
              expired_keys.push_back(lease_key);
            }
          }
        }
      }
    }

    // Opportunistic cleanup: delete expired leases
    if (!expired_keys.empty()) {
      request del_req;
      for (const auto& key : expired_keys) {
        del_req.push("DEL", key);
      }
      response<int> del_resp;
      boost::system::error_code del_ec;
      if (redis_pool) {
        redis_exec_cp(dpp, redis_pool, del_ec, del_req, del_resp, null_yield);
      } else {
        redis_exec(REDISconn, del_ec, del_req, del_resp, null_yield);
      }
      if (!del_ec) {
        ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                           << " cleaned up " << expired_keys.size() << " expired leases" << dendl;
      }
    }

    bool has_active = (active_leases > 0);
    ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                       << " resource_prefix=" << resource_prefix
                       << " total_leases=" << total_leases
                       << " active_leases=" << active_leases
                       << " has_active=" << has_active << dendl;
    return {.active = has_active, .error = 0};

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                      << " exception for resource_prefix=" << resource_prefix
                      << ": " << e.what() << dendl;
    return {.active = false, .error = -EIO};
  }
}

LeaseCheckResult RedisLease::is_active(const DoutPrefixProvider* dpp,
                                       const std::string& resource_name,
                                       const std::string& holder_id,
                                       const std::string& token)
{
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return {.active = false, .error = -EINVAL};
  }

  const auto redis_key = make_lease_key(resource_name);

  response<std::optional<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;
    req.push("GET", redis_key);

    if (redis_pool) {
      redis_exec_cp(dpp, redis_pool, ec, req, resp, null_yield);
    } else {
      redis_exec(REDISconn, ec, req, resp, null_yield);
    }

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                        << " Redis error for resource=" << resource_name
                        << ": " << ec.message() << dendl;
      return {.active = false, .error = -EIO};
    }

    const auto& get_result = std::get<0>(resp).value();
    if (!get_result.has_value() || get_result->empty()) {
      ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                         << " lease not found: resource=" << resource_name << dendl;
      return {.active = false, .error = 0};
    }

    // Parse lease data
    LeaseData lease;
    if (!LeaseData::deserialize(*get_result, lease)) {
      ldpp_dout(dpp, 10) << "RedisLease::" << __func__
                         << " failed to parse lease data for resource=" << resource_name << dendl;
      return {.active = false, .error = -EINVAL};
    }

    // Check expiry
    const auto now = current_time_nanoseconds();
    if (!lease.is_active(now)) {
      ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                         << " lease expired: resource=" << resource_name
                         << " expiry_ns=" << lease.expiry << dendl;
      return {.active = false, .error = 0};
    }

    // Validate ownership
    if (lease.holder_id != holder_id || lease.token != token) {
      ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                         << " ownership mismatch: resource=" << resource_name
                         << " expected_holder=" << lease.holder_id
                         << " provided_holder=" << holder_id << dendl;
      return {.active = false, .error = 0};
    }

    ldpp_dout(dpp, 20) << "RedisLease::" << __func__
                       << " lease is active: resource=" << resource_name
                       << " holder=" << holder_id
                       << " expiry_ns=" << lease.expiry << dendl;
    return {.active = true, .error = 0};

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisLease::" << __func__
                      << " exception for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return {.active = false, .error = -EIO};
  }
}

int RedisTransaction::commit(const DoutPrefixProvider* dpp, optional_yield y){
  int r = execute_request(dpp, y);
  executed_ = true;
  if (pool_) pool_->release(conn_);
  return r;
}

int RedisTransaction::abort(const DoutPrefixProvider* dpp, optional_yield y){
  executed_ = true;   // nothing was sent yet, just drop the buffered request
  if (pool_) pool_->release(conn_);
  return 0;
}

int RedisTransaction::execute_request(const DoutPrefixProvider* dpp, optional_yield y)
{
  boost::redis::generic_response resp;
  try {
    boost::system::error_code ec;
    executed_ = false;
    redis_exec_connection_pool(dpp, pool_, conn_, ec, req_, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisTransaction - Directory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisTransaction - Directory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

int RedisDirectory::prepare_request(const DoutPrefixProvider* dpp,
                                    std::optional<std::reference_wrapper<Transaction>> txn,
                                    request& req,
                                    RedisTransaction*& rtxn,
                                    request*& target)
{
  rtxn = nullptr;

  if (txn) {
    rtxn = dynamic_cast<RedisTransaction*>(&txn->get());
    if (!rtxn) {
      ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                        << "(): transaction is not a RedisTransaction"
                        << dendl;
      return -EINVAL;
    }
    target = &rtxn->get_request();
  } else {
    target = &req;
  }

  return 0;
}

int RedisDirectory::set_kv(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           const std::string& key,
                           const std::string& field,
                           const std::string& val,
                           std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  target->push("HSET", key, field, val);

  if (rtxn) {
    return 0;
  }

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisDirectory::get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val,
                       std::optional<std::reference_wrapper<Transaction>> txn)
{
  response<std::optional<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;
    req.push("HGET", key, field);
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception& e) {
    return -EINVAL;
  }
  const auto& val = std::get<0>(resp).value();
  if (val.has_value() && !val->empty()) {
    out_val = *val;
  }
  return 0;
}

int RedisDirectory::set_kv_multi(const DoutPrefixProvider* dpp,
                                 optional_yield y,
                                 const std::string& key,
                                 const std::map<std::string, std::string>& vals,
                                 std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  target->push_range("HSET", key, vals);

  if (rtxn) {
    return 0;
  }

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisDirectory::get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& key,
                        const std::vector<std::string>& fields,
                        std::map<std::string, std::string>& out_vals,
			std::optional<std::reference_wrapper<Transaction>> txn)
{
  response<std::vector<std::string>> resp;
  try {
    boost::system::error_code ec;
    request req;
    req.push_range("HMGET", key, fields);
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception& e) {
    return -EINVAL;
  }
  const auto& values = std::get<0>(resp).value();
  for (size_t i = 0; i < fields.size() && i < values.size(); ++i) {
    out_vals[fields[i]] = values[i];
  }
  return 0;
}

int RedisDirectory::set_kv_if_not_exists(const DoutPrefixProvider* dpp,
                                         optional_yield y,
                                         const std::string& key,
                                         const std::string& field,
                                         const std::string& val,
                                         std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  target->push("HSETNX", key, field, val);

  if (rtxn) {
    return 0;
  }

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisDirectory::" << __func__
                      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBucketDirectory::zadd(const DoutPrefixProvider* dpp, optional_yield y,
                               const std::string& bucket_id,
                               double score,
                               const std::string& member,
                               std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZADD", bucket_id, "CH", std::to_string(score), member);

    if (rtxn) {
      return 0;
    }

    boost::system::error_code ec;
    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 10) << "RedisBucketDirectory::" << __func__
                         << "() Response value is: "
                         << std::get<0>(resp).value() << dendl;
      return -ENOENT;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__
                      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBucketDirectory::zrem(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& member,
                                std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZREM", bucket_id, member);

    if (rtxn) {
      return 0;
    }

    boost::system::error_code ec;
    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__
                        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 10) << "RedisBucketDirectory::" << __func__
                         << "() Response is: "
                         << std::get<0>(resp).value() << dendl;
      return -ENOENT;
    }
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__
                      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBucketDirectory::zrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& members, std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {

    if (offset == 0 && count == 0) {
      target->push("ZRANGE", bucket_id, start, stop, "bylex");
    } else {
      target->push("ZRANGE", bucket_id, start, stop, "bylex", "LIMIT", offset, count);
    }

    if (rtxn) {
      return 0;
    }

    boost::system::error_code ec;
    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "RedisBucketDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBucketDirectory::zscan(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, uint64_t cursor, const std::string& prefix, uint64_t count, std::vector<CacheObject>& objs_info, uint64_t& next_cursor, std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    std::string pattern = prefix + "*";
    target->push("ZSCAN", bucket_id, cursor, "MATCH", pattern, "COUNT", count);

    if (rtxn) {
      return 0;
    }

    boost::system::error_code ec;
    boost::redis::generic_response resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    std::vector<boost::redis::resp3::basic_node<std::__cxx11::basic_string<char> > > root_array;
    if (resp.has_value()) {
      root_array = resp.value();
      ldpp_dout(dpp, 20) << "RedisBucketDirectory::" << __func__ << "() aggregate size is: " << root_array.size() << dendl;
      auto size = root_array.size();
      if (size >= 2) {
        //Nothing of interest at index 0, index 1 has the next cursor value
        next_cursor = std::stoull(root_array[1].value);

        //skip the first 3 values to get the actual member, score
        for (uint64_t i = 3; i < size; i = i+2) {
          objs_info.push_back(CacheObject{});
          objs_info.back().objName = root_array[i].value;
          ldpp_dout(dpp, 20) << "RedisBucketDirectory::" << __func__ << "() objName is: " << objs_info.back().objName << dendl;
        }
      }
    } else {
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBucketDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  std::string key = bucket_id;
  try {
    target->push("EXISTS", key);

    if (rtxn) {
      return 0;
    }

    boost::system::error_code ec;
    response<int> resp;

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    return std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBucketDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

//FIXME: this is a dummy function and should be updated.
int RedisBucketDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return 0;
}

int RedisBucketDirectory::add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return zadd(dpp, y, bucket_id, 0, object_name, txn);
}

int RedisBucketDirectory::remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return zrem(dpp, y, bucket_id, object_name, txn);
}

int RedisBucketDirectory::list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!prefix.empty()) {
    // SCAN_OBJECTS path (with prefix)
    std::string continuation_token;

    auto ret = scan_objects(
      dpp,
      y,
      bucket_id,
      start_token,
      prefix,
      marker,
      marker_inclusive,
      count,
      objs_info,
      continuation_token,
      txn);

    if (ret < 0 ) {
      ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << " scan_objects failed: " << ret << dendl;
      return ret;
    }
  } else {
    // GET_RANGE path (no prefix)
    std::string continuation_token;
    auto ret = get_range(
      dpp,
      y,
      bucket_id,
      marker,
      count,
      marker_inclusive,
      objs_info,
      continuation_token, 
      txn);

    if (ret < 0) {
      ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << " get_range failed: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

//Performs an incremental scan of objects within the specified bucket, returning a subset of results based on the provided cursor position and count.
int RedisBucketDirectory::scan_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn)
{
  uint64_t cursor = start_token.empty() ? 0 : std::stoull(start_token);
  uint64_t next_cursor = 0;
  auto ret = zscan(dpp, y, bucket_id, cursor, prefix, count, objs_info, next_cursor, txn);
  continuation_token = (next_cursor == 0) ? "" : std::to_string(next_cursor);
  return ret;
}

int RedisBucketDirectory::get_range(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start, uint64_t count, bool start_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string redis_start;
  if (start.empty()) {
    redis_start = "-";
  } else {
    redis_start = (start_inclusive ? "[" : "(") + start;
  }

  uint64_t fetch_count = count ? count + 1 : 0;

  std::vector<std::string> members;
  auto ret = zrange(dpp, y, bucket_id, redis_start, "+", 0, fetch_count, members, txn);
  if (ret < 0) {
    return ret;
  }

  uint64_t actual_size = (count == 0) ? members.size() : std::min<uint64_t>(count, members.size());
  for (uint64_t i = 0; i < actual_size; i++) {
    objs_info.push_back(CacheObject{});
    objs_info.back().objName = members[i];
  }

  if (count && members.size() > count) {
    continuation_token = objs_info.back().objName;
  }
  return 0;
}

int RedisObjectDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, std::optional<std::reference_wrapper<Transaction>> txn) 
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  std::string key = build_index(bucket_id, obj_name);
  response<int> resp;
  try {
    target->push_range("EXISTS", key);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}

int RedisObjectDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, std::optional<std::reference_wrapper<Transaction>> txn) 
{
  std::string key = build_index(object->bucketName, object->objName);
  ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "(): index is: " << key << dendl;

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push_range("DEL", key);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<int> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "(): No values deleted." << dendl;
      return -ENOENT;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0; 
}

int RedisObjectDirectory::zadd(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, double score, const std::string& member, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(bucket_id, obj_name);

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZADD", key, "CH", std::to_string(score), member);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "() Response value is: " << std::get<0>(resp).value() << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}


int RedisObjectDirectory::zrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, int start, int stop, std::vector<std::string>& members, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(bucket_id, obj_name);

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZRANGE", key, std::to_string(start), std::to_string(stop));

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value().empty()) {
      ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "() Empty response" << dendl;
      return -ENOENT;
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisObjectDirectory::zrevrange(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<std::string>& members, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(bucket_id, obj_name);

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZREVRANGE", key, start, stop);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<std::vector<std::string> > resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    members = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisObjectDirectory::zrem(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(bucket_id, obj_name);

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZREM", key, member);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() != "1") {
      ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "() Response is: " << std::get<0>(resp).value() << dendl;
      return -ENOENT;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisObjectDirectory::zremrangebyscore(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, double min, double max, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(bucket_id, obj_name);

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZREMRANGEBYSCORE", key, std::to_string(min), std::to_string(max));

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(resp).value() == "0") {
      ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "() No element removed!" << dendl;
      return -ENOENT;
    }

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisObjectDirectory::zrank(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& member, std::string& index, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(bucket_id, obj_name);

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    target->push("ZRANK", key, member);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    boost::system::error_code ec;
    response<std::string> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    index = std::get<0>(resp).value();

  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

int RedisObjectDirectory::add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, std::optional<std::reference_wrapper<Transaction>> txn)
{
  // Redis sorted-set scores are doubles; use microseconds for precision within double's exact-integer range (~9e15)
  auto score = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
      creation_time.time_since_epoch()).count());
  ldpp_dout(dpp, 10) << "RedisObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;
  return zadd(dpp, y, bucket_id, obj_name, score, version, txn);
}

int RedisObjectDirectory::remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return zrem(dpp, y, bucket_id, obj_name, version, txn);
}

int RedisObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, std::optional<std::reference_wrapper<Transaction>> txn)
{
  auto min = ceph::real_clock::to_double(creation_time);
  auto max = ceph::real_clock::to_double(creation_time);
  return zremrangebyscore(dpp, y, bucket_id, obj_name, min, max, txn);
}

int RedisObjectDirectory::list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn)
{
  continuation_token.clear();
  // Get starting version index from marker
  uint64_t start_rank = 0;
  if(!marker_version.empty()) {
    std::string index;
    auto ret = get_version_index(dpp, y, bucket_id, obj_name, marker_version, index, txn);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "D4NFilterBucket::" << __func__ << " zrank failed: " << ret << dendl;
      return ret;
    }
    start_rank = std::stoull(index) + 1; //start version is exclusive
  }
  std::string start = std::to_string(start_rank);
  std::string stop = count ? std::to_string(start_rank + (count + 1) - 1) : "-1";
  std::vector<std::string> members;
  auto ret = zrevrange(dpp, y, bucket_id, obj_name, start, stop, members, txn);
  if (ret < 0 ) {
    return ret;
  }
  if (members.empty()) {
    return -ENOENT;
  }
  uint64_t actual_size = count ? std::min<uint64_t>(count, members.size()) : members.size();
  obj_versions.reserve(actual_size);
  for (uint64_t i = 0; i < actual_size; i++) {
    auto& obj_version = obj_versions.emplace_back();
    obj_version.bucketId = bucket_id;
    obj_version.objName = obj_name;
    obj_version.version = members[i];
  }
  if(members.size() > count) {
    continuation_token = members[count - 1];
  }

  return 0;
}

int RedisObjectDirectory::get_version_index(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return zrank(dpp, y, bucket_id, obj_name, version, index, txn);
}

int RedisBlockDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn) 
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  if (!block) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  response<int> resp;
  try {
    std::string key = build_index(block);
    boost::system::error_code ec;
    target->push("EXISTS", key);

    if (rtxn) {
      return 0;   // batched — caller commits later
    }

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return false;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisObjectDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return std::get<0>(resp).value();
}


/* This is only for Associative containers with operator [] */
template<AssociativeContainer Container>
int RedisBlockDirectory::set_values(const DoutPrefixProvider* dpp, CacheBlock& block, Container& redisValues, optional_yield y)
{
  std::string hosts;
  /* Creating a redisValues of the entry's properties */
  redisValues["blockID"] = std::to_string(block.blockID);
  redisValues["version"] = block.version;

  int ret = -1;
  if ((ret = check_bool(std::to_string(block.deleteMarker))) != -EINVAL) {
    block.deleteMarker = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value for delete marker" << dendl;
    return -EINVAL;
  }
  redisValues["deleteMarker"] = std::to_string(block.deleteMarker);
  redisValues["invalid"] = std::to_string(block.invalid);
  redisValues["size"] = std::to_string(block.size);
  redisValues["globalWeight"] = std::to_string(block.globalWeight);
  redisValues["objName"] = block.cacheObj.objName;
  redisValues["bucketName"] = block.cacheObj.bucketName;
  redisValues["creationTime"] = block.cacheObj.creationTime;

  if ((ret = check_bool(std::to_string(block.cacheObj.dirty))) != -EINVAL) {
    block.cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0) << "BlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
    return -EINVAL;
  }
  redisValues["dirty"] = std::to_string(block.cacheObj.dirty);

  hosts.clear();
  for (auto const& host : block.cacheObj.hostsList) {
    if (hosts.empty())
    hosts = host + "_";
    else
    hosts = hosts + host + "_";
  }

  if (!hosts.empty())
    hosts.pop_back();

  redisValues["hosts"] = hosts;
  redisValues["etag"] = block.cacheObj.etag;
  redisValues["objSize"] = std::to_string(block.cacheObj.size);
  redisValues["userId"] = block.cacheObj.user_id;
  redisValues["displayName"] = block.cacheObj.display_name;
  redisValues["acl"] = block.cacheObj.acl;

  redisValues["attrsCount"]   = std::to_string(block.cacheObj.attrs.size());
  for (auto const& [key, bl] : block.cacheObj.attrs) {
    redisValues["attr_" + key] = bl.to_str();
  }
  return 0;
}

int RedisBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  std::map<std::string, std::string> redisValues;
  auto ret = set_values(dpp, *block, redisValues, y);
  if (ret < 0) {
    return ret;
  }

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  target->push_range("HSET", key, redisValues);

  if (rtxn) {
    return 0;   // batched — caller commits later
  }

  try {
    boost::system::error_code ec;
    response<ignore_t> resp;

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y,
                             std::vector<CacheBlock>& blocks,
                             std::optional<std::reference_wrapper<Transaction>> txn)
{
  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  try {
    for (auto& block : blocks) {
      std::string key = build_index(&block);
      ldpp_dout(dpp, 10)
        << "RedisBlockDirectory::" << __func__
        << "(): index is: " << key << dendl;

      std::map<std::string, std::string> redisValues;
      auto ret = set_values(dpp, block, redisValues, y);
      if (ret < 0) {
        return ret;
      }

      target->push_range("HSET", key, redisValues);
    }

    if (rtxn) {
      return 0;  // batched — caller commits later
    }

    boost::system::error_code ec;
    boost::redis::generic_response resp;

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0)
        << "RedisBlockDirectory::" << __func__
        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception& e) {
    ldpp_dout(dpp, 0)
      << "RedisBlockDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

template<typename T, typename Seq>
struct expander;

template<typename T, std::size_t... Is>
struct expander<T, std::index_sequence<Is...>> {
template<typename E, std::size_t>
using elem = E;

using type = boost::redis::response<elem<T, Is>...>;
};

template <size_t N, class Type>
struct redis_response
{
  using type = typename expander<Type, std::make_index_sequence<N>>::type;
};

template <typename Integer, Integer ...I, typename F>
constexpr void constexpr_for_each(std::integer_sequence<Integer, I...>, F &&func)
{
    (func(std::integral_constant<Integer, I>{}) , ...);
}

template <auto N, typename F>
constexpr void constexpr_for(F &&func)
{
    if constexpr (N > 0)
    {
        constexpr_for_each(std::make_integer_sequence<decltype(N), N>{}, std::forward<F>(func));
    }
}

template <typename T>
void parse_response(T t, std::vector<std::vector<std::string>>& responses)
{
    constexpr_for<std::tuple_size_v<T>>([&](auto index)
    {
      std::vector<std::string> empty_vector;
      constexpr auto i = index.value;
      if (std::get<i>(t).value().has_value()) {
        if (std::get<i>(t).value().value().empty()) {
          responses.emplace_back(empty_vector);
        } else {
          responses.emplace_back(std::get<i>(t).value().value());
        }
      } else {
        responses.emplace_back(empty_vector);
      }
    });
}

int RedisBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn) 
{
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    response< std::optional<std::map<std::string, std::string>> > resp;
    request req;
    req.push("HGETALL", key);

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    const auto& result = std::get<0>(resp);
    if (!result.has_value()) {
      ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): No values returned for key=" << key << dendl;
      return -ENOENT;
    }

    const auto& opt = result.value();
    if (!opt.has_value() || opt.value().empty()) {
      ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): No values returned for key=" << key << dendl;
      return -ENOENT;
    }
    const std::map<std::string, std::string>& fieldMap = opt.value();
    block->blockID = std::stoull(fieldMap.at("blockID"));
    block->version = fieldMap.at("version");
    block->deleteMarker = (std::stoi(fieldMap.at("deleteMarker")) != 0);
    if (fieldMap.count("invalid")) block->invalid = (std::stoi(fieldMap.at("invalid")) != 0);
    block->size = std::stoull(fieldMap.at("size"));
    block->globalWeight = std::stoull(fieldMap.at("globalWeight"));
    block->cacheObj.objName = fieldMap.at("objName");
    block->cacheObj.bucketName = fieldMap.at("bucketName");
    block->cacheObj.creationTime = fieldMap.at("creationTime");
    block->cacheObj.dirty = (std::stoi(fieldMap.at("dirty")) != 0);
    boost::split(block->cacheObj.hostsList, fieldMap.at("hosts"), boost::is_any_of("_"));
    block->cacheObj.etag = fieldMap.at("etag");
    block->cacheObj.size = std::stoull(fieldMap.at("objSize"));
    block->cacheObj.user_id = fieldMap.at("userId");
    block->cacheObj.display_name = fieldMap.at("displayName");
    block->cacheObj.acl = fieldMap.at("acl");

    size_t attrsCount = std::stoull(fieldMap.at("attrsCount"));
    size_t found = 0;
    for (auto const& [field, value] : fieldMap) {
      if (field.starts_with("attr_")) {
        std::string attrKey = field.substr(5);
        ceph::buffer::list bl;
        bl.append(value);
        block->cacheObj.attrs[attrKey] = std::move(bl);

        if (++found == attrsCount) break;
      }
    }

    if (found != attrsCount) {
      ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "() ERROR: expected " << attrsCount << " attrs but found " << found << dendl;
      return -EINVAL;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (blocks.empty())
    return 0;

  boost::redis::generic_response resp;
  request req;
  for (auto& block : blocks) {
    std::string key = build_index(&block);
    std::vector<std::string> fields;
    ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    fields.push_back("blockID");
    fields.push_back("version");
    fields.push_back("deleteMarker");
    fields.push_back("invalid");
    fields.push_back("size");
    fields.push_back("globalWeight");

    fields.push_back("objName");
    fields.push_back("bucketName");
    fields.push_back("creationTime");
    fields.push_back("dirty");
    fields.push_back("hosts");
    fields.push_back("etag");
    fields.push_back("objSize");
    fields.push_back("userId");
    fields.push_back("displayName");
    fields.push_back("acl");

    try {
      req.push("HGETALL", key);
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } //end - for

  try {
    boost::system::error_code ec;
    redis_exec(REDISconn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  //i is used to index blocks
  //j is used to keep a track of number of elements for aggregate type map or array
  auto i = 0, j = 0;
  bool field_key=true, field_val=false;
  std::string key, fieldkey, fieldval, prev_val;
  int num_elements = 0;
  for (auto& element : resp.value()) {
    ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): i is: " << i << dendl;
    CacheBlock* block = &blocks[i];
    std::string key = build_index(block);
    ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): index is: " << key << dendl;
    if (element.data_type == boost::redis::resp3::type::array || element.data_type == boost::redis::resp3::type::map) {
      num_elements = element.aggregate_size;
      if (num_elements == 0) {
        i++;
        j = 0;
      }
      ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "() num_elements: " << num_elements << dendl;
      continue;
    } else {
      if (j < num_elements) {
        if (field_key && !field_val) {
          if (element.value == "blockID" || element.value == "version" || element.value == "deleteMarker" ||
              element.value == "invalid" || element.value == "size" || element.value == "globalWeight" || element.value == "objName" ||
              element.value == "bucketName" || element.value == "creationTime" || element.value == "dirty" ||
              element.value == "hosts" || element.value == "etag" || element.value == "objSize" ||
              element.value == "userId" || element.value == "displayName" || element.value == "acl" ||
              element.value == "attrsCount" || element.value.starts_with("attr_")) {
            prev_val = element.value;
            ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "() field key: " << prev_val << dendl;
            field_key = false;
            field_val = true;
          }
          continue;
        } else {
          ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "() field val: " << element.value << dendl;
          if (prev_val == "blockID") {
            block->blockID = std::stoull(element.value);
          } else if (prev_val == "version") {
            block->version = element.value;
          } else if (prev_val == "deleteMarker") {
            block->deleteMarker = (std::stoi(element.value) != 0);
          } else if (prev_val == "invalid") {
            block->invalid = (std::stoi(element.value) != 0);
          } else if (prev_val == "size") {
            block->size = std::stoull(element.value);
          } else if (prev_val == "globalWeight") {
            block->globalWeight = std::stoull(element.value);
          } else if (prev_val == "objName") {
            block->cacheObj.objName = element.value;
          } else if (prev_val == "bucketName") {
            block->cacheObj.bucketName = element.value;
          } else if (prev_val == "creationTime") {
            block->cacheObj.creationTime = element.value;
          } else if (prev_val == "dirty") {
            block->cacheObj.dirty = (std::stoi(element.value) != 0);
          } else if (prev_val == "hosts") {
            boost::split(block->cacheObj.hostsList, element.value, boost::is_any_of("_"));
          } else if (prev_val == "etag") {
            block->cacheObj.etag = element.value;
          } else if (prev_val == "objSize") {
            block->cacheObj.size = std::stoull(element.value);
          } else if (prev_val == "userId") {
            block->cacheObj.user_id = element.value;
          } else if (prev_val == "displayName") {
            block->cacheObj.display_name = element.value;
          } else if (prev_val == "acl") {
            block->cacheObj.acl = element.value;
          } else if (prev_val == "attrsCount") {
            size_t attrsCount = std::stoi(element.value); //This is unused.
          } else if (prev_val.starts_with("attr_")) {
            std::string attrKey = prev_val.substr(5);
            ceph::buffer::list bl;
            bl.append(element.value);
            block->cacheObj.attrs[attrKey] = std::move(bl);
          }
          j++;
          field_key= true;
          field_val = false;
          prev_val.clear();
        }
      }
      if (j == num_elements) {
        i++;
        j = 0;
      }
    }
  }
  return 0;
}

int RedisBlockDirectory::copy(const DoutPrefixProvider* dpp, optional_yield y,
                              CacheBlock* block,
                              const std::string& copyName,
                              const std::string& copyBucketName,
                              std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  std::string key = build_index(block);
  auto copyBlock = CacheBlock{
    .cacheObj = {
      .objName = copyName,
      .bucketName = copyBucketName
    },
    .blockID = 0
  };
  std::string copyKey = build_index(&copyBlock);

  try {
    target->push("COPY", key, copyKey);
    target->push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);

    if (rtxn) {
      return 0;  // batched — caller commits later
    }

    boost::system::error_code ec;
    response<ignore_t, ignore_t> resp;

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0)
        << "RedisBlockDirectory::" << __func__
        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception& e) {
    ldpp_dout(dpp, 0)
      << "RedisBlockDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int RedisBlockDirectory::del(const DoutPrefixProvider* dpp, optional_yield y,
                             CacheBlock* block,
                             std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  request req;
  RedisTransaction* rtxn = nullptr;
  request* target = nullptr;
  if (int ret = prepare_request(dpp, txn, req, rtxn, target); ret < 0)
    return ret;

  std::string key = build_index(block);
  ldpp_dout(dpp, 10)
    << "RedisBlockDirectory::" << __func__
    << "(): index is: " << key << dendl;

  try {
    target->push("DEL", key);

    if (rtxn) {
      return 0;  // batched — caller commits later
    }

    boost::system::error_code ec;
    response<int> resp;

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, *target, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0)
        << "RedisBlockDirectory::" << __func__
        << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10)
        << "RedisBlockDirectory::" << __func__
        << "(): No values deleted for key=" << key << dendl;
      return -ENOENT;
    }
  } catch (std::exception& e) {
    ldpp_dout(dpp, 0)
      << "RedisBlockDirectory::" << __func__
      << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

/* Note: This method is not compatible for use on Ubuntu systems. */
/*
int RedisBlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  std::string key = build_index(block);
  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

  try {
    boost::system::error_code ec;
    response<
      ignore_t,
      ignore_t,
      ignore_t,
      response<std::optional<int>, std::optional<int>> 
    > resp;
    request req;
    req.push("MULTI");
    req.push("COPY", key, copyKey);
    req.push("HSET", copyKey, "objName", copyName, "bucketName", copyBucketName);
    req.push("EXEC");

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }

    if (std::get<0>(std::get<3>(resp).value()).value().value() == 1) {
      return 0;
    } else {
      ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): No values copied." << dendl;
      return -ENOENT;
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int RedisBlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
{
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  try {
    boost::system::error_code ec;
    request req;
    req.push("DEL", key);
    response<int> resp;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);
    if (!std::get<0>(resp).value()) {
      ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): No values deleted for key=" << key << dendl;
      return -ENOENT;
    }
    if (ec) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      std::cout << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << std::endl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    std::cout << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << std::endl;
    return -EINVAL;
  }

  return 0; 
}
*/

//FIXME: We cannot guarantee atomicty for update_field and remove_host functions unless we use Redis lua script.
//TODO: update these functions accrodingly.
int RedisBlockDirectory::update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, std::optional<std::reference_wrapper<Transaction>> txn)
{
  int ret = -1;
  std::string key = build_index(block);

  if ((ret = exist_key(dpp, y, block, txn))) {
    try {
      if (field == "hosts") { 
	/* Append rather than overwrite */
	ldpp_dout(dpp, 20) << "RedisBlockDirectory::" << __func__ << "() Appending to hosts list." << dendl;

	boost::system::error_code ec;
	response< std::optional<std::string> > resp;
	request req;
	req.push("HGET", key, field);

    	redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

	if (ec) {
	  ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	  return -ec.value();
	}

	/* If entry exists, it should have at least one host */
	std::get<0>(resp).value().value() += "_";
	std::get<0>(resp).value().value() += value;
	value = std::get<0>(resp).value().value();
      } else if (field == "dirty") { 
	int ret = -1;
	if ((ret = check_bool(value)) != -EINVAL) {
          bool val = (ret != 0);
	  value = std::to_string(val);
	} else {
	  ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: Invalid bool value" << dendl;
	  return -EINVAL;
	}
      }

      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, field, value);

    	redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      return 0; 
    } catch (std::exception &e) {
      ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
      return -EINVAL;
    }
  } else if (ret == -ENOENT) {
    ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
  } else {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "(): ERROR: ret=" << ret << dendl;
  }
  
  return ret;
}

int RedisBlockDirectory::remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, std::optional<std::reference_wrapper<Transaction>> txn)
{
  std::string key = build_index(block);
  std::string tmpVal = value;

  try {
    {
      boost::system::error_code ec;
      response< std::optional<std::string> > resp;
      request req;
      req.push("HGET", key, "hosts");

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }

      if (std::get<0>(resp).value().value().empty()) {
	ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): No values returned." << dendl;
	return -ENOENT;
      }

      std::string result = std::get<0>(resp).value().value();
      auto it = result.find(tmpVal);
      if (it != std::string::npos) { 
	result.erase(result.begin() + it, result.begin() + it + tmpVal.size());
      } else {
	ldpp_dout(dpp, 10) << "RedisBlockDirectory::" << __func__ << "(): Host was not found." << dendl;
	return -ENOENT;
      }

      if (result[0] == '_') {
	result.erase(0, 1);
      } else if (result.length() && result[result.length() - 1] == '_') {
	result.erase(result.length() - 1, 1);
      }

      if (result.length() == 0) { /* Last host, delete entirely */
        int ret = del(dpp, y, block, txn); 
        if (ret < 0) {
		  ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Failed to delete entire block, ret=" << ret << dendl;
        }
		return ret;
      }

      tmpVal = result;
    }

    {
      boost::system::error_code ec;
      response<ignore_t> resp;
      request req;
      req.push("HSET", key, "hosts", tmpVal);

    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

      if (ec) {
	ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << ec.what() << dendl;
	return -ec.value();
      }
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "RedisBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }

  return 0;
}

int Pipeline::execute(const DoutPrefixProvider* dpp, optional_yield y)
{
  boost::redis::generic_response resp;
  try {
    boost::system::error_code ec;
    pipeline_mode = false;
    redis_exec_connection_pool(dpp, redis_pool, REDISconn, ec, req, resp, y);

    if (ec) {
      ldpp_dout(dpp, 0) << "Directory::" << __func__ << "() ERROR: " << ec.what() << dendl;
      return -ec.value();
    }
  } catch (std::exception &e) {
    ldpp_dout(dpp, 0) << "Directory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
  return 0;
}

} // namespace rgw::d4n
