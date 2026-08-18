#include <algorithm>
#//include <limits>
#include <type_traits>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "common/dout_fmt.h"
#include "d4n_directory_fdb.h"
#include <source_location>

namespace rgw::d4n {

using std::map;
using std::string;
namespace fdbc = lfdb::layer::content;
namespace q    = lfdb::query;

static std::string encode_score(int64_t score)
{
  return fmt::format("{:019d}", score);
}

// Helper: Convert seconds to nanoseconds for TTL specification
constexpr uint64_t seconds_to_nanoseconds(uint64_t seconds) {
  // Check for overflow: max uint64_t / 1e9 ≈ 18.4e9 seconds ≈ 584 years
  if (seconds > std::numeric_limits<uint64_t>::max() / 1000000000ULL) {
    return std::numeric_limits<uint64_t>::max();
  }
  return seconds * 1000000000ULL;
}

// Helper: Convert milliseconds to nanoseconds for TTL specification
constexpr uint64_t milliseconds_to_nanoseconds(uint64_t milliseconds) {
  if (milliseconds > std::numeric_limits<uint64_t>::max() / 1000000ULL) {
    return std::numeric_limits<uint64_t>::max();
  }
  return milliseconds * 1000000ULL;
}

// Lease metadata structure
struct LeaseData {
  uint64_t expiry = 0;          // When the lease expires (epoch nanoseconds)
  std::string holder_id;
  std::string token;
  uint64_t tick_count = 0;
  std::string last_renewal_id;  // ID of last renewal - for replay detection

  bool is_active(uint64_t now) const {
    return expiry > now;
  }
};

// Build FDB key for a lease on a resource using FDB directory layer
// Uses compiled_key directly to avoid string conversion overhead
// Parses hierarchical resource names into FDB tuple components
// Format: "bucket/object/version/operation" or "bucket/object/version/operation/uuid"
// Note: Components are URL-encoded by the caller to handle '/' in S3 object names
auto make_lease_key(const std::string& resource_name)
{
  auto root = fdbc::keyspace("d4n") / "leases";

  std::vector<std::string> parts;
  boost::split(parts, resource_name, boost::is_any_of("/"));

  auto key = root;
  for (const auto& part : parts) {
    if (!part.empty()) {
      key = key / part;
    }
  }
  return key;
}

// Get current time in nanoseconds since epoch
uint64_t current_time_nanoseconds()
{
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return static_cast<uint64_t>(std::max<int64_t>(now, 0));
}

// Calculate expiry time from TTL in nanoseconds
uint64_t calculate_expiry(uint64_t ttl_nanoseconds)
{
  const auto now = current_time_nanoseconds();
  if (ttl_nanoseconds > std::numeric_limits<uint64_t>::max() - now) {
    return std::numeric_limits<uint64_t>::max();
  }
  return now + ttl_nanoseconds;
}

int FDBLease::acquire(const DoutPrefixProvider* dpp,
                             const std::string& resource_name,
                             const std::string& holder_id,
                             const std::string& token,
                             uint64_t ttl_nanoseconds)
{
  if (ttl_nanoseconds == 0) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " invalid TTL=0 for resource=" << resource_name << dendl;
    return -EINVAL;
  }

  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  const auto fdb_key = make_lease_key(resource_name);

  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      const auto now = current_time_nanoseconds();

      // Check if resource already has an active lease
      LeaseData existing_lease;
      if (lfdb::get(tr, fdb_key, existing_lease)) {
        if (existing_lease.is_active(now)) {
          // Handle transaction replay - if we already acquired this lease
          // (commit_unknown_result replay), return success instead of -EEXIST
          if (existing_lease.holder_id == holder_id && existing_lease.token == token) {
            ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                               << " lease already acquired by us (replay): resource="
                               << resource_name << dendl;
            return 0;  // Idempotent - already acquired by us
          }
          ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                             << " resource already leased by holder=" << existing_lease.holder_id
                             << " resource=" << resource_name << dendl;
          return -EEXIST;
        }
        // Expired, can reuse
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " reusing expired lease on resource=" << resource_name << dendl;
      }

      // Acquire the lease
      LeaseData new_lease{
        .expiry = calculate_expiry(ttl_nanoseconds),
        .holder_id = holder_id,
        .token = token,
        .tick_count = 0,
        .last_renewal_id = ""  // No renewals yet
      };
      lfdb::set(tr, fdb_key, new_lease);

      // NOTE: Log before commit() - may print multiple times on transaction replay
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " acquired lease: resource=" << resource_name
                         << " holder=" << holder_id
                         << " expiry_ns=" << new_lease.expiry << dendl;
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return -EIO;
  }
}

int FDBLease::renew(const DoutPrefixProvider* dpp,
                           const std::string& resource_name,
                           const std::string& holder_id,
                           const std::string& token,
                           uint64_t ttl_nanoseconds,
                           uint64_t max_ticks)
{
  if (ttl_nanoseconds == 0) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " invalid TTL=0 for resource=" << resource_name << dendl;
    return -EINVAL;
  }

  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  // Generate unique renewal_id OUTSIDE transaction lambda
  // This ensures the same ID is used on transaction replay
  // Format: timestamp_nanoseconds:holder_id:token_hash
  const uint64_t renewal_timestamp = current_time_nanoseconds();
  const std::string renewal_id = std::to_string(renewal_timestamp) + ":" +
                                  holder_id + ":" +
                                  std::to_string(std::hash<std::string>{}(token));

  const auto fdb_key = make_lease_key(resource_name);

  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      LeaseData existing_lease;
      const auto now = current_time_nanoseconds();

      if (!lfdb::get(tr, fdb_key, existing_lease)) {
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " lease not found: resource=" << resource_name << dendl;
        return -ENOENT;
      }

      // REPLAY DETECTION: Check if this exact renewal was already applied
      if (existing_lease.last_renewal_id == renewal_id) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " renewal already applied (transaction replay detected)"
                           << " renewal_id=" << renewal_id
                           << " resource=" << resource_name << dendl;
        return 0;  // Idempotent - this renewal already happened
      }

      if (!existing_lease.is_active(now)) {
        // Lease expired, clean it up
        lfdb::erase(tr, fdb_key);
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " lease expired: resource=" << resource_name << dendl;
        return -ENOENT;
      }

      // Validate ownership
      if (existing_lease.holder_id != holder_id || existing_lease.token != token) {
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " ownership validation failed: resource=" << resource_name
                           << " expected_holder=" << existing_lease.holder_id
                           << " provided_holder=" << holder_id << dendl;
        return -EACCES;
      }

      // Check if max_ticks limit reached
      if (max_ticks > 0 && existing_lease.tick_count >= max_ticks) {
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " max_ticks limit reached: resource=" << resource_name
                           << " tick_count=" << existing_lease.tick_count
                           << " max_ticks=" << max_ticks << dendl;
        return -EINVAL;  // Lease still held - caller decides what to do
      }

      // Renew the lease with incremented tick count and this renewal_id
      LeaseData renewed_lease{
        .expiry = calculate_expiry(ttl_nanoseconds),
        .holder_id = holder_id,
        .token = token,
        .tick_count = existing_lease.tick_count + 1,
        .last_renewal_id = renewal_id  // Mark this renewal as applied
      };
      lfdb::set(tr, fdb_key, renewed_lease);

      // NOTE: Log before commit() - may print multiple times on transaction replay
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " renewed lease: resource=" << resource_name
                         << " holder=" << holder_id
                         << " new_expiry_ns=" << renewed_lease.expiry
                         << " tick_count=" << renewed_lease.tick_count
                         << " renewal_id=" << renewal_id << dendl;
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return -EIO;
  }
}

int FDBLease::release(const DoutPrefixProvider* dpp,
                             const std::string& resource_name,
                             const std::string& holder_id,
                             const std::string& token)
{
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return -EINVAL;
  }

  const auto fdb_key = make_lease_key(resource_name);

  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      LeaseData existing_lease;
      if (!lfdb::get(tr, fdb_key, existing_lease)) {
        // Make release() idempotent for transaction replay
        // If lease not found, it was already released (possibly by replay)
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " lease not found (already released or replay): resource="
                           << resource_name << dendl;
        return 0;  // Idempotent - already released
      }

      // Validate ownership
      if (existing_lease.holder_id != holder_id || existing_lease.token != token) {
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " ownership validation failed: resource=" << resource_name
                           << " expected_holder=" << existing_lease.holder_id
                           << " provided_holder=" << holder_id << dendl;
        return -EACCES;
      }

      // Release the lease
      lfdb::erase(tr, fdb_key);

      // NOTE: Log before commit() - may print multiple times on transaction replay
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " released lease: resource=" << resource_name
                         << " holder=" << holder_id << dendl;
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return -EIO;
  }
}

LeaseCheckResult FDBLease::any_active(const DoutPrefixProvider* dpp,
                                      const std::string& resource_prefix)
{
  if (resource_prefix.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_prefix" << dendl;
    return {.active = false, .error = -EINVAL};
  }

  // Build prefix for scanning all leases matching this resource
  // With hierarchical keys, prefix matching now works correctly:
  // "bucket" prefix will match "bucket/object1", "bucket/object2", etc.
  // "bucket/obj/v1/GET" prefix will match all "bucket/obj/v1/GET/*" (all uuids)
  const auto prefix_key = make_lease_key(resource_prefix);

  try {
    bool has_active = lfdb::make_transactor(FDBdb)([&](auto& tr) -> bool {
      // Use query algebra for prefix scan
      auto gen = lfdb::scan<LeaseData>(tr, q::prefix(prefix_key));
      auto it  = std::ranges::begin(gen);
      auto end = std::ranges::end(gen);

      // Collect all matching leases
      std::vector<std::pair<std::string, LeaseData>> rows;
      for (; it != end; ++it) {
        rows.push_back(*it);
      }

      if (rows.empty()) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " no leases found for resource prefix: " << resource_prefix << dendl;
        return false;
      }

      // Check if any lease is still active and opportunistically cleanup expired ones
      const auto now = current_time_nanoseconds();
      int total_leases = rows.size();
      int active_leases = 0;
      std::vector<std::string> expired_keys;

      for (const auto& [key, lease] : rows) {
        if (lease.is_active(now)) {
          active_leases++;
          ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                             << " found active lease: holder=" << lease.holder_id
                             << " expiry_ns=" << lease.expiry << dendl;
        } else {
          // Collect expired leases for opportunistic cleanup
          expired_keys.push_back(key);
        }
      }

      // Opportunistic cleanup: delete expired leases in the same transaction
      for (const auto& key : expired_keys) {
        lfdb::erase(tr, key);
      }

      if (!expired_keys.empty()) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " cleaned up " << expired_keys.size() << " expired leases" << dendl;
      }

      bool result = (active_leases > 0);
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " resource_prefix=" << resource_prefix
                         << " total_leases=" << total_leases
                         << " active_leases=" << active_leases
                         << " has_active=" << result << dendl;
      return result;
    });

    return {.active = has_active, .error = 0};

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource_prefix=" << resource_prefix
                      << ": " << e.what() << dendl;
    return {.active = false, .error = -EIO};
  }
}

LeaseCheckResult FDBLease::is_active(const DoutPrefixProvider* dpp,
                                     const std::string& resource_name,
                                     const std::string& holder_id,
                                     const std::string& token)
{
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return {.active = false, .error = -EINVAL};
  }

  const auto fdb_key = make_lease_key(resource_name);

  try {
    // Atomic check-and-cleanup in single transaction to avoid TOCTOU race
    // NOTE: If callers use is_active() to authorize a later FDB update,
    // consider sharing the transaction: read the lease key in the update
    // transaction itself - FDB will retry if the lease changes
    bool active = lfdb::make_transactor(FDBdb)([&](auto& tr) -> bool {
      LeaseData lease;
      if (!lfdb::get(tr, fdb_key, lease)) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " lease not found: resource=" << resource_name << dendl;
        return false;
      }

      // Check expiry and opportunistically cleanup if expired
      const auto now = current_time_nanoseconds();
      if (!lease.is_active(now)) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " lease expired: resource=" << resource_name
                           << " expiry_ns=" << lease.expiry << " - deleting" << dendl;
        // Opportunistic cleanup: delete in same transaction (atomic)
        lfdb::erase(tr, fdb_key);
        return false;
      }

      // Validate ownership
      if (lease.holder_id != holder_id || lease.token != token) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " ownership mismatch: resource=" << resource_name
                           << " expected_holder=" << lease.holder_id
                           << " provided_holder=" << holder_id << dendl;
        return false;
      }

      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " lease is active: resource=" << resource_name
                         << " holder=" << holder_id
                         << " expiry_ns=" << lease.expiry << dendl;
      return true;
    });

    return {.active = active, .error = 0};

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return {.active = false, .error = -EIO};
  }
}
// Returns count+1 so callers can detect whether a continuation token is needed:
// if FDB returns count+1 rows, there are more; if fewer, the range is exhausted.
// Returns 0 when count==0 (unbounded).
static int fdb_page_read_limit(uint64_t count)
{
  if (count == 0) {
    return 0;
  }

  constexpr auto max = static_cast<uint64_t>(std::numeric_limits<int>::max());
  return static_cast<int>(count >= max ? max : count + 1);
}


inline void report_fdb_error(
		    const DoutPrefixProvider* dpp,
		    const lfdb::libfdb_exception& e,
		    std::source_location whence = std::source_location::current())
{
  ldpp_dout_fmt(
      dpp, 0, "{} ERROR at {}:{}: {}",
      whence.function_name(),
      whence.file_name(),
      whence.line(),
      e.what());
}


int FDBTransaction::commit(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (executed_) {
    return -EINVAL;
  }

  try {
    if (lfdb::commit(txn_)) {
      executed_ = true;
      return 0;
    }

    executed_ = true;
    ldpp_dout(dpp, 10) << "FDBTransaction::" << __func__ << "() transaction failed, replay required" << dendl;
    return -EAGAIN;
  } catch (const lfdb::libfdb_exception& e) {
    executed_ = true;
    ldpp_dout(dpp, 0) << "FDBTransaction::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBTransaction::abort(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (executed_) {
    return -EINVAL;
  }

  executed_ = true;
  txn_.reset();
  return 0;
}

template <typename Func>
int FDBDirectory::fdb_invoke(const DoutPrefixProvider* dpp,
		    std::optional<std::reference_wrapper<Transaction>> txn,
		    Func&& operation,
		    std::source_location whence)
{
  try {
    return with_fdb_transaction(txn, std::forward<Func>(operation));
  } catch (const lfdb::libfdb_exception& e) {
    report_fdb_error(dpp, e, whence);
    return -EIO;
  }
}


template <typename Func>
int FDBDirectory::with_fdb_transaction(std::optional<std::reference_wrapper<Transaction>> txn, Func&& func)
{
  if (txn) {
    auto* fdb_txn = dynamic_cast<FDBTransaction*>(&txn->get());
    if (!fdb_txn) {
      return -EINVAL;
    }

    auto& tr = fdb_txn->get_transaction();
    return func(tr);
  }

  return lfdb::make_transactor(FDBdb)([&](auto& tr) {
    return func(tr);
  });
}

int FDBDirectory::get_kv(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         const std::string& key,
                         const std::string& field,
                         std::string& out_val,
                         std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::map<std::string, std::string> kvs;
    if (!lfdb::get(tr, key, kvs)) {
      return -ENOENT;
    }
    auto it = kvs.find(field);
    if (it == kvs.end()) {
      return -ENOENT;
    }
    out_val = it->second;
    return 0;
  });
}

int FDBDirectory::set_kv(const DoutPrefixProvider* dpp, optional_yield y,
		    const std::string& key, const std::string& field,
		    const std::string& val, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      existing[field] = val;
      lfdb::set(tr, key, existing);
      return 0;
    });
}

int FDBDirectory::get_kv_multi(const DoutPrefixProvider* dpp, optional_yield,
		    const std::string& key, const std::vector<std::string>& fields,
		    std::map<std::string, std::string>& out_vals, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::map<std::string, std::string> kvs;
    if (!lfdb::get(tr, key, kvs)) {
      return -ENOENT;
    }

    out_vals.clear();
    for (const auto& field : fields) {
      auto it = kvs.find(field);
      if (it == kvs.end()) {
        return -ENOENT;
      }
      out_vals[field] = it->second;
    }

    return 0;
  });
}

int FDBDirectory::set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& key,
    const std::map<std::string, std::string>& vals,
    std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::map<std::string, std::string> existing;
    lfdb::get(tr, key, existing);
    for (const auto& [field, value] : vals) {
      existing[field] = value;
    }
    lfdb::set(tr, key, existing);
    return 0;
  });
}

int FDBDirectory::set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& key,
                                        const std::string& field,
                                        const std::string& val,
                                        std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      if (existing.find(field) == existing.end()) {
        existing[field] = val;
        lfdb::set(tr, key, existing);
      }
      return 0;
    });
}

int FDBBucketDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    lfdb::erase(tr, bucket_id);
    return 0;
  });
}

int FDBBucketDirectory::add_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_add(dpp, y, bucket_id, 0, object_name, std::move(params), txn);
}

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& object_name, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_rem(dpp, y, bucket_id, object_name, txn);
}

int FDBBucketDirectory::list_objects(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_scan(dpp, y, bucket_id, marker, prefix, count, marker_inclusive, objs_info, continuation_token, txn);
}

//Key form is <bucket-id>/objects/<object-name>
std::string FDBBucketDirectory::build_object_index(const std::string& bucket_id, const std::string& obj_name)
{
  return std::string(libfdb_key_view(fdbc::keyspace(bucket_id) / "objects")) + obj_name;
}

int FDBBucketDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y,
                                   const std::string& bucket_id, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    return lfdb::key_exists(tr, bucket_id);
  });
}

int FDBBucketDirectory::collect_range(const DoutPrefixProvider* dpp, 
		    const FDBRange& range,
		    const std::string& base,
		    uint64_t count,
		    std::vector<CacheObject>& objs_info,
		    std::string& continuation_token,
		    std::optional<std::reference_wrapper<Transaction>> txn)
{
  objs_info.clear();
  continuation_token.clear();

  return fdb_invoke(dpp, txn, [&](auto& tr) {
    auto selector = lfdb::select{range.begin, range.end};
    selector.options.result_limit = fdb_page_read_limit(count);

    bool have_more = false;
    std::size_t fetched = 0;

    for (auto&& [key, value] :
         lfdb::scan<CacheObject>(tr, selector)) {

      if (key.size() < base.size()) {
        ldpp_dout(dpp, 0)
            << "FDBBucketDirectory::" << __func__
            << "() malformed key: " << key
            << dendl;
        continue;
      }

      // The scan asks FDB for count + 1 rows.
      // The extra row tells us that another page exists.
      if (count && fetched >= count) {
        have_more = true;
        break;
      }

      const std::string member = key.substr(base.size());

      objs_info.push_back(std::move(value));
      objs_info.back().objName = member;

      ++fetched;
    }

    if (have_more && !objs_info.empty()) {
      continuation_token = objs_info.back().objName;
    }

    return 0;
  });
}

FDBRange FDBBucketDirectory::build_range(const std::string& base, const std::string& start, bool inclusive)
{
  FDBRange range;

  if (start.empty()) {
    range.begin = base;
  } else if (inclusive) {
    range.begin = base + start;
  } else {
    range.begin = base + start + '\0';
  }

  range.end = base + "\xff";

  return range;
}

int FDBBucketDirectory::fdb_add(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                double score,
                                const std::string& member,
                                std::optional<CacheObject> params,
                                std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!params) {
    return -EINVAL;
  }

  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::string member_key = build_object_index(bucket_id, member);
    lfdb::set(tr, member_key, *params);
    return 0;
  });
}

int FDBBucketDirectory::fdb_rem(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& member,
                                std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::string member_key = build_object_index(bucket_id, member);
    lfdb::erase(tr, member_key);
    return 0;
  });
}


int FDBBucketDirectory::fdb_scan(const DoutPrefixProvider* dpp, optional_yield y,
                                 const std::string& bucket_id,
                                 const std::string& start_token,
                                 const std::string& prefix,
                                 uint64_t count,
                                 bool marker_inclusive,
                                 std::vector<CacheObject>& objs_info,
                                 std::string& continuation_token,
                                 std::optional<std::reference_wrapper<Transaction>> txn)
{
  continuation_token.clear();

  const std::string base = std::string(libfdb_key_view(fdbc::keyspace(bucket_id) / "objects"));
  const std::string prefix_begin = base + prefix;
  const std::string marker_key = base + start_token;

  const auto object_query = start_token.empty()
      ? q::prefix(prefix_begin)
      : marker_inclusive
        ? q::prefix_starting_at(prefix_begin, marker_key)
        : q::prefix_starting_after(prefix_begin, marker_key);

  if (q::is_empty(object_query)) {
    return -ENOENT;
  }

  ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << "() prefix_begin: " << prefix_begin << dendl;

  const auto page_query = q::with_options(
      object_query,
      q::query_options{
          .result_limit = fdb_page_read_limit(count)});

  return fdb_invoke(dpp, txn, [&](auto& tr) {
    auto gen = lfdb::scan<CacheObject>(tr, page_query);
    auto it = std::ranges::begin(gen);
    auto end = std::ranges::end(gen);

    std::vector<std::pair<std::string, CacheObject>> rows;
    const int limit = fdb_page_read_limit(count);

    for (int n = 0; (limit == 0 || n < limit) && it != end; ++n, ++it) {
      rows.push_back(*it);
    }

    if (rows.empty()) {
      return -ENOENT;
    }

    const auto returned = (count == 0)
        ? std::size(rows) : std::min(std::size(rows), static_cast<std::size_t>(count));

    objs_info.reserve(returned);

    for (std::size_t i = 0; i < returned; ++i) {
      objs_info.push_back(std::move(rows[i].second));
    }

    if (returned < std::size(rows) && !objs_info.empty()) {
      continuation_token = objs_info.back().objName;
    }

    return 0;
  });
}



/*
  Key formats:
  <bucket-id>#<object-name>/versions/<score>/<version> --> stores versions in order
  <bucket-id>#<object-name>/score/<version> --> for reverse lookup of a version key using its score
*/
std::string FDBObjectDirectory::get_versions_subspace(const DoutPrefixProvider* dpp,
                                                      const std::string& bucket_id,
                                                      const std::string& obj_name)
{
  const std::string index = build_index(bucket_id, obj_name);
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
  return std::string(libfdb_key_view(fdbc::keyspace(index) / "versions"));
}

std::string FDBObjectDirectory::get_score_subspace(const DoutPrefixProvider* dpp,
                                                    const std::string& bucket_id,
                                                    const std::string& obj_name)
{
  const std::string index = build_index(bucket_id, obj_name);
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
  return std::string(libfdb_key_view(fdbc::keyspace(index) / "score"));
}

std::string FDBObjectDirectory::build_versions_index(const DoutPrefixProvider* dpp,
                                                     const std::string& bucket_id,
                                                     const std::string& obj_name,
                                                     const std::string& score,
                                                     const std::string& version)
{
  const std::string subspace = get_versions_subspace(dpp, bucket_id, obj_name);
  return subspace + std::string(libfdb_key_view(fdbc::key(score, version)));
}

std::string FDBObjectDirectory::build_version_score_index(const DoutPrefixProvider* dpp,
                                                          const std::string& bucket_id,
                                                          const std::string& obj_name,
                                                          const std::string& version)
{
  const std::string subspace = get_score_subspace(dpp, bucket_id, obj_name);
  return subspace + std::string(libfdb_key_view(fdbc::key(version)));
}

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::string key = build_index(bucket_id, obj_name);
    return lfdb::key_exists(tr, key);
  });
}


int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheObj* object, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::string key = build_index(object->bucketName, object->objName);
    lfdb::erase(tr, key);
    return 0;
  });
}

std::string FDBObjectDirectory::get_versions_range_end(const std::string& versions_subspace) const
{
    return versions_subspace + "\xff";
}

bool FDBObjectDirectory::scan_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& begin, const std::string& end, bool reverse, std::vector<std::pair<std::string, CacheObjectVersion>>& kvs, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
    auto range = lfdb::select{begin, end};
    range.options.reverse_order = reverse;

    for (auto&& [key, value] : lfdb::scan<CacheObjectVersion>(tr, range)) {
      kvs.emplace_back(std::string(key), std::move(value));
    }
    return !kvs.empty();
  });
}

bool FDBObjectDirectory::parse_version_key(
		    const std::string& versions_subspace,
		    const std::string& key,
		    std::string& score,
		    std::string& member) const
{
    size_t score_start = versions_subspace.size();
    size_t score_end = key.find('/', score_start);

    if (score_end == std::string::npos) {
        return false;
    }

    score = key.substr(score_start, score_end - score_start);
    member = key.substr(score_end + 1);

    return true;
}

int FDBObjectDirectory::fdb_add(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                int64_t score,
                                const std::string& version,
                                std::optional<CacheObjectVersion> params,
                                std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!params) {
    return -EINVAL;
  }
  return fdb_invoke(dpp, txn, [&](auto& tr) {
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :bucket_id " << bucket_id << dendl;
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :obj_name " << obj_name << dendl;

      std::string encoded_score = encode_score(score);
      std::string score_key = build_version_score_index(dpp, bucket_id, obj_name, version);

      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " score_key " << score_key << dendl;

      std::string existing;
      if (lfdb::get(tr, score_key, existing)) {
        std::string existing_versions_key = build_versions_index(dpp, bucket_id, obj_name, existing, version);
        lfdb::erase(tr, existing_versions_key);
      }

      std::string versions_key = build_versions_index(dpp, bucket_id, obj_name, encoded_score, version);
      lfdb::set(tr, versions_key, *params);
      lfdb::set(tr, score_key, encoded_score);
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " versions_key: " << versions_key << dendl;
      return 0;
    });
}

int FDBObjectDirectory::fdb_revrange(const DoutPrefixProvider* dpp, optional_yield y,
		    const std::string& bucket_id,
		    const std::string& obj_name,
		    const std::string& marker_version,
		    uint64_t count,
		    std::vector<CacheObjectVersion>& obj_versions,
		    std::string& continuation_token,
		    std::optional<std::reference_wrapper<Transaction>> txn)
{
continuation_token.clear(); 
obj_versions.clear();

const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() versions_subspace: " << versions_subspace << dendl;

return fdb_invoke(dpp, txn, [&](auto& tr) -> int {
  q::interval versions_query = q::prefix(versions_subspace);

  if (!marker_version.empty()) {
    std::string marker_score;
    const std::string score_key = build_version_score_index(dpp, bucket_id, obj_name, marker_version);

    if (!lfdb::get(tr, score_key, marker_score)) {
      ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "() marker version not found: " << marker_version << dendl;
      return -ENOENT;
    }

    const std::string marker_key = build_versions_index(dpp, bucket_id, obj_name, marker_score, marker_version);
    versions_query = q::ending_before(q::prefix(versions_subspace), marker_key);
  }

  if (q::is_empty(versions_query)) {
    return -ENOENT;
  }

  const auto page_query = q::with_options(versions_query, q::query_options{
      .result_limit = fdb_page_read_limit(count),
      .reverse_order = true});

  auto gen = lfdb::scan<CacheObjectVersion>(tr, page_query);
  auto it = std::ranges::begin(gen);
  auto end = std::ranges::end(gen);
  const int limit = fdb_page_read_limit(count);
  std::vector<std::pair<std::string, CacheObjectVersion>> rows;

  for (int n = 0; (limit == 0 || n < limit) && it != end; ++n, ++it) {
    rows.push_back(*it);
  }

  if (rows.empty()) {
    return -ENOENT;
  }

  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() count: " << count << dendl;

  for (const auto& [key, value] : rows) {
    obj_versions.push_back(value);
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() version: " << obj_versions.back().version << dendl;
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() user_id: " << obj_versions.back().user_id << dendl;
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() display_name: " << obj_versions.back().display_name << dendl;

    if (count && obj_versions.size() == count) {
      if (rows.size() > count) {
        continuation_token = obj_versions.back().version;
      }
      break;
    }
  }

  return 0;
  });
}

int FDBObjectDirectory::fdb_rem(const DoutPrefixProvider* dpp, optional_yield y,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                const std::string& version,
                                std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr) {
      std::string score_key = build_version_score_index(dpp, bucket_id, obj_name, version);
      std::string existing_score;
      bool found = lfdb::get(tr, score_key, existing_score);

      if (!found) {
        return -ENOENT;
      }

      std::string version_key = build_versions_index(dpp, bucket_id, obj_name, existing_score, version);
      lfdb::erase(tr, version_key);
      lfdb::erase(tr, score_key);
      return 0;
    });
}

int FDBObjectDirectory::fdb_remrangebyscore(const DoutPrefixProvider* dpp, optional_yield y,
                                            const std::string& bucket_id,
                                            const std::string& obj_name,
                                            int64_t min,
                                            int64_t max,
                                            std::optional<std::reference_wrapper<Transaction>> txn)
{
  const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);
  const std::string min_s = encode_score(min);
  const std::string max_s = encode_score(max);
  const auto score_range = q::intersection(q::prefix(versions_subspace), q::between(versions_subspace + min_s, versions_subspace + max_s + "\xff"));

  if (q::is_empty(score_range)) {
    return -ENOENT;
  }

  return fdb_invoke(dpp, txn, [&](auto& tr) -> int {
    lfdb::erase(tr, score_range);
    return 0;
  });
}

int FDBObjectDirectory::fdb_rank(const DoutPrefixProvider* dpp, optional_yield y,
                                 const std::string& bucket_id,
                                 const std::string& obj_name,
                                 const std::string& member,
                                 std::string& index,
                                 std::optional<std::reference_wrapper<Transaction>> txn)
{
  const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

  return fdb_invoke(dpp, txn, [&](auto& tr) -> int {
    const auto kvs = lfdb::collect<CacheObjectVersion>(tr, q::prefix(versions_subspace));

    if (kvs.empty()) {
      return -ENOENT;
    }

    for (size_t i = 0; i < kvs.size(); ++i) {
      if (kvs[i].second.version == member) {
        index = std::to_string(i);
        return 0;
      }
    }

    return -ENOENT;
  });
}

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, std::optional<std::reference_wrapper<Transaction>> txn)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(creation_time.time_since_epoch()).count();

  ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;
  return fdb_add(dpp, y, bucket_id, obj_name, score, version, params, txn);
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_rem(dpp, y, bucket_id, obj_name, version, txn);
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, std::optional<std::reference_wrapper<Transaction>> txn)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(
      creation_time.time_since_epoch()).count();
  return fdb_remrangebyscore(dpp, y, bucket_id, obj_name, score, score, txn);
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, optional_yield y, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, std::optional<std::reference_wrapper<Transaction>> txn)
{
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " obj_name: " << obj_name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " marker_version: " << marker_version << dendl;
  std::vector<std::string> members;
  auto ret = fdb_revrange(dpp, y, bucket_id, obj_name, marker_version, count, obj_versions, continuation_token, txn);
  if (ret < 0 ) {
    return ret;
  }
  return 0;
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr){
    std::string key = build_index(block);
    return lfdb::key_exists(tr, key);
  });
}

template<AssociativeContainer Container>
int FDBBlockDirectory::set_values(const DoutPrefixProvider* dpp,
                                  CacheBlock& block,
                                  Container& fdbValues,
                                  optional_yield y)
{
  std::string hosts;

  auto add_value = [&](const std::string& key, const auto& value) {
    using ValueType = typename Container::value_type;

    std::string str_value;

    if constexpr (std::is_convertible_v<decltype(value), std::string>) {
      str_value = value;
    } else {
      str_value = std::to_string(value);
    }

    if constexpr (requires(Container c, ValueType v) {
                    c.push_back(v);
                  }) {
      fdbValues.push_back(ValueType{key, str_value});
    } else {
      fdbValues.insert(ValueType{key, str_value});
    }
  };

  int ret = -1;

  add_value("blockID", block.blockID);
  add_value("version", block.version);

  if ((ret = check_bool(std::to_string(block.deleteMarker))) != -EINVAL) {
    block.deleteMarker = (ret != 0);
  } else {
    ldpp_dout(dpp, 0)
      << "BlockDirectory::" << __func__
      << "() ERROR: Invalid bool value for delete marker"
      << dendl;
    return -EINVAL;
  }

  add_value("deleteMarker", block.deleteMarker);

  if ((ret = check_bool(std::to_string(block.invalid))) != -EINVAL) {
    block.invalid = (ret != 0);
  } else {
    ldpp_dout(dpp, 0)
      << "BlockDirectory::" << __func__
      << "() ERROR: Invalid bool value for invalid"
      << dendl;
    return -EINVAL;
  }
  add_value("invalid", block.invalid);
  add_value("size", block.size);
  add_value("globalWeight", block.globalWeight);
  add_value("objName", block.cacheObj.objName);
  add_value("bucketName", block.cacheObj.bucketName);
  add_value("creationTime", block.cacheObj.creationTime);

  if ((ret = check_bool(std::to_string(block.cacheObj.dirty))) != -EINVAL) {
    block.cacheObj.dirty = (ret != 0);
  } else {
    ldpp_dout(dpp, 0)
      << "BlockDirectory::" << __func__
      << "() ERROR: Invalid bool value"
      << dendl;
    return -EINVAL;
  }

  add_value("dirty", block.cacheObj.dirty);

  hosts.clear();
  for (const auto& host : block.cacheObj.hostsList) {
    if (hosts.empty())
      hosts = host + "_";
    else
      hosts += host + "_";
  }

  if (!hosts.empty())
    hosts.pop_back();

  add_value("hosts", hosts);
  add_value("etag", block.cacheObj.etag);
  add_value("objSize", block.cacheObj.size);
  add_value("userId", block.cacheObj.user_id);
  add_value("displayName", block.cacheObj.display_name);
  add_value("acl", block.cacheObj.acl);

  add_value("attrsCount", block.cacheObj.attrs.size());

  for (const auto& [key, bl] : block.cacheObj.attrs) {
    add_value("attr_" + key, bl.to_str());
  }

  return 0;
}

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  return fdb_invoke(dpp, txn, [&](auto& tr){
    std::string key = build_index(block);
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    std::map<std::string, std::string> fdbValues;
    auto ret = set_values(dpp, *block, fdbValues, y);
    if (ret < 0) {
      return ret;
    }

    lfdb::set(tr, key, fdbValues);
    return 0;
  });
}

/* In this function, if a transaction is supplied, all the blocks will be set in the same transaction */
int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<std::reference_wrapper<Transaction>> txn)
{
  return fdb_invoke(dpp, txn, [&](auto& tr){
    for (auto& block : blocks) {
      std::string key = build_index(&block);
      ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;
      std::map<std::string, std::string> fdbValues;
      auto ret = set_values(dpp, block, fdbValues, y);
      if (ret < 0) {
        return ret;
      }

      lfdb::set(tr, key, fdbValues);
    }
    return 0;
  });
}

int FDBBlockDirectory::populate_block(CacheBlock* block, const std::map<std::string, std::string>& kvs) const
{
  if (!block) return -EINVAL;

  block->blockID = std::stoull(kvs.at("blockID"));
  block->version = kvs.at("version");
  block->deleteMarker = (kvs.at("deleteMarker") == "1");
  block->invalid = (kvs.at("invalid") == "1");
  block->size = std::stoull(kvs.at("size"));
  block->globalWeight = std::stoull(kvs.at("globalWeight"));
  block->cacheObj.objName = kvs.at("objName");
  block->cacheObj.bucketName = kvs.at("bucketName");
  block->cacheObj.creationTime = kvs.at("creationTime");
  block->cacheObj.dirty = (kvs.at("dirty") == "1");
  block->cacheObj.hostsList.clear();
  boost::split(block->cacheObj.hostsList, kvs.at("hosts"), boost::is_any_of("_"));
  block->cacheObj.etag = kvs.at("etag");
  block->cacheObj.size = std::stoull(kvs.at("objSize"));
  block->cacheObj.user_id = kvs.at("userId");
  block->cacheObj.display_name = kvs.at("displayName");
  block->cacheObj.acl = kvs.at("acl");
  block->cacheObj.attrs.clear();

  const size_t attrs_count = std::stoull(kvs.at("attrsCount"));
  size_t found_attrs = 0;

  for (const auto& [field, value] : kvs) {
    if (!field.starts_with("attr_")) continue;
    ceph::buffer::list bl;
    bl.append(value);
    block->cacheObj.attrs[field.substr(5)] = std::move(bl);
    ++found_attrs;
  }

  if (found_attrs != attrs_count) {
    return -EINVAL;
  }

  return 0;
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  try {
    std::string key = build_index(block);
    std::map<std::string, std::string> out_kvs;

    return fdb_invoke(dpp, txn, [&](auto& tr) {
      if (!lfdb::get(tr, key, out_kvs)) {
        ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: get function returned false!" << dendl;
        return -ENOENT;
      }

      int ret = populate_block(block, out_kvs);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: failed to populate block" << dendl;
      }
      return ret;
    });
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, optional_yield y, std::vector<CacheBlock>& blocks, std::optional<std::reference_wrapper<Transaction>> txn)
{
  try {
    return fdb_invoke(dpp, txn, [&](auto& tr) {
      std::vector<std::map<std::string, std::string>> out_kvs(blocks.size());

      for (size_t i = 0; i < blocks.size(); ++i) {
        auto& block = blocks[i];
        std::string key = build_index(&block);

        ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

        if (!lfdb::get(tr, key, out_kvs[i])) {
          ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: get function returned false!" << dendl;
          return -ENOENT;
        }
      }

      for (size_t i = 0; i < blocks.size(); ++i) {
        int ret = populate_block(&blocks[i], out_kvs[i]);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: failed to populate block" << dendl;
          return ret;
        }
      }

      return 0;
    });
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EINVAL;
  }
}

int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "(): null block pointer" << dendl;
    return -EINVAL;
  }


  auto ret = this->get(dpp, y, block, txn);
  if (ret < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
    return ret;
  }

  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };

  copyBlock.version = block->version;
  copyBlock.deleteMarker = block->deleteMarker;
  copyBlock.size = block->size;
  copyBlock.globalWeight = block->globalWeight;
  copyBlock.cacheObj.dirty = block->cacheObj.dirty;
  copyBlock.cacheObj.creationTime = block->cacheObj.creationTime;
  copyBlock.cacheObj.hostsList = block->cacheObj.hostsList;
  copyBlock.cacheObj.etag = block->cacheObj.etag;
  copyBlock.cacheObj.size = block->cacheObj.size;
  copyBlock.cacheObj.user_id = block->cacheObj.user_id;
  copyBlock.cacheObj.display_name = block->cacheObj.display_name;
  copyBlock.cacheObj.acl = block->cacheObj.acl;
  copyBlock.cacheObj.attrs = block->cacheObj.attrs;

  return this->set(dpp, y, &copyBlock, txn);
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, std::optional<std::reference_wrapper<Transaction>> txn)
{
  if (block == nullptr) {
    return -EINVAL;
  }

  return fdb_invoke(dpp, txn, [&](auto& tr) {
    std::string key = build_index(block);
    lfdb::erase(tr, key);
    return 0;
  });
}


int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& field, std::string& value, std::optional<std::reference_wrapper<Transaction>> txn)
{
  int ret = -1;
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  ret = exist_key(dpp, y, block, txn);
  if (ret < 0) return ret;
  if (ret == 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
    return -ENOENT;
  }

  ret = this->get(dpp, y, block, txn);
  if (ret < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
    return ret;
  }

  if (field == "blockID") {
    block->blockID = std::stoull(value);
  }
  else if (field == "version") {
    block->version = value;
  }
  else if (field == "deleteMarker") {
    block->deleteMarker = (value == "1");
  }
  else if (field == "size") {
    block->size = std::stoull(value);
  }
  else if (field == "globalWeight") {
    block->globalWeight = std::stoull(value);
  }
  else if (field == "invalid") {
    block->invalid = (value == "1" || value == "true");
  }
  else if (field == "objName") {
    block->cacheObj.objName = value;
  }
  else if (field == "bucketName") {
    block->cacheObj.bucketName = value;
  }
  else if (field == "dirty") {
    block->cacheObj.dirty = (value == "1");
  }
  else if (field == "creationTime") {
    block->cacheObj.creationTime = value;
  }
  else if (field == "hosts") {
    block->cacheObj.hostsList.insert(value);
  }
  else if (field == "etag") {
    block->cacheObj.etag = value;
  }
  else if (field == "objSize") {
    block->cacheObj.size = std::stoull(value);
  }
  else if (field == "userId") {
    block->cacheObj.user_id = value;
  }
  else if (field == "displayName") {
    block->cacheObj.display_name = value;
  }

  return this->set(dpp, y, block, txn);

}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, optional_yield y, CacheBlock* block, const std::string& value, std::optional<std::reference_wrapper<Transaction>> txn)
{
  int ret = this->get(dpp, y, block, txn);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrieve the object." << dendl;
    return ret;
  }

  block->cacheObj.hostsList.erase(value);

  ret = this->set(dpp, y, block, txn);
  if (ret < 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not update the block." << dendl;
    return ret;
  }

  return 0;
}

} // namespace rgw::d4n
