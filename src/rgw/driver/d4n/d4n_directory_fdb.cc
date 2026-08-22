#include <algorithm>
#//include <limits>
#include <type_traits>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_fdb.h"

namespace rgw::d4n {

using std::map;
using std::string;
namespace fdbc = lfdb::layer::content;
namespace q    = lfdb::query;

static std::string encode_score(int64_t score)
{
  return fmt::format("{:019d}", score);
}

// Lease metadata structure
struct LeaseData {
  uint64_t expiry = 0;
  std::string holder_id;
  std::string token;
  uint64_t tick_count = 0;

  bool is_active(uint64_t now) const {
    return expiry > now;
  }
};

// Build FDB key for a lease on a resource using FDB directory layer
std::string make_lease_key(const std::string& resource_name)
{
  return std::string(libfdb_key_view(fdbc::keyspace("d4n") / "leases" / resource_name));
}

// Get current time in seconds since epoch
uint64_t current_time_seconds()
{
  const auto now = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  return static_cast<uint64_t>(std::max<int64_t>(now, 0));
}

// Calculate expiry time from TTL
uint64_t calculate_expiry(uint64_t ttl_seconds)
{
  const auto now = current_time_seconds();
  if (ttl_seconds > std::numeric_limits<uint64_t>::max() - now) {
    return std::numeric_limits<uint64_t>::max();
  }
  return now + ttl_seconds;
}

int FDBLease::acquire(const DoutPrefixProvider* dpp,
                             const std::string& resource_name,
                             const std::string& holder_id,
                             const std::string& token,
                             uint64_t ttl_seconds)
{
  if (ttl_seconds == 0) {
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
      const auto now = current_time_seconds();

      // Check if resource already has an active lease
      LeaseData existing_lease;
      if (lfdb::get(tr, fdb_key, existing_lease)) {
        if (existing_lease.is_active(now)) {
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
        .expiry = calculate_expiry(ttl_seconds),
        .holder_id = holder_id,
        .token = token,
        .tick_count = 0
      };
      lfdb::set(tr, fdb_key, new_lease);

      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " acquired lease: resource=" << resource_name
                         << " holder=" << holder_id
                         << " expiry=" << new_lease.expiry << dendl;
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
                           uint64_t ttl_seconds,
                           uint64_t max_ticks)
{
  if (ttl_seconds == 0) {
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
      LeaseData existing_lease;
      const auto now = current_time_seconds();

      if (!lfdb::get(tr, fdb_key, existing_lease)) {
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " lease not found: resource=" << resource_name << dendl;
        return -ENOENT;
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

      // Renew the lease with incremented tick count
      LeaseData renewed_lease{
        .expiry = calculate_expiry(ttl_seconds),
        .holder_id = holder_id,
        .token = token,
        .tick_count = existing_lease.tick_count + 1
      };
      lfdb::set(tr, fdb_key, renewed_lease);

      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " renewed lease: resource=" << resource_name
                         << " holder=" << holder_id
                         << " new_expiry=" << renewed_lease.expiry
                         << " tick_count=" << renewed_lease.tick_count << dendl;
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
        ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                           << " lease not found: resource=" << resource_name << dendl;
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

      // Release the lease
      lfdb::erase(tr, fdb_key);

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

bool FDBLease::any_active(const DoutPrefixProvider* dpp,
                          const std::string& resource_prefix)
{
  if (resource_prefix.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_prefix" << dendl;
    return false;
  }

  // Build prefix for scanning all leases matching this resource
  const auto prefix_key = make_lease_key(resource_prefix);

  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) -> bool {
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
      const auto now = current_time_seconds();
      int total_leases = rows.size();
      int active_leases = 0;
      std::vector<std::string> expired_keys;

      for (const auto& [key, lease] : rows) {
        if (lease.is_active(now)) {
          active_leases++;
          ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                             << " found active lease: holder=" << lease.holder_id
                             << " expiry=" << lease.expiry << dendl;
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

      bool has_active = (active_leases > 0);
      ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                         << " resource_prefix=" << resource_prefix
                         << " total_leases=" << total_leases
                         << " active_leases=" << active_leases
                         << " has_active=" << has_active << dendl;
      return has_active;
    });

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource_prefix=" << resource_prefix
                      << ": " << e.what() << dendl;
    return false;
  }
}

bool FDBLease::is_active(const DoutPrefixProvider* dpp,
                                const std::string& resource_name,
                                const std::string& holder_id,
                                const std::string& token)
{
  if (resource_name.empty() || holder_id.empty() || token.empty()) {
    ldpp_dout(dpp, 10) << "FDBLease::" << __func__
                       << " empty resource_name/holder_id/token" << dendl;
    return false;
  }

  const auto fdb_key = make_lease_key(resource_name);

  try {
    // Atomic check-and-cleanup in single transaction to avoid TOCTOU race
    return lfdb::make_transactor(FDBdb)([&](auto& tr) -> bool {
      LeaseData lease;
      if (!lfdb::get(tr, fdb_key, lease)) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " lease not found: resource=" << resource_name << dendl;
        return false;
      }

      // Check expiry and opportunistically cleanup if expired
      const auto now = current_time_seconds();
      if (!lease.is_active(now)) {
        ldpp_dout(dpp, 20) << "FDBLease::" << __func__
                           << " lease expired: resource=" << resource_name
                           << " expiry=" << lease.expiry << " - deleting" << dendl;
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
                         << " expiry=" << lease.expiry << dendl;
      return true;
    });

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBLease::" << __func__
                      << " FDB error for resource=" << resource_name
                      << ": " << e.what() << dendl;
    return false;
  }
}

int FDBDirectory::get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val)
{
  std::map<std::string, std::string> kvs;
  if (!lfdb::get(FDBdb, key, kvs)) {
    return -ENOENT;
  }
  const auto it = kvs.find(field);
  if (std::end(kvs) == it) {
    return -ENOENT;
  }
  out_val = it->second;
  return 0;
}

int FDBDirectory::set_kv(const DoutPrefixProvider* dpp, optional_yield y,
                    const std::string& key,
                    const std::string& field,
                    const std::string& val)
{
  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      existing[field] = val;
      lfdb::set(tr, key, existing);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBDirectory::get_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                      const std::string& key,
                      const std::vector<std::string>& fields,
                      std::map<std::string, std::string>& out_vals)
{
  std::map<std::string, std::string> kvs;
  if (!lfdb::get(FDBdb, key, kvs)) {
    return -ENOENT;
  }
  out_vals.clear();
  for (const auto& field : fields) {
    const auto it = kvs.find(field);
    if (std::end(kvs) == it) {
      return -ENOENT;
    }
    out_vals[field] = it->second;
  }
  return 0;
}

int FDBDirectory::set_kv_multi(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& key,
                        const std::map<std::string, std::string>& vals)
{
  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      for (const auto& [field, value] : vals) {
        existing[field] = value;
      }
      lfdb::set(tr, key, existing);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBDirectory::set_kv_if_not_exists(const DoutPrefixProvider* dpp, optional_yield y,
                                        const std::string& key,
                                        const std::string& field,
                                        const std::string& val)
{
  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      std::map<std::string, std::string> existing;
      lfdb::get(tr, key, existing);
      if (existing.find(field) == existing.end()) {
        existing[field] = val;
        lfdb::set(tr, key, existing);
      }
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0) << "FDBDirectory::" << __func__ << "() ERROR: " << e.what() << dendl;
    return -EIO;
  }
}

int FDBBucketDirectory::exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y) 
{
  return lfdb::key_exists(FDBdb, bucket_id);
}

//FIXME: this is a dummy function and should be updated.
int FDBBucketDirectory::del(const DoutPrefixProvider* dpp, const std::string& bucket_id, optional_yield y)
{
  return 0;
}

int FDBBucketDirectory::add_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, std::optional<CacheObject> params, optional_yield y, Pipeline* pipeline)
{
  return fdb_add(dpp, bucket_id, 0, object_name, std::move(params), y);
}

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y)
{
  return fdb_rem(dpp, bucket_id, object_name, y);
}

int FDBBucketDirectory::list_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start_token, const std::string& prefix, const std::string& marker, uint64_t count, bool marker_inclusive, std::vector<CacheObject>& objs_info, std::string& continuation_token, optional_yield y)
{
  return fdb_scan(dpp, bucket_id, marker, prefix, count, marker_inclusive, objs_info, continuation_token, y);
}

//Key form is <bucket-id>/objects/<object-name>
std::string FDBBucketDirectory::build_object_index(const std::string& bucket_id, const std::string& obj_name)
{
  return std::string(libfdb_key_view(fdbc::keyspace(bucket_id) / "objects")) + obj_name;
}


int FDBBucketDirectory::collect_range(
    const DoutPrefixProvider* dpp,
    const FDBRange& range,
    const std::string& base,
    uint64_t count,
    std::vector<CacheObject>& objs_info,
    std::string& continuation_token)
{
  bool have_more = false;

  objs_info.clear();
  objs_info.reserve(count);

  for (auto&& block : lfdb::block_generator<CacheObject>(FDBdb, lfdb::select{range.begin, range.end})) {
    for (auto&& [key, value] : block) {
      if (count && objs_info.size() == count) {
        have_more = true;
        break;
      }

      objs_info.emplace_back(std::move(value));
      objs_info.back().objName.assign(
          key.data() + base.size(),
          key.size() - base.size());
    }
    if (have_more) {
      break;
    }
  }

  if (have_more && !objs_info.empty()) {
    continuation_token = objs_info.back().objName;
  }

  return 0;
}

FDBRange FDBBucketDirectory::build_range(
    const std::string& base,
    const std::string& start,
    bool inclusive)
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

int FDBBucketDirectory::fdb_add(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                double score,
                                const std::string& member,
                                std::optional<CacheObject> params,
                                optional_yield y)
{
  if (!params) {
    return -EINVAL;
  }

  try {
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " :member " << member << dendl;
    std::string member_key = build_object_index(bucket_id, member);

    lfdb::set(FDBdb, member_key, *params);

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " :member_key " << member_key << dendl;

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBucketDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& member,
                                optional_yield y)
{
  try {
    lfdb::erase(FDBdb, build_object_index(bucket_id, member));

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

// Returns count+1 so callers can detect whether a continuation token is needed:
// if FDB returns count+1 rows, there are more; if fewer, the range is exhausted.
// Returns 0 when count==0 (unbounded).
static int fdb_page_read_limit(uint64_t count)
{
  if (count == 0) return 0;
  return count < static_cast<uint64_t>(std::numeric_limits<int>::max())
       ? static_cast<int>(count + 1)
       : std::numeric_limits<int>::max();
}

int FDBBucketDirectory::fdb_scan(const DoutPrefixProvider* dpp,
                             const std::string& bucket_id,
                             const std::string& start_token,
                             const std::string& prefix,
                             uint64_t count,
                             bool marker_inclusive,
                             std::vector<CacheObject>& objs_info,
                             std::string& continuation_token,
                             optional_yield y)
{
  continuation_token.clear();

  try {
    const std::string base         = std::string(libfdb_key_view(fdbc::keyspace(bucket_id) / "objects"));
    const std::string prefix_begin = base + prefix;
    const std::string marker_key   = base + start_token;

    // Query algebra handles marker clamping implicitly: prefix_starting_at/after
    // intersects the prefix range with the marker bound, so a marker outside
    // the prefix range yields an empty interval.
    const auto object_query = start_token.empty()
        ? q::prefix(prefix_begin)
        : marker_inclusive
          ? q::prefix_starting_at(prefix_begin, marker_key)
          : q::prefix_starting_after(prefix_begin, marker_key);

    if (q::is_empty(object_query)) { return -ENOENT; }

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__
                       << "() prefix_begin: " << prefix_begin << dendl;

    const auto page_query = q::with_options(object_query,
        q::query_options{.result_limit = fdb_page_read_limit(count)});

    return lfdb::make_transactor(FDBdb)([&](auto& tr) -> int {
      auto gen = lfdb::scan<CacheObject>(tr, page_query);
      auto it  = std::ranges::begin(gen);
      auto end = std::ranges::end(gen);

      std::vector<std::pair<std::string, CacheObject>> rows;
      const int limit = fdb_page_read_limit(count);
      for (int n = 0; (limit == 0 || n < limit) && it != end; ++n, ++it) {
        rows.push_back(*it);
      }

      if (rows.empty()) { return -ENOENT; }

      const auto returned = (count == 0)
          ? std::size(rows)
          : std::min(std::size(rows), static_cast<std::size_t>(count));

      objs_info.reserve(returned);
      for (std::size_t i = 0; i < returned; ++i) {
        objs_info.push_back(std::move(rows[i].second));
      }

      if (returned < std::size(rows) && !objs_info.empty()) {
        continuation_token = objs_info.back().objName;
      }
      return 0;
    });

  } catch (const lfdb::libfdb_exception& e) {

    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::"
        << __func__
        << "() ERROR: "
        << e.what()
        << dendl;

    return -EINVAL;
  }
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

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y)
{
  std::string key = build_index(bucket_id, obj_name);
  return lfdb::key_exists(FDBdb, key);
}

int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  lfdb::erase(FDBdb, build_index(object->bucketName, object->objName));
  return 0;
}

std::string FDBObjectDirectory::get_versions_range_end(const std::string& versions_subspace) const
{
    return versions_subspace + "\xff";
}

bool FDBObjectDirectory::scan_versions(
    const DoutPrefixProvider* dpp,
    const std::string& begin,
    const std::string& end,
    bool reverse,
    std::vector<std::pair<std::string, CacheObjectVersion>>& kvs)
{
    try {

        auto range = lfdb::select{begin, end};
        range.options.reverse_order = reverse;


        for (auto&& block :
             lfdb::block_generator<CacheObjectVersion>(
                 FDBdb,
                 range)) {

            kvs.insert(
                kvs.end(),
                std::begin(block),
                std::end(block));
        }

        return !kvs.empty();

    } catch (const lfdb::libfdb_exception& e) {

        ldpp_dout(dpp, 0)
            << "FDBObjectDirectory::scan_versions ERROR: "
            << e.what()
            << dendl;

        return false;
    }
}

bool FDBObjectDirectory::parse_version_key(
    const std::string& versions_subspace,
    const std::string& key,
    std::string& score,
    std::string& member) const
{
    size_t score_start = versions_subspace.size();

    size_t score_end =
        key.find('/', score_start);


    if (score_end == std::string::npos) {
        return false;
    }


    score = key.substr(
        score_start,
        score_end - score_start);


    member =
        key.substr(score_end + 1);


    return true;
}


int FDBObjectDirectory::fdb_add(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                int64_t score,
                                const std::string& version,
                                std::optional<CacheObjectVersion> params,
                                optional_yield y)
{
  if (!params) {
    return -EINVAL;
  }
  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :bucket_id " << bucket_id << dendl;
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :obj_name " << obj_name << dendl;

      std::string encoded_score = encode_score(score);
      std::string score_key = build_version_score_index(dpp, bucket_id, obj_name, version);

      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " score_key " << score_key << dendl;
      std::string existing;
      if (lfdb::get(tr, score_key, existing)){
        std::string existing_versions_key = build_versions_index(dpp, bucket_id, obj_name, existing, version);
        lfdb::erase(tr, existing_versions_key);
      }

      std::string versions_key = build_versions_index(dpp, bucket_id, obj_name, encoded_score, version);
      lfdb::set(tr, versions_key, *params);
      lfdb::set(tr, score_key, encoded_score);
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " versions_key: " << versions_key << dendl;
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBObjectDirectory::fdb_revrange(const DoutPrefixProvider* dpp,
                                    const std::string& bucket_id,
                                    const std::string& obj_name,
                                    const std::string& marker_version,
                                    uint64_t count,
                                    std::vector<CacheObjectVersion>& obj_versions,
                                    std::string& continuation_token,
                                    optional_yield y)
{
  continuation_token.clear();

  try {
    const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

    // Build the eligible key range using query algebra.  Both branches produce
    // q::interval so the variable can hold either without type erasure.
    q::interval versions_query = q::prefix(versions_subspace);

    if (!marker_version.empty()) {
      // Point lookup: resolve the marker's encoded score via the reverse index
      // rather than scanning for it.
      std::string marker_score;
      const std::string score_key =
          build_version_score_index(dpp, bucket_id, obj_name, marker_version);
      if (!lfdb::get(FDBdb, score_key, marker_score)) {
        ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__
                           << "() marker version not found: " << marker_version << dendl;
        return -ENOENT;
      }
      // ending_before gives [versions_subspace, marker_key) -- marker itself excluded.
      const std::string marker_key =
          build_versions_index(dpp, bucket_id, obj_name, marker_score, marker_version);
      versions_query = q::ending_before(q::prefix(versions_subspace), marker_key);
    }

    if (q::is_empty(versions_query)) { return -ENOENT; }

    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__
                       << "() versions_subspace: " << versions_subspace << dendl;

    const auto page_query = q::with_options(versions_query,
        q::query_options{
          .result_limit = fdb_page_read_limit(count),
          .reverse_order = true
        });

    return lfdb::make_transactor(FDBdb)([&](auto& tr) -> int {
      auto gen = lfdb::scan<CacheObjectVersion>(tr, page_query);
      auto it  = std::ranges::begin(gen);
      auto end = std::ranges::end(gen);

      std::vector<std::pair<std::string, CacheObjectVersion>> rows;
      const int limit = fdb_page_read_limit(count);
      for (int n = 0; (0 == limit || n < limit) && it != end; ++n, ++it) {
        rows.push_back(*it);
      }

      if (rows.empty()) { return -ENOENT; }

      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__
                         << "() count: " << count << dendl;

      for (const auto& row : rows) {
        const auto& value = row.second;
        obj_versions.push_back(value);
        ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__
                           << "() version: " << obj_versions.back().version << dendl;
        ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__
                           << "() user_id: " << obj_versions.back().user_id << dendl;
        ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__
                           << "() display_name: " << obj_versions.back().display_name << dendl;
        if (count && obj_versions.size() == count) {
          if (rows.size() > count) {
            continuation_token = obj_versions.back().version;
          }
          break;
        }
      }
      return 0;
    });

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }
}

int FDBObjectDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                const std::string& version,
                                optional_yield y)
{
  try
  {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {

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

  } catch (const  lfdb::libfdb_exception& e) {
      ldpp_dout(dpp, 0)
        << "FDBObjectDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
      return -EINVAL;
    }
    return 0;
}


int FDBObjectDirectory::fdb_remrangebyscore(const DoutPrefixProvider* dpp,
                                            const std::string& bucket_id,
                                            const std::string& obj_name,
                                            int64_t min,
                                            int64_t max,
                                            optional_yield y)
try
{
  const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);
  const std::string min_s = encode_score(min);
  const std::string max_s = encode_score(max);

  // Key layout: versions_subspace + score(19 digits) + "/" + version_id
  // All keys with scores in [min, max] occupy the byte range:
  //   [versions_subspace + min_s, versions_subspace + max_s + "\xff")
  const auto score_range = q::intersection(
      q::prefix(versions_subspace),
      q::between(versions_subspace + min_s,
                 versions_subspace + max_s + "\xff"));

  if (q::is_empty(score_range)) { return -ENOENT; }

  lfdb::erase(FDBdb, score_range);
  return 0;

} catch (const lfdb::libfdb_exception& e) {
  ldpp_dout(dpp, 0)
    << "FDBObjectDirectory::" << __func__
    << "() ERROR: " << e.what()
    << dendl;
  return -EINVAL;
}


int FDBObjectDirectory::fdb_rank(
    const DoutPrefixProvider* dpp,
    const std::string& bucket_id,
    const std::string& obj_name,
    const std::string& member,
    std::string& index,
    optional_yield y)
{
  try {
    const std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

    const auto kvs = lfdb::collect<CacheObjectVersion>(FDBdb, q::prefix(versions_subspace));

    if (kvs.empty())
      return -ENOENT;

    for (size_t i = 0; i < kvs.size(); ++i) {
      if (kvs[i].second.version == member) {
        index = std::to_string(i);
        return 0;
      }
    }

    return -ENOENT;

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }
  return 0;
}

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(
      creation_time.time_since_epoch()).count();
  ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;
  return fdb_add(dpp, bucket_id, obj_name, score, version, params, y);
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y)
{
  return fdb_rem(dpp, bucket_id, obj_name, version, y);
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, ceph::real_time creation_time, optional_yield y)
{
  auto score = std::chrono::duration_cast<std::chrono::nanoseconds>(
      creation_time.time_since_epoch()).count();
  return fdb_remrangebyscore(dpp, bucket_id, obj_name, score, score, y);
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& marker_version, uint64_t count, std::vector<CacheObjectVersion>& obj_versions, std::string& continuation_token, optional_yield y)
{
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " obj_name: " << obj_name << dendl;
  ldpp_dout(dpp, 20) << "D4NFilterBucket::" << __func__ << " marker_version: " << marker_version << dendl;
  std::vector<std::string> members;
  auto ret = fdb_revrange(dpp, bucket_id, obj_name, marker_version, count, obj_versions, continuation_token, y);
  if (ret < 0 ) {
    return ret;
  }
  return 0;
}

int FDBBlockDirectory::exist_key(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  return lfdb::key_exists(FDBdb, key);
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

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y, Pipeline* pipeline)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  map<string, string> fdbValues;

  auto ret = set_values(dpp, *block, fdbValues, y);
  if (ret < 0) {
    return ret;
  }

  lfdb::set(FDBdb, key, fdbValues);
  return 0;
}

int FDBBlockDirectory::set(const DoutPrefixProvider* dpp,
                           std::vector<CacheBlock>& blocks,
                           optional_yield y)
try
{
  struct PendingWrite {
    std::string key;
    std::map<std::string, std::string> values;
  };

  std::vector<PendingWrite> writes;
  writes.reserve(blocks.size());

  // ---------- Preparation phase (outside transactions) ----------
  for (auto& block : blocks) {
    PendingWrite w;

    w.key = build_index(&block);

    ldpp_dout(dpp, 20)
        << "FDBBlockDirectory::" << __func__
        << "(): index is: " << w.key
        << dendl;

    int ret = set_values(dpp, block, w.values, y);
    if (ret < 0) {
      return ret;
    }

    writes.emplace_back(std::move(w));
  }

  // ---------- Commit phase (chunked transactions) ----------

  for (size_t start = 0; start < writes.size(); start += COMMIT_SIZE) {
    const size_t end = std::min(start + COMMIT_SIZE, writes.size());

    int ret = lfdb::make_transactor(FDBdb)([&](auto& tr) {

      for (size_t i = start; i < end; ++i) {
        lfdb::set(tr, writes[i].key, writes[i].values);
      }

      return 0;
    });

    if (ret < 0) {
      return ret;
    }
  }

  return 0;

} catch (const lfdb::libfdb_exception& e) {
  ldpp_dout(dpp, 0)
      << "FDBBlockDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
  return -EINVAL;
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp,
                           CacheBlock* block,
                           optional_yield y)
{
  if (!block) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: null block pointer" << dendl;
    return -EINVAL;
  }

  try {
    std::string key = build_index(block);
    std::map<std::string, std::string> out_kvs;

    if (!lfdb::get(FDBdb, key, out_kvs)) {
      ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                        << "() ERROR: get function returned false!"
                        << dendl;
      return -ENOENT;
    }

    CacheBlock tmp;

    auto get_value = [&](const std::string& field, std::string& value) {
      auto it = out_kvs.find(field);
      if (it == out_kvs.end()) {
        ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                          << "() missing field: " << field << dendl;
        return false;
      }
      value = it->second;
      return true;
    };

    std::string value;

    if (!get_value("blockID", value))
      return -EINVAL;
    tmp.blockID = std::stoull(value);

    if (!get_value("version", tmp.version))
      return -EINVAL;

    if (!get_value("deleteMarker", value))
      return -EINVAL;
    tmp.deleteMarker = (value == "1");

    if (!get_value("size", value))
      return -EINVAL;
    tmp.size = std::stoull(value);

    if (!get_value("globalWeight", value))
      return -EINVAL;
    tmp.globalWeight = std::stoull(value);

    if (!get_value("objName", tmp.cacheObj.objName) ||
        !get_value("bucketName", tmp.cacheObj.bucketName) ||
        !get_value("creationTime", tmp.cacheObj.creationTime))
      return -EINVAL;

    if (!get_value("dirty", value))
      return -EINVAL;
    tmp.cacheObj.dirty = (value == "1");

    if (!get_value("hosts", value))
      return -EINVAL;

    if (!value.empty()) {
      boost::split(tmp.cacheObj.hostsList,
                   value,
                   boost::is_any_of("_"));
    }

    if (!get_value("etag", tmp.cacheObj.etag))
      return -EINVAL;

    if (!get_value("objSize", value))
      return -EINVAL;
    tmp.cacheObj.size = std::stoull(value);

    if (!get_value("userId", tmp.cacheObj.user_id) ||
        !get_value("displayName", tmp.cacheObj.display_name) ||
        !get_value("acl", tmp.cacheObj.acl))
      return -EINVAL;

    if (!get_value("attrsCount", value))
      return -EINVAL;

    size_t attrs_count = std::stoull(value);
    size_t found_attrs = 0;

    for (const auto& [k, v] : out_kvs) {
      if (!k.starts_with("attr_"))
        continue;

      ceph::buffer::list bl;
      bl.append(v);
      tmp.cacheObj.attrs[k.substr(5)] = std::move(bl);
      found_attrs++;
    }

    if (found_attrs != attrs_count) {
      ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                        << "() ERROR: expected "
                        << attrs_count << " attrs but found "
                        << found_attrs << dendl;
      return -EINVAL;
    }

    *block = std::move(tmp);
    return 0;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "() ERROR: " << e.what()
                      << dendl;
    return -EINVAL;
  }
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp,
                           std::vector<CacheBlock>& blocks,
                           optional_yield y)
{
  try {
    std::vector<std::map<std::string, std::string>> out_kvs(blocks.size());


    // ---------- FETCH PHASE (chunked transactions) ----------
    for (size_t start = 0; start < blocks.size(); start += COMMIT_SIZE) {
      const size_t end = std::min(start + COMMIT_SIZE, blocks.size());

      int ret = lfdb::make_transactor(FDBdb)([&](auto& tr) {

        for (size_t i = start; i < end; ++i) {
          std::string key = build_index(&blocks[i]);

          ldpp_dout(dpp, 10)
              << "FDBBlockDirectory::" << __func__
              << "(): index is: " << key
              << dendl;

          if (!lfdb::get(tr, key, out_kvs[i])) {
            ldpp_dout(dpp, 0)
                << "FDBBlockDirectory::" << __func__
                << "() ERROR: get function returned false!"
                << dendl;
            return -ENOENT;
          }
        }

        return 0;
      });

      if (ret < 0) {
        return ret;
      }
    }

    // ---------- POPULATE PHASE (outside transactions) ----------
    for (size_t i = 0; i < blocks.size(); ++i) {
      auto& block = blocks[i];
      auto& kvs = out_kvs[i];

      block.blockID       = std::stoull(kvs.at("blockID"));
      block.version       = kvs.at("version");
      block.deleteMarker  = (std::stoi(kvs.at("deleteMarker")) != 0);
      block.size          = std::stoull(kvs.at("size"));
      block.globalWeight  = std::stoull(kvs.at("globalWeight"));

      block.cacheObj.objName      = kvs.at("objName");
      block.cacheObj.bucketName   = kvs.at("bucketName");
      block.cacheObj.creationTime = kvs.at("creationTime");
      block.cacheObj.dirty        = (std::stoi(kvs.at("dirty")) != 0);

      block.cacheObj.hostsList.clear();
      boost::split(block.cacheObj.hostsList,
                   kvs.at("hosts"),
                   boost::is_any_of("_"));

      block.cacheObj.etag         = kvs.at("etag");
      block.cacheObj.size         = std::stoull(kvs.at("objSize"));
      block.cacheObj.user_id      = kvs.at("userId");
      block.cacheObj.display_name = kvs.at("displayName");
      block.cacheObj.acl          = kvs.at("acl");

      // Match Redis implementation.
      if (auto it = kvs.find("attrsCount"); it != kvs.end()) {
        [[maybe_unused]] size_t attrsCount = std::stoul(it->second);
      }

      block.cacheObj.attrs.clear();

      for (const auto& [field, value] : kvs) {
        if (field.rfind("attr_", 0) == 0) {
          ceph::buffer::list bl;
          bl.append(value);
          block.cacheObj.attrs[field.substr(5)] = std::move(bl);
        }
      }
    }

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBlockDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBlockDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp,
                            CacheBlock* block,
                            const std::string& copyName,
                            const std::string& copyBucketName,
                            optional_yield y)
{
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  CacheBlock source = *block;

  if (int ret = get(dpp, &source, y); ret < 0) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__
                       << "(): get failed, ret=" << ret
                       << dendl;
    return ret;
  }

  CacheBlock copy = source;
  copy.blockID = 0;
  copy.cacheObj.objName = copyName;
  copy.cacheObj.bucketName = copyBucketName;

  return set(dpp, &copy, y);
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp,
                           CacheBlock* block,
                           optional_yield y)
{
  if (block == nullptr) {
    return -EINVAL;
  }

  try {
    lfdb::erase(FDBdb, build_index(block));
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBBlockDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  int ret = -1;
  if (block == nullptr) {
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__
                      << "(): null block pointer" << dendl;
    return -EINVAL;
  }

  if (!(ret = exist_key(dpp, block, y))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -ENOENT;
  }

  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
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

  return this->set(dpp, block, y);

}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp,
                                   CacheBlock* block,
                                   std::string& value,
                                   optional_yield y)
{
  if (block == nullptr) {
    ldpp_dout(dpp, 0)
      << "FDBBlockDirectory::" << __func__
      << "(): null block pointer"
      << dendl;
    return -EINVAL;
  }

  try {
    return lfdb::make_transactor(FDBdb)([&](auto& tr) {
      std::string key = build_index(block);

      std::map<std::string, std::string> fdbValues;
      if (!lfdb::get(tr, key, fdbValues)) {
        ldpp_dout(dpp, 10)
          << "FDBBlockDirectory::" << __func__
          << "(): Block does not exist."
          << dendl;
        return -ENOENT;
      }

      auto hostsIt = fdbValues.find("hosts");
      if (hostsIt == fdbValues.end()) {
        ldpp_dout(dpp, 10)
          << "FDBBlockDirectory::" << __func__
          << "(): hosts field missing."
          << dendl;
        return -EINVAL;
      }

      std::vector<std::string> hosts;
      boost::split(hosts, hostsIt->second, boost::is_any_of("_"));

      hosts.erase(
        std::remove(hosts.begin(), hosts.end(), value),
        hosts.end());

      std::string encodedHosts;
      for (size_t i = 0; i < hosts.size(); ++i) {
        if (i != 0) {
          encodedHosts += "_";
        }
        encodedHosts += hosts[i];
      }

      fdbValues["hosts"] = encodedHosts;

      lfdb::set(tr, key, fdbValues);
      return 0;
    });
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBBlockDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }
}

} // namespace rgw::d4n
