#include <algorithm>
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

static std::string encode_score(int64_t score)
{
  return fmt::format("{:019d}", score);
}

int FDBDirectory::get_kv(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& key,
                       const std::string& field,
                       std::string& out_val)
{
  std::map<std::string, std::string> kvs;
  if (!lfdb::get(FDBconn, key, kvs)) {
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
    return lfdb::make_transactor(FDBconn)([&](auto& tr) {
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
  if (!lfdb::get(FDBconn, key, kvs)) {
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
    return lfdb::make_transactor(FDBconn)([&](auto& tr) {
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
    return lfdb::make_transactor(FDBconn)([&](auto& tr) {
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
  return lfdb::key_exists(FDBconn, bucket_id);
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
  //TODO - check if the two paths can be combined into one
  if (!prefix.empty()) {
    // SCAN_OBJECTS path (with prefix)
    auto ret = fdb_scan(dpp, bucket_id, marker, prefix, count, marker_inclusive, objs_info, continuation_token, y);
    if (ret < 0 ) {
      ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << " fdb_scan: " << ret << dendl;
      return ret;
    }
  } else {
    // GET_RANGE path (no prefix)
    auto ret = fdb_range(dpp, bucket_id, marker, count, objs_info, continuation_token, marker_inclusive, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "FDBBucketDirectory::" << __func__ << " fdb_range failed: " << ret << dendl;
      return ret;
    }
  }
  return 0;
}

//Key form is <bucket-id>/objects/<object-name>
std::string FDBBucketDirectory::get_object_subspace(const std::string& bucket_id)
{
  return url_encode(bucket_id, true) + "/objects/";
}
std::string FDBBucketDirectory::build_object_index(const std::string& bucket_id, const std::string& obj_name)
{
  return get_object_subspace(bucket_id) + obj_name;
}

int FDBBucketDirectory::fdb_add(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                double score,
                                const std::string& member,
                                std::optional<CacheObject> params,
                                optional_yield y)
{
  try {
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " :member " << member << dendl;
    std::string member_key = build_object_index(bucket_id, member);

    if (params) {
      lfdb::set(FDBconn, member_key, *params);
    } else {
      lfdb::set(FDBconn, member_key, "1");
    }

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
    std::string member_key = build_object_index(bucket_id, member);
    lfdb::erase(FDBconn, member_key);

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBBucketDirectory::fdb_range(const DoutPrefixProvider* dpp,
                              const std::string& bucket_id,
                              const std::string& start,
                              uint64_t count,
                              std::vector<CacheObject>& objs_info,
                              std::string& continuation_token,
                              bool start_inclusive,
                              optional_yield y)
{
  continuation_token.clear();
  try {
    std::string base = get_object_subspace(bucket_id);

    std::string begin_key;
    if (start.empty()) {
      begin_key = base;
    } else if (start_inclusive) {
      begin_key = base + start; // include this key
    } else {
      begin_key = base + start + std::string(1, '\x00'); // exclusive resume
    }
    std::string end_key = base + "\xff";

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " begin_key: " << begin_key << dendl;
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " end_key: " << end_key << dendl;

    std::vector<std::pair<std::string, CacheObject>> kvs;
    for (auto&& block : lfdb::block_generator<CacheObject>(FDBconn, lfdb::select { begin_key, end_key })) {
      kvs.insert(kvs.end(), std::begin(block), std::end(block));
    }

    if (kvs.empty()) {
      ldpp_dout(dpp, 10)
          << "FDBBucketDirectory::" << __func__
          << "() Empty response"
          << dendl;
      return -ENOENT;
    }

    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " kvs.size(): " << kvs.size() << dendl;
    uint64_t i = 0;
    for (const auto& [key, value] : kvs) {
      // key layout: "<bucket_id>/objects/<member>"
      // member itself may contain '/', so we can't use rfind("/") here.
      std::string member = key.substr(base.size());
      objs_info.push_back(value);
      objs_info.back().objName = member;

      if (count && objs_info.size() == count) {
        // More results exist only if there's at least one more
        // key left in what we fetched.
        if (i + 1 < kvs.size()) {
          continuation_token = member;
        }
        break;
      }
      ++i;
    }
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << " continuation_token: " << continuation_token << dendl;

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
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
    std::string base = get_object_subspace(bucket_id);

    std::string prefix_lo = base + prefix;
    std::string prefix_hi = base + prefix + "\xff";

    // start_token may come from an S3-level marker the caller chose
    // freely -- it is NOT guaranteed to fall within [prefix_lo, prefix_hi).
    // Clamp it into that range rather than trusting it blindly.
    std::string candidate_begin;
    if (start_token.empty()) {
      candidate_begin = prefix_lo;
    } else {
      if (marker_inclusive) {
        candidate_begin = base + start_token;
      } else {
        candidate_begin = base + start_token + std::string(1, '\x00');
      }
    }

    std::string range_begin = std::max(candidate_begin, prefix_lo);
    std::string range_end   = prefix_hi;
    if (range_begin >= range_end) {
      // The marker is already past the end of this prefix's range --
      // nothing to return.
      return -ENOENT;
    }
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << "() range_begin: " << range_begin << dendl;
    ldpp_dout(dpp, 20) << "FDBBucketDirectory::" << __func__ << "() range_end: " << range_end << dendl;

    std::vector<std::pair<std::string, CacheObject>> kvs;
    for (auto&& block : lfdb::block_generator<CacheObject>(FDBconn, lfdb::select { range_begin, range_end })) {
      kvs.insert(kvs.end(), std::begin(block), std::end(block));
    }
    if (kvs.empty()) {
      return -ENOENT;
    }

    uint64_t i = 0;
    for (const auto& [key, value] : kvs) {
      // key layout: "<bucket_id>/objects/<member>"
      // member itself may contain '/', so we can't use rfind("/") here.
      std::string member = key.substr(base.size());
      objs_info.push_back(value);
      objs_info.back().objName = member;

      if (count && objs_info.size() == count) {
        // More results exist only if there's at least one more
        // key left in what we fetched.
        if (i + 1 < kvs.size()) {
          continuation_token = member;
        }
        break;
      }
      ++i;
    }
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::" << __func__
        << "() ERROR: " << e.what()
        << dendl;
    return -EINVAL;
  }

  return 0;
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
  std::string index = build_index(bucket_id, obj_name);
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
  return index + "/versions/";
}

std::string FDBObjectDirectory::get_score_subspace(const DoutPrefixProvider* dpp,
                                                    const std::string& bucket_id,
                                                    const std::string& obj_name)
{
  std::string index = build_index(bucket_id, obj_name);
  ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << " :index " << index << dendl;
  return index + "/score/";
}

std::string FDBObjectDirectory::build_versions_index(const DoutPrefixProvider* dpp,
                                                    const std::string& bucket_id,
                                                    const std::string& obj_name,
                                                    const std::string& score,
                                                    const std::string& version)
{
  return get_versions_subspace(dpp, bucket_id, obj_name) + score + "/" + version;
}

std::string FDBObjectDirectory::build_version_score_index(const DoutPrefixProvider* dpp,
                                                          const std::string& bucket_id,
                                                          const std::string& obj_name,
                                                          const std::string& version)
{
  return get_score_subspace(dpp, bucket_id, obj_name) + version;
}

int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y) 
{
  std::string key = build_index(bucket_id, obj_name);
  return lfdb::key_exists(FDBconn, key);
}

int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  std::string key = build_index(object->bucketName, object->objName);
  lfdb::erase(FDBconn, key);
  return 0; 
}

int FDBObjectDirectory::fdb_add(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                int64_t score,
                                const std::string& version,
                                std::optional<CacheObjectVersion> params,
                                optional_yield y)
{
  try {
    return lfdb::make_transactor(FDBconn)([&](auto& tr) {
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
      if (params) {
        lfdb::set(tr, versions_key, *params);
      } else {
        lfdb::set(tr, versions_key, "1");
      }
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

int FDBObjectDirectory::fdb_range(const DoutPrefixProvider* dpp,
                                  const std::string& bucket_id,
                                  const std::string& obj_name,
                                  int start,
                                  int stop,
                                  std::vector<std::string>& members,
                                  optional_yield y)
{
  try {
    std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);
    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBconn,
        lfdb::select{versions_subspace, versions_subspace + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    int end = std::min(stop + 1, (int)kvs.size());

    for (int i = start; i < end; ++i) {
      const std::string& key = kvs[i].first;

      auto pos = key.find('/', versions_subspace.size());

      if (pos != std::string::npos) {
        members.push_back(key.substr(pos + 1));
      }
    }

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
    std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);
    std::string range_begin = versions_subspace;
    std::string range_end = versions_subspace + "\xff";  // default: everything, if no marker

    if (!marker_version.empty()) {
      // Point lookup: resolve the marker's own encoded_score via the
      // reverse index (index + "/score/" + marker -> encoded_score),
      // rather than scanning to find it.
      std::string marker_score;
      std::string member_key = build_version_score_index(dpp, bucket_id, obj_name, marker_version);
      bool found = lfdb::get(FDBconn, member_key, marker_score);
      if (!found) {
          ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__
                              << "() marker version not found: " << marker_version << dendl;
          return -ENOENT;
      }

      // Bound strictly before the marker's own key (score + "/" + version),
      // so the marker itself is excluded and only genuinely older
      // versions (lower scores) are returned.
      range_end = build_versions_index(dpp, bucket_id, obj_name, marker_score, marker_version);
    }
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() range_begin: " << range_begin << dendl;
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() range_end: " << range_end << dendl;
    auto reverse_range = lfdb::select { range_begin, range_end };
    reverse_range.options.reverse_order = true;
    std::vector<std::pair<std::string, CacheObjectVersion>> kvs;
    for (auto&& block : lfdb::block_generator<CacheObjectVersion>(FDBconn, reverse_range)) {
      kvs.insert(kvs.end(), std::begin(block), std::end(block));
    }
    if (kvs.empty()) {
      return -ENOENT;
    }
    ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "count: " << count << dendl;

    uint64_t i = 0;
    for (const auto& [key, value] : kvs) {
      //const std::string& key = kvs[i].first;
      // key layout: versions_subspace + encoded_score + "/" + member.
      // member may itself contain '/', so don't use rfind("/") -- instead
      // skip forward past the fixed versions_subspace prefix, then past the score segment
      // (delimited by the first '/' after the prefix), and take everything
      // that remains as the member.
      size_t score_start = versions_subspace.size();
      size_t score_end = key.find('/', score_start);
      if (score_end == std::string::npos) {
        ldpp_dout(dpp, 0) << "FDBObjectDirectory::" << __func__
                          << "() malformed key (no member segment): " << key << dendl;
        continue;
      }
      obj_versions.push_back(value);
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() obj_versions: " << obj_versions[i].version << dendl;
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() obj_versions: " << obj_versions[i].user_id << dendl;
      ldpp_dout(dpp, 20) << "FDBObjectDirectory::" << __func__ << "() obj_versions: " << obj_versions[i].display_name << dendl;
      if (count && obj_versions.size() == count) {
        // More results exist only if there's at least one more
        // key left in what we fetched.
        if (i + 1 < kvs.size()) {
          continuation_token = key.substr(score_end + 1);
        }
        break;
      }
      ++i;
    }
  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBObjectDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                const std::string& version,
                                optional_yield y)
{
  try
  {
    return lfdb::make_transactor(FDBconn)([&](auto& tr) {

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
  return lfdb::make_transactor(FDBconn)([&](auto& tr) {

    std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

    const std::string min_s = encode_score(min);
    const std::string max_s = encode_score(max);

    bool removed = false;

    for (const auto& kv : lfdb::pair_generator(tr, lfdb::select { versions_subspace })) {

      const auto& key = kv.first;

      const auto pos = key.find('/', versions_subspace.size());

      if (pos == std::string::npos)
        continue;

      const std::string score =
          key.substr(versions_subspace.size(), pos - versions_subspace.size());

      if (score >= min_s && score <= max_s) {
        lfdb::erase(tr, key);
        removed = true;
      }
    }

    return removed ? 0 : -ENOENT;
  });

} catch (const lfdb::libfdb_exception& e) {
  ldpp_dout(dpp, 0)
    << "FDBObjectDirectory::" << __func__
    << "() ERROR: " << e.what()
    << dendl;

  return -EINVAL;
}


int FDBObjectDirectory::fdb_rank(const DoutPrefixProvider* dpp,
                                 const std::string& bucket_id,
                                 const std::string& obj_name,
                                 const std::string& member,
                                 std::string& index,
                                 optional_yield y)
{
  try {

    std::string versions_subspace = get_versions_subspace(dpp, bucket_id, obj_name);

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBconn,
        lfdb::select{versions_subspace, versions_subspace + "\xff"},
        std::back_inserter(kvs));

    if (!ok)
      return -ENOENT;

    for (size_t i = 0; i < kvs.size(); ++i) {
      auto pos = kvs[i].first.find('/', versions_subspace.size());

      if (pos != std::string::npos &&
          kvs[i].first.substr(pos + 1) == member) {
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
  return lfdb::key_exists(FDBconn, key);
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
  std::string key = build_index(block);
  ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

  map<string, string> fdbValues;

  auto ret = set_values(dpp, *block, fdbValues, y);
  if (ret < 0) {
    return ret;
  }

  lfdb::set(FDBconn, key, fdbValues);
  return 0;
}


int FDBBlockDirectory::set(const DoutPrefixProvider* dpp, std::vector<CacheBlock>& blocks, optional_yield y)
{
  for (auto block : blocks) {
    std::string key = build_index(&block);
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): index is: " << key << dendl;

    map<string, string> fdbValues;

    auto ret = set_values(dpp, block, fdbValues, y);
    if (ret < 0) {
      return ret;
    }

    lfdb::set(FDBconn, key, fdbValues);
  }

  return 0;
}

int FDBBlockDirectory::get(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y) 
{
  std::string key = build_index(block);
  std::map<std::string, std::string> out_kvs;

  if (lfdb::get(FDBconn, key, out_kvs) != true){
    ldpp_dout(dpp, 0) << "FDBBlockDirectory::" << __func__ << "() ERROR: " << "get function returned false! " << dendl;
	return -ENOENT;
  }

  block->blockID = std::stoull(out_kvs.at("blockID"));
  block->version = out_kvs.at("version");
  block->deleteMarker = (out_kvs.at("deleteMarker") == "1");
  block->size = std::stoull(out_kvs.at("size"));
  block->globalWeight = std::stoull(out_kvs.at("globalWeight"));
  block->cacheObj.objName      = out_kvs.at("objName");
  block->cacheObj.bucketName   = out_kvs.at("bucketName");
  block->cacheObj.creationTime = out_kvs.at("creationTime");
  block->cacheObj.dirty        = (out_kvs.at("dirty") == "1");
  boost::split(
    block->cacheObj.hostsList,
    out_kvs.at("hosts"),
    boost::is_any_of("_")
  );
  block->cacheObj.etag         = out_kvs.at("etag");
  block->cacheObj.size         = std::stoull(out_kvs.at("objSize"));
  block->cacheObj.user_id      = out_kvs.at("userId");
  block->cacheObj.display_name = out_kvs.at("displayName");

  block->cacheObj.acl = out_kvs.at("acl");

  size_t attrsCount = std::stoull(out_kvs.at("attrsCount"));
  size_t found = 0;
  for (auto const& [key, value] : out_kvs) {
    if (key.starts_with("attr_")) {
      std::string attrKey = key.substr(5);
      ceph::buffer::list bl;
      bl.append(value);
      block->cacheObj.attrs[attrKey] = std::move(bl);
      if (++found == attrsCount) break;
    }
  }

  if (found != attrsCount) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "() ERROR: expected " << attrsCount << " attrs but found " << found << dendl;
    return -EINVAL;
  }

  return 0;
}


int FDBBlockDirectory::get(const DoutPrefixProvider* dpp,
                           std::vector<CacheBlock>& blocks,
                           optional_yield y)
{
  try {
    std::vector<std::map<std::string, std::string>> out_kvs(blocks.size());

    // -------- FETCH PHASE --------
    for (size_t i = 0; i < blocks.size(); ++i) {
      auto& block = blocks[i];

      std::string key = build_index(&block);

      ldpp_dout(dpp, 10)
        << "FDBBlockDirectory::" << __func__
        << "(): index is: " << key
        << dendl;

      if (!lfdb::get(FDBconn, key, out_kvs[i])) {
        ldpp_dout(dpp, 0)
          << "FDBBlockDirectory::" << __func__
          << "() ERROR: get function returned false!"
          << dendl;
        return -ENOENT;
      }
    }

    // -------- POPULATE PHASE --------
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

      // Match Redis implementation: read attrsCount even though it is unused.
      if (auto it = kvs.find("attrsCount"); it != kvs.end()) {
        [[maybe_unused]] size_t attrsCount = std::stoul(it->second);
      }

      block.cacheObj.attrs.clear();

      // Restore attr_* entries.
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



//FIXME: shouldn't copyName reflect block's name instead of object name?
//the same for redis class.
int FDBBlockDirectory::copy(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& copyName, const std::string& copyBucketName, optional_yield y)
{
  // Retrieve the block from the directory in case it has been updated by a remote cache.
  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  auto copyBlock = CacheBlock{ .cacheObj = { .objName = copyName, .bucketName = copyBucketName }, .blockID = 0 };
  std::string copyKey = build_index(&copyBlock);

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

  this->set(dpp, &copyBlock, y);

  return 0;
}

int FDBBlockDirectory::del(const DoutPrefixProvider* dpp, CacheBlock* block, optional_yield y)
{
  std::string key = build_index(block);
  lfdb::erase(FDBconn, key);
  return 0; 
}

int FDBBlockDirectory::update_field(const DoutPrefixProvider* dpp, CacheBlock* block, const std::string& field, std::string& value, optional_yield y)
{
  int ret = -1;

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

  this->set(dpp, block, y);

  return 0;
}

int FDBBlockDirectory::remove_host(const DoutPrefixProvider* dpp, CacheBlock* block, std::string& value, optional_yield y)
{
  int ret = -1;

  if (!(ret = exist_key(dpp, block, y))) {
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Block does not exist." << dendl;
	return -ENOENT;
  }

  if (this->get(dpp, block, y) < 0){
    ldpp_dout(dpp, 10) << "FDBBlockDirectory::" << __func__ << "(): Could not retrive the object." << dendl;
	return -ENOENT;
  }

  block->cacheObj.hostsList.erase(value);

  this->set(dpp, block, y);

  return 0;
}

} // namespace rgw::d4n
