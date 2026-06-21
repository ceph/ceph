#include <algorithm>
#include <type_traits>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory_fdb.h"
#include <iomanip>
#include <sstream>

namespace rgw::d4n {

using std::map;
using std::string;

static std::string encode_score(double score)
{
  std::ostringstream ss;
  ss << std::setw(20) << std::setfill('0') << std::fixed << std::setprecision(6) << score;
  return ss.str();
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
  return fdb_add(dpp, bucket_id, 0, object_name, y);
}

int FDBBucketDirectory::remove_object(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& object_name, optional_yield y)
{
  return fdb_rem(dpp, bucket_id, object_name, y);
}

int FDBBucketDirectory::scan_objects(const DoutPrefixProvider* dpp, const std::string& bucket_id, uint64_t start_pos, const std::string& pattern, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, uint64_t& next_pos, optional_yield y)
{
  return fdb_scan(dpp, bucket_id, start_pos, pattern, count, objects, next_pos, y);
}

int FDBBucketDirectory::get_range(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& start, const std::string& stop, uint64_t offset, uint64_t count, std::vector<std::string>& objects, std::optional<CacheObject>& params, optional_yield y)
{
  return fdb_range(dpp, bucket_id, start, stop, offset, count, objects, y);
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
                                double /* score */,
                                const std::string& member,
                                optional_yield y)
{
  try {
    lfdb::set(FDBdb, bucket_id + "/" + member, "");

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
    lfdb::erase(FDBdb, bucket_id + "/" + member);

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
                              const std::string& stop,
                              uint64_t offset,
                              uint64_t count,
                              std::vector<std::string>& members,
                              optional_yield y)
{
  try {
    std::string begin_key = bucket_id + "/";
    if (!start.empty()) {
      begin_key += start;
    }

    std::string end_key = bucket_id + "/";
    if (!stop.empty()) {
      end_key += stop + "\xff";
    } else {
      end_key += "\xff";
    }

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBdb,
        lfdb::select{begin_key, end_key},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      ldpp_dout(dpp, 10)
          << "FDBBucketDirectory::" << __func__
          << "() Empty response"
          << dendl;
      return -ENOENT;
    }

    uint64_t begin = std::min(offset, (uint64_t)kvs.size());
    uint64_t end = count
                       ? std::min(begin + count, (uint64_t)kvs.size())
                       : kvs.size();

    const size_t prefix_len = bucket_id.size() + 1;

    for (uint64_t i = begin; i < end; ++i) {
      members.emplace_back(kvs[i].first.substr(prefix_len));
    }

  } catch (const lfdb::libfdb_exception& e) {

    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::"
        << __func__
        << "() ERROR: "
        << e.what()
        << dendl;

    return -EINVAL;
  }

  return 0;
}

int FDBBucketDirectory::fdb_scan(const DoutPrefixProvider* dpp,
                             const std::string& bucket_id,
                             uint64_t cursor,
                             const std::string& pattern,
                             uint64_t count,
                             std::vector<std::string>& members,
                             uint64_t next_cursor,
                             optional_yield y)
{
  try {
    std::string begin_key = bucket_id + "/";
    std::string end_key = bucket_id + "/\xff";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBdb,
        lfdb::select{begin_key, end_key},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      next_cursor = 0;
      return -ENOENT;
    }

    uint64_t start = std::min(cursor, (uint64_t)kvs.size());
    uint64_t end = count
                       ? std::min(start + count, (uint64_t)kvs.size())
                       : kvs.size();

    next_cursor = (end >= kvs.size()) ? 0 : end;

    const size_t prefix_len = bucket_id.size() + 1;

    for (uint64_t i = start; i < end; ++i) {
      std::string member = kvs[i].first.substr(prefix_len);

      if (!pattern.empty() &&
          member.find(pattern) == std::string::npos) {
        continue;
      }

      members.emplace_back(std::move(member));
    }

  } catch (const lfdb::libfdb_exception& e) {

    ldpp_dout(dpp, 0)
        << "FDBBucketDirectory::"
        << __func__
        << "() ERROR: "
        << e.what()
        << dendl;

    return -EINVAL;
  }

  return 0;
}


int FDBObjectDirectory::exist_key(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, optional_yield y) 
{
  std::string key = build_index(bucket_id, obj_name);
  return lfdb::key_exists(FDBdb, key);
}

int FDBObjectDirectory::del(const DoutPrefixProvider* dpp, CacheObj* object, optional_yield y)
{
  std::string key = build_index(object->bucketName, object->objName);
  lfdb::erase(FDBdb, key);
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
                                double score,
                                const std::string& version,
                                optional_yield y)
{
  try {
    std::string index = build_index(bucket_id, obj_name);

    lfdb::set(FDBdb,
              index + "/" + std::to_string(score) + "/" + version,
              "");

  } catch (const lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;
    return -EINVAL;
  }

  return 0;
}

int FDBObjectDirectory::fdb_range(
    const DoutPrefixProvider* dpp,
    const std::string& bucket_id,
    const std::string& obj_name,
    int start,
    int stop,
    std::vector<std::string>& members,
    optional_yield y)
{
  try {
    std::string prefix = build_index(bucket_id, obj_name) + "/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBdb,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }
    int end = std::min(stop + 1, static_cast<int>(kvs.size()));

    for (int i = start; i < end; ++i) {
      const std::string& key = kvs[i].first;

      auto pos = key.find('/', prefix.size());

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
                                     const std::string& start,
                                     const std::string& stop,
                                     std::vector<std::string>& members,
                                     optional_yield y)
{
  try {
    std::string prefix = build_index(bucket_id, obj_name) + "/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBdb,
        lfdb::select{prefix + start, prefix + stop + "\xff"},
        std::back_inserter(kvs));

    if (!ok || kvs.empty()) {
      return -ENOENT;
    }

    std::reverse(kvs.begin(), kvs.end());

    for (const auto& kv : kvs) {
      auto pos = kv.first.find('/', prefix.size());

      if (pos != std::string::npos) {
        members.push_back(kv.first.substr(pos + 1));
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

int FDBObjectDirectory::fdb_rem(const DoutPrefixProvider* dpp,
                                const std::string& bucket_id,
                                const std::string& obj_name,
                                const std::string& version,
                                optional_yield y)
try
{
  return lfdb::make_transactor(FDBdb)([&](auto& tr) {

    const auto prefix = build_index(bucket_id, obj_name) + "/";

    for (const auto& kv : lfdb::pair_generator(tr, lfdb::select { prefix })) {
      const auto& key = kv.first;
      const auto pos = key.find('/', prefix.size());

      if (pos == key.npos || key.substr(pos + 1) != version)
        continue;

      lfdb::erase(tr, key);

      return 0;
    }

    return -ENOENT;
  });

} catch (const  lfdb::libfdb_exception& e) {
    ldpp_dout(dpp, 0)
      << "FDBObjectDirectory::" << __func__
      << "() ERROR: " << e.what()
      << dendl;

  return -EINVAL;
}


int FDBObjectDirectory::fdb_remrangebyscore(const DoutPrefixProvider* dpp,
                                            const std::string& bucket_id,
                                            const std::string& obj_name,
                                            double min,
                                            double max,
                                            optional_yield y)
try
{
  return lfdb::make_transactor(FDBdb)([&](auto& tr) {

    const auto prefix = build_index(bucket_id, obj_name) + "/";

    const std::string min_s = encode_score(min);
    const std::string max_s = encode_score(max);

    bool removed = false;

    for (const auto& kv : lfdb::pair_generator(tr, lfdb::select { prefix })) {

      const auto& key = kv.first;

      const auto pos = key.find('/', prefix.size());

      if (pos == std::string::npos)
        continue;

      const std::string score =
          key.substr(prefix.size(), pos - prefix.size());

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


int FDBObjectDirectory::fdb_rank(
    const DoutPrefixProvider* dpp,
    const std::string& bucket_id,
    const std::string& obj_name,
    const std::string& member,
    std::string& index,
    optional_yield y)
{
  try {
    std::string prefix = build_index(bucket_id, obj_name) + "/";

    std::vector<std::pair<std::string, std::string>> kvs;

    bool ok = lfdb::get(
        FDBdb,
        lfdb::select{prefix, prefix + "\xff"},
        std::back_inserter(kvs));

    if (!ok)
      return -ENOENT;

    for (size_t i = 0; i < kvs.size(); ++i) {
      auto pos = kvs[i].first.find('/', prefix.size());

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
}

int FDBObjectDirectory::add_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, ceph::real_time& creation_time, std::optional<CacheObjectVersion> params, optional_yield y, Pipeline* pipeline)
{
  auto score = ceph::real_clock::to_double(creation_time);
  ldpp_dout(dpp, 10) << "FDBObjectDirectory::" << __func__ << "(): Score of object name: "<< obj_name << " version: " << version << " is: "  << score << dendl;
  return fdb_add(dpp, bucket_id, obj_name, score, version, y);
}

int FDBObjectDirectory::remove_version(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, optional_yield y)
{
  return fdb_rem(dpp, bucket_id, obj_name, version, y);
}

int FDBObjectDirectory::remove_version_by_creation_time(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const double& creation_time,optional_yield y)
{
  return fdb_remrangebyscore(dpp, bucket_id, obj_name, creation_time, creation_time, y);;
}

int FDBObjectDirectory::list_versions(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& start, const std::string& stop, std::vector<CacheObjectVersion>& obj_versions, optional_yield y)
{
  std::vector<std::string> members;
  auto ret = fdb_revrange(dpp, bucket_id, obj_name, start, stop, members, y);
  obj_versions.reserve(members.size());
  for (const auto& version : members) {
    auto& obj_version = obj_versions.emplace_back();
    obj_version.bucketId = bucket_id;
    obj_version.objName = obj_name;
    obj_version.version = version;
  }

  return ret;
}

int FDBObjectDirectory::get_version_index(const DoutPrefixProvider* dpp, const std::string& bucket_id, const std::string& obj_name, const std::string& version, std::string& index, optional_yield y)
{
  return fdb_rank(dpp, bucket_id, obj_name, version, index, y);
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
