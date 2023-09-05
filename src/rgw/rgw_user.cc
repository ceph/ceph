// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sal_rados.h"

#include "include/types.h"
#include "rgw_user.h"

// until everything is moved from rgw_common
#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int rgw_user_sync_all_stats(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
			    rgw::sal::User* user, optional_yield y)
{
  rgw::sal::BucketList user_buckets;

  CephContext *cct = driver->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  string marker;
  int ret;

  do {
    ret = user->list_buckets(dpp, marker, string(), max_entries, false, user_buckets, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to read user buckets: ret=" << ret << dendl;
      return ret;
    }
    auto& buckets = user_buckets.get_buckets();
    for (auto i = buckets.begin(); i != buckets.end(); ++i) {
      marker = i->first;

      auto& bucket = i->second;

      ret = bucket->load_bucket(dpp, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not read bucket info: bucket=" << bucket << " ret=" << ret << dendl;
        continue;
      }
      ret = bucket->sync_user_stats(dpp, y);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: could not sync bucket stats: ret=" << ret << dendl;
        return ret;
      }
      ret = bucket->check_bucket_shards(dpp, y);
      if (ret < 0) {
	ldpp_dout(dpp, 0) << "ERROR in check_bucket_shards: " << cpp_strerror(-ret)<< dendl;
      }
    }
  } while (user_buckets.is_truncated());

  ret = user->complete_flush_stats(dpp, y);
  if (ret < 0) {
    cerr << "ERROR: failed to complete syncing user stats: ret=" << ret << std::endl;
    return ret;
  }

  return 0;
}

int rgw_user_get_all_buckets_stats(const DoutPrefixProvider *dpp,
				   rgw::sal::Driver* driver,
				   rgw::sal::User* user,
				   map<string, bucket_meta_entry>& buckets_usage_map,
				   optional_yield y)
{
  CephContext *cct = driver->ctx();
  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;
  bool done;
  string marker;
  int ret;

  do {
    rgw::sal::BucketList buckets;
    ret = user->list_buckets(dpp, marker, string(), max_entries, false, buckets, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to read user buckets: ret=" << ret << dendl;
      return ret;
    }
    auto& m = buckets.get_buckets();
    for (const auto& i :  m) {
      marker = i.first;

      auto& bucket_ent = i.second;
      ret = bucket_ent->load_bucket(dpp, y, true /* load user stats */);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: could not get bucket stats: ret=" << ret << dendl;
        return ret;
      }
      bucket_meta_entry entry;
      entry.size = bucket_ent->get_size();
      entry.size_rounded = bucket_ent->get_size_rounded();
      entry.creation_time = bucket_ent->get_creation_time();
      entry.count = bucket_ent->get_count();
      buckets_usage_map.emplace(bucket_ent->get_name(), entry);
    }
    done = (buckets.count() < max_entries);
  } while (!done);

  return 0;
}

int rgw_validate_tenant_name(const string& t)
{
  struct tench {
    static bool is_good(char ch) {
      return isalnum(ch) || ch == '_';
    }
  };
  std::string::const_iterator it =
    std::find_if_not(t.begin(), t.end(), tench::is_good);
  return (it == t.end())? 0: -ERR_INVALID_TENANT_NAME;
}

/**
 * Get the anonymous (ie, unauthenticated) user info.
 */
void rgw_get_anon_user(RGWUserInfo& info)
{
  info.user_id = RGW_USER_ANON_ID;
  info.display_name.clear();
  info.access_keys.clear();
}

