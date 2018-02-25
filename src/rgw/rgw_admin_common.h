#ifndef CEPH_RGW_ADMIN_COMMON_H
#define CEPH_RGW_ADMIN_COMMON_H

#include "cls/rgw/cls_rgw_types.h"

#include "common/ceph_json.h"
#include <common/errno.h>
#include <common/safe_io.h>

#include "rgw_common.h"
#include "rgw_rados.h"

enum RgwAdminCommand {
  OPT_NO_CMD = 0,
  OPT_USER_CREATE,
  OPT_USER_INFO,
  OPT_USER_MODIFY,
  OPT_USER_RM,
  OPT_USER_SUSPEND,
  OPT_USER_ENABLE,
  OPT_USER_CHECK,
  OPT_USER_STATS,
  OPT_USER_LIST,
  OPT_SUBUSER_CREATE,
  OPT_SUBUSER_MODIFY,
  OPT_SUBUSER_RM,
  OPT_KEY_CREATE,
  OPT_KEY_RM,
  OPT_BUCKETS_LIST,
  OPT_BUCKET_LIMIT_CHECK,
  OPT_BUCKET_LINK,
  OPT_BUCKET_UNLINK,
  OPT_BUCKET_STATS,
  OPT_BUCKET_CHECK,
  OPT_BUCKET_SYNC_STATUS,
  OPT_BUCKET_SYNC_INIT,
  OPT_BUCKET_SYNC_RUN,
  OPT_BUCKET_SYNC_DISABLE,
  OPT_BUCKET_SYNC_ENABLE,
  OPT_BUCKET_RM,
  OPT_BUCKET_REWRITE,
  OPT_BUCKET_RESHARD,
  OPT_POLICY,
  OPT_POOL_ADD,
  OPT_POOL_RM,
  OPT_POOLS_LIST,
  OPT_LOG_LIST,
  OPT_LOG_SHOW,
  OPT_LOG_RM,
  OPT_USAGE_SHOW,
  OPT_USAGE_TRIM,
  OPT_USAGE_CLEAR,
  OPT_OBJECT_RM,
  OPT_OBJECT_UNLINK,
  OPT_OBJECT_STAT,
  OPT_OBJECT_REWRITE,
  OPT_OBJECTS_EXPIRE,
  OPT_BI_GET,
  OPT_BI_PUT,
  OPT_BI_LIST,
  OPT_BI_PURGE,
  OPT_OLH_GET,
  OPT_OLH_READLOG,
  OPT_QUOTA_SET,
  OPT_QUOTA_ENABLE,
  OPT_QUOTA_DISABLE,
  OPT_GC_LIST,
  OPT_GC_PROCESS,
  OPT_LC_LIST,
  OPT_LC_PROCESS,
  OPT_ORPHANS_FIND,
  OPT_ORPHANS_FINISH,
  OPT_ORPHANS_LIST_JOBS,
  OPT_ZONEGROUP_ADD,
  OPT_ZONEGROUP_CREATE,
  OPT_ZONEGROUP_DEFAULT,
  OPT_ZONEGROUP_DELETE,
  OPT_ZONEGROUP_GET,
  OPT_ZONEGROUP_MODIFY,
  OPT_ZONEGROUP_SET,
  OPT_ZONEGROUP_LIST,
  OPT_ZONEGROUP_REMOVE,
  OPT_ZONEGROUP_RENAME,
  OPT_ZONEGROUP_PLACEMENT_ADD,
  OPT_ZONEGROUP_PLACEMENT_MODIFY,
  OPT_ZONEGROUP_PLACEMENT_RM,
  OPT_ZONEGROUP_PLACEMENT_LIST,
  OPT_ZONEGROUP_PLACEMENT_DEFAULT,
  OPT_ZONE_CREATE,
  OPT_ZONE_DELETE,
  OPT_ZONE_GET,
  OPT_ZONE_MODIFY,
  OPT_ZONE_SET,
  OPT_ZONE_LIST,
  OPT_ZONE_RENAME,
  OPT_ZONE_DEFAULT,
  OPT_ZONE_PLACEMENT_ADD,
  OPT_ZONE_PLACEMENT_MODIFY,
  OPT_ZONE_PLACEMENT_RM,
  OPT_ZONE_PLACEMENT_LIST,
  OPT_CAPS_ADD,
  OPT_CAPS_RM,
  OPT_METADATA_GET,
  OPT_METADATA_PUT,
  OPT_METADATA_RM,
  OPT_METADATA_LIST,
  OPT_METADATA_SYNC_STATUS,
  OPT_METADATA_SYNC_INIT,
  OPT_METADATA_SYNC_RUN,
  OPT_MDLOG_LIST,
  OPT_MDLOG_AUTOTRIM,
  OPT_MDLOG_TRIM,
  OPT_MDLOG_FETCH,
  OPT_MDLOG_STATUS,
  OPT_SYNC_ERROR_LIST,
  OPT_SYNC_ERROR_TRIM,
  OPT_BILOG_LIST,
  OPT_BILOG_TRIM,
  OPT_BILOG_STATUS,
  OPT_BILOG_AUTOTRIM,
  OPT_DATA_SYNC_STATUS,
  OPT_DATA_SYNC_INIT,
  OPT_DATA_SYNC_RUN,
  OPT_DATALOG_LIST,
  OPT_DATALOG_STATUS,
  OPT_DATALOG_TRIM,
  OPT_OPSTATE_LIST,
  OPT_OPSTATE_SET,
  OPT_OPSTATE_RENEW,
  OPT_OPSTATE_RM,
  OPT_REPLICALOG_GET,
  OPT_REPLICALOG_UPDATE,
  OPT_REPLICALOG_DELETE,
  OPT_REALM_CREATE,
  OPT_REALM_DELETE,
  OPT_REALM_GET,
  OPT_REALM_GET_DEFAULT,
  OPT_REALM_LIST,
  OPT_REALM_LIST_PERIODS,
  OPT_REALM_RENAME,
  OPT_REALM_SET,
  OPT_REALM_DEFAULT,
  OPT_REALM_PULL,
  OPT_PERIOD_DELETE,
  OPT_PERIOD_GET,
  OPT_PERIOD_GET_CURRENT,
  OPT_PERIOD_PULL,
  OPT_PERIOD_PUSH,
  OPT_PERIOD_LIST,
  OPT_PERIOD_UPDATE,
  OPT_PERIOD_COMMIT,
  OPT_GLOBAL_QUOTA_GET,
  OPT_GLOBAL_QUOTA_SET,
  OPT_GLOBAL_QUOTA_ENABLE,
  OPT_GLOBAL_QUOTA_DISABLE,
  OPT_SYNC_STATUS,
  OPT_ROLE_CREATE,
  OPT_ROLE_DELETE,
  OPT_ROLE_GET,
  OPT_ROLE_MODIFY,
  OPT_ROLE_LIST,
  OPT_ROLE_POLICY_PUT,
  OPT_ROLE_POLICY_LIST,
  OPT_ROLE_POLICY_GET,
  OPT_ROLE_POLICY_DELETE,
  OPT_RESHARD_ADD,
  OPT_RESHARD_LIST,
  OPT_RESHARD_STATUS,
  OPT_RESHARD_PROCESS,
  OPT_RESHARD_CANCEL,
};

enum ReplicaLogType {
  ReplicaLog_Invalid = 0,
  ReplicaLog_Metadata,
  ReplicaLog_Data,
  ReplicaLog_Bucket,
};

int init_bucket(RGWRados *store, const std::string& tenant_name, const std::string& bucket_name, const std::string& bucket_id,
                RGWBucketInfo& bucket_info, rgw_bucket& bucket, map<std::string, bufferlist> *pattrs = nullptr);

int read_input(const std::string& infile, bufferlist& bl);

template <class T>
static int read_decode_json(const std::string& infile, T& t)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "failed to parse JSON" << std::endl;
    return -EINVAL;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}

int parse_date_str(const std::string& date_str, utime_t& ut);

int check_min_obj_stripe_size(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, uint64_t min_stripe_size, bool *need_rewrite);

int read_current_period_id(RGWRados* store, const std::string& realm_id,
                           const std::string& realm_name,
                           std::string* period_id);

int check_reshard_bucket_params(RGWRados *store,
                                const std::string& bucket_name,
                                const std::string& tenant,
                                const std::string& bucket_id,
                                bool num_shards_specified,
                                int num_shards,
                                int yes_i_really_mean_it,
                                rgw_bucket& bucket,
                                RGWBucketInfo& bucket_info,
                                map<std::string, bufferlist>& attrs);

#endif //CEPH_RGW_ADMIN_COMMON_H
