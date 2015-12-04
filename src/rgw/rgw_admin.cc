// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#include "auth/Crypto.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "global/global_init.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_replica_log.h"
#include "rgw_orphan.h"

#define dout_subsys ceph_subsys_rgw

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

static RGWRados *store = NULL;

void _usage() 
{
  cout << "usage: radosgw-admin <cmd> [options...]" << std::endl;
  cout << "commands:\n";
  cout << "  user create                create a new user\n" ;
  cout << "  user modify                modify user\n";
  cout << "  user info                  get user info\n";
  cout << "  user rm                    remove user\n";
  cout << "  user suspend               suspend a user\n";
  cout << "  user enable                re-enable user after suspension\n";
  cout << "  user check                 check user info\n";
  cout << "  user stats                 show user stats as accounted by quota subsystem\n";
  cout << "  caps add                   add user capabilities\n";
  cout << "  caps rm                    remove user capabilities\n";
  cout << "  subuser create             create a new subuser\n" ;
  cout << "  subuser modify             modify subuser\n";
  cout << "  subuser rm                 remove subuser\n";
  cout << "  key create                 create access key\n";
  cout << "  key rm                     remove access key\n";
  cout << "  bucket list                list buckets\n";
  cout << "  bucket link                link bucket to specified user\n";
  cout << "  bucket unlink              unlink bucket from specified user\n";
  cout << "  bucket stats               returns bucket statistics\n";
  cout << "  bucket rm                  remove bucket\n";
  cout << "  bucket check               check bucket index\n";
  cout << "  object rm                  remove object\n";
  cout << "  object unlink              unlink object from bucket index\n";
  cout << "  objects expire             run expired objects cleanup\n";
  cout << "  quota set                  set quota params\n";
  cout << "  quota enable               enable quota\n";
  cout << "  quota disable              disable quota\n";
  cout << "  region get                 show region info\n";
  cout << "  regions list               list all regions set on this cluster\n";
  cout << "  region set                 set region info (requires infile)\n";
  cout << "  region default             set default region\n";
  cout << "  region-map get             show region-map\n";
  cout << "  region-map set             set region-map (requires infile)\n";
  cout << "  zone get                   show zone cluster params\n";
  cout << "  zone set                   set zone cluster params (requires infile)\n";
  cout << "  zone list                  list all zones set on this cluster\n";
  cout << "  pool add                   add an existing pool for data placement\n";
  cout << "  pool rm                    remove an existing pool from data placement set\n";
  cout << "  pools list                 list placement active set\n";
  cout << "  policy                     read bucket/object policy\n";
  cout << "  log list                   list log objects\n";
  cout << "  log show                   dump a log from specific object or (bucket + date\n";
  cout << "                             + bucket-id)\n";
  cout << "  log rm                     remove log object\n";
  cout << "  usage show                 show usage (by user, date range)\n";
  cout << "  usage trim                 trim usage (by user, date range)\n";
  cout << "  temp remove                remove temporary objects that were created up to\n";
  cout << "                             specified date (and optional time)\n";
  cout << "  gc list                    dump expired garbage collection objects (specify\n";
  cout << "                             --include-all to list all entries, including unexpired)\n";
  cout << "  gc process                 manually process garbage\n";
  cout << "  metadata get               get metadata info\n";
  cout << "  metadata put               put metadata info\n";
  cout << "  metadata rm                remove metadata info\n";
  cout << "  metadata list              list metadata info\n";
  cout << "  mdlog list                 list metadata log\n";
  cout << "  mdlog trim                 trim metadata log (use start-date, end-date or\n";
  cout << "                             start-marker, end-marker)\n";
  cout << "  bilog list                 list bucket index log\n";
  cout << "  bilog trim                 trim bucket index log (use start-marker, end-marker)\n";
  cout << "  datalog list               list data log\n";
  cout << "  datalog trim               trim data log\n";
  cout << "  opstate list               list stateful operations entries (use client_id,\n";
  cout << "                             op_id, object)\n";
  cout << "  opstate set                set state on an entry (use client_id, op_id, object, state)\n";
  cout << "  opstate renew              renew state on an entry (use client_id, op_id, object)\n";
  cout << "  opstate rm                 remove entry (use client_id, op_id, object)\n";
  cout << "  replicalog get             get replica metadata log entry\n";
  cout << "  replicalog update          update replica metadata log entry\n";
  cout << "  replicalog delete          delete replica metadata log entry\n";
  cout << "options:\n";
  cout << "   --uid=<id>                user id\n";
  cout << "   --subuser=<name>          subuser name\n";
  cout << "   --access-key=<key>        S3 access key\n";
  cout << "   --email=<email>\n";
  cout << "   --secret/--secret-key=<key>\n";
  cout << "                             specify secret key\n";
  cout << "   --gen-access-key          generate random access key (for S3)\n";
  cout << "   --gen-secret              generate random secret key\n";
  cout << "   --key-type=<type>         key type, options are: swift, s3\n";
  cout << "   --temp-url-key[-2]=<key>  temp url key\n";
  cout << "   --access=<access>         Set access permissions for sub-user, should be one\n";
  cout << "                             of read, write, readwrite, full\n";
  cout << "   --display-name=<name>\n";
  cout << "   --max_buckets             max number of buckets for a user\n";
  cout << "   --system                  set the system flag on the user\n";
  cout << "   --bucket=<bucket>\n";
  cout << "   --pool=<pool>\n";
  cout << "   --object=<object>\n";
  cout << "   --date=<date>\n";
  cout << "   --start-date=<date>\n";
  cout << "   --end-date=<date>\n";
  cout << "   --bucket-id=<bucket-id>\n";
  cout << "   --shard-id=<shard-id>     optional for mdlog list\n";
  cout << "                             required for: \n";
  cout << "                               mdlog trim\n";
  cout << "                               replica mdlog get/delete\n";
  cout << "                               replica datalog get/delete\n";
  cout << "   --metadata-key=<key>      key to retrieve metadata from with metadata get\n";
  cout << "   --rgw-region=<region>     region in which radosgw is running\n";
  cout << "   --rgw-zone=<zone>         zone in which radosgw is running\n";
  cout << "   --fix                     besides checking bucket index, will also fix it\n";
  cout << "   --check-objects           bucket check: rebuilds bucket index according to\n";
  cout << "                             actual objects state\n";
  cout << "   --format=<format>         specify output format for certain operations: xml,\n";
  cout << "                             json\n";
  cout << "   --purge-data              when specified, user removal will also purge all the\n";
  cout << "                             user data\n";
  cout << "   --purge-keys              when specified, subuser removal will also purge all the\n";
  cout << "                             subuser keys\n";
  cout << "   --purge-objects           remove a bucket's objects before deleting it\n";
  cout << "                             (NOTE: required to delete a non-empty bucket)\n";
  cout << "   --sync-stats              option to 'user stats', update user stats with current\n";
  cout << "                             stats reported by user's buckets indexes\n";
  cout << "   --show-log-entries=<flag> enable/disable dump of log entries on log show\n";
  cout << "   --show-log-sum=<flag>     enable/disable dump of log summation on log show\n";
  cout << "   --skip-zero-entries       log show only dumps entries that don't have zero value\n";
  cout << "                             in one of the numeric field\n";
  cout << "   --infile                  specify a file to read in when setting data\n";
  cout << "   --state=<state string>    specify a state for the opstate set command\n";
  cout << "   --replica-log-type        replica log type (metadata, data, bucket), required for\n";
  cout << "                             replica log operations\n";
  cout << "   --categories=<list>       comma separated list of categories, used in usage show\n";
  cout << "   --caps=<caps>             list of caps (e.g., \"usage=read, write; user=read\"\n";
  cout << "   --yes-i-really-mean-it    required for certain operations\n";
  cout << "   --reset-regions           reset regionmap when regionmap update";
  cout << "\n";
  cout << "<date> := \"YYYY-MM-DD[ hh:mm:ss]\"\n";
  cout << "\nQuota options:\n";
  cout << "   --bucket                  specified bucket for quota command\n";
  cout << "   --max-objects             specify max objects (negative value to disable)\n";
  cout << "   --max-size                specify max size (in bytes, negative value to disable)\n";
  cout << "   --quota-scope             scope of quota (bucket, user)\n";
  cout << "\n";
  generic_client_usage();
}

int usage()
{
  _usage();
  return 1;
}

void usage_exit()
{
  _usage();
  exit(1);
}

enum {
  OPT_NO_CMD = 0,
  OPT_USER_CREATE,
  OPT_USER_INFO,
  OPT_USER_MODIFY,
  OPT_USER_RM,
  OPT_USER_SUSPEND,
  OPT_USER_ENABLE,
  OPT_USER_CHECK,
  OPT_USER_STATS,
  OPT_SUBUSER_CREATE,
  OPT_SUBUSER_MODIFY,
  OPT_SUBUSER_RM,
  OPT_KEY_CREATE,
  OPT_KEY_RM,
  OPT_BUCKETS_LIST,
  OPT_BUCKET_LINK,
  OPT_BUCKET_UNLINK,
  OPT_BUCKET_STATS,
  OPT_BUCKET_CHECK,
  OPT_BUCKET_RM,
  OPT_BUCKET_REWRITE,
  OPT_POLICY,
  OPT_POOL_ADD,
  OPT_POOL_RM,
  OPT_POOLS_LIST,
  OPT_LOG_LIST,
  OPT_LOG_SHOW,
  OPT_LOG_RM,
  OPT_USAGE_SHOW,
  OPT_USAGE_TRIM,
  OPT_OBJECT_RM,
  OPT_OBJECT_UNLINK,
  OPT_OBJECT_STAT,
  OPT_OBJECT_REWRITE,
  OPT_OBJECTS_EXPIRE,
  OPT_BI_GET,
  OPT_BI_PUT,
  OPT_BI_LIST,
  OPT_OLH_GET,
  OPT_OLH_READLOG,
  OPT_QUOTA_SET,
  OPT_QUOTA_ENABLE,
  OPT_QUOTA_DISABLE,
  OPT_GC_LIST,
  OPT_GC_PROCESS,
  OPT_ORPHANS_FIND,
  OPT_ORPHANS_FINISH,
  OPT_REGION_GET,
  OPT_REGION_LIST,
  OPT_REGION_SET,
  OPT_REGION_DEFAULT,
  OPT_REGIONMAP_GET,
  OPT_REGIONMAP_SET,
  OPT_REGIONMAP_UPDATE,
  OPT_ZONE_GET,
  OPT_ZONE_SET,
  OPT_ZONE_LIST,
  OPT_CAPS_ADD,
  OPT_CAPS_RM,
  OPT_METADATA_GET,
  OPT_METADATA_PUT,
  OPT_METADATA_RM,
  OPT_METADATA_LIST,
  OPT_MDLOG_LIST,
  OPT_MDLOG_TRIM,
  OPT_BILOG_LIST,
  OPT_BILOG_TRIM,
  OPT_DATALOG_LIST,
  OPT_DATALOG_TRIM,
  OPT_OPSTATE_LIST,
  OPT_OPSTATE_SET,
  OPT_OPSTATE_RENEW,
  OPT_OPSTATE_RM,
  OPT_REPLICALOG_GET,
  OPT_REPLICALOG_UPDATE,
  OPT_REPLICALOG_DELETE,
};

static int get_cmd(const char *cmd, const char *prev_cmd, bool *need_more)
{
  *need_more = false;
  // NOTE: please keep the checks in alphabetical order !!!
  if (strcmp(cmd, "bi") == 0 ||
      strcmp(cmd, "bilog") == 0 ||
      strcmp(cmd, "bucket") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "caps") == 0 ||
      strcmp(cmd, "datalog") == 0 ||
      strcmp(cmd, "gc") == 0 || 
      strcmp(cmd, "key") == 0 ||
      strcmp(cmd, "log") == 0 ||
      strcmp(cmd, "mdlog") == 0 ||
      strcmp(cmd, "metadata") == 0 ||
      strcmp(cmd, "object") == 0 ||
      strcmp(cmd, "objects") == 0 ||
      strcmp(cmd, "olh") == 0 ||
      strcmp(cmd, "opstate") == 0 ||
      strcmp(cmd, "orphans") == 0 || 
      strcmp(cmd, "pool") == 0 ||
      strcmp(cmd, "pools") == 0 ||
      strcmp(cmd, "quota") == 0 ||
      strcmp(cmd, "region") == 0 ||
      strcmp(cmd, "regions") == 0 ||
      strcmp(cmd, "region-map") == 0 ||
      strcmp(cmd, "regionmap") == 0 ||
      strcmp(cmd, "replicalog") == 0 ||
      strcmp(cmd, "subuser") == 0 ||
      strcmp(cmd, "temp") == 0 ||
      strcmp(cmd, "usage") == 0 ||
      strcmp(cmd, "user") == 0 ||
      strcmp(cmd, "zone") == 0) {
    *need_more = true;
    return 0;
  }

  if (strcmp(cmd, "policy") == 0)
    return OPT_POLICY;

  if (!prev_cmd)
    return -EINVAL;

  if (strcmp(prev_cmd, "user") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_USER_CREATE;
    if (strcmp(cmd, "info") == 0)
      return OPT_USER_INFO;
    if (strcmp(cmd, "modify") == 0)
      return OPT_USER_MODIFY;
    if (strcmp(cmd, "rm") == 0)
      return OPT_USER_RM;
    if (strcmp(cmd, "suspend") == 0)
      return OPT_USER_SUSPEND;
    if (strcmp(cmd, "enable") == 0)
      return OPT_USER_ENABLE;
    if (strcmp(cmd, "check") == 0)
      return OPT_USER_CHECK;
    if (strcmp(cmd, "stats") == 0)
      return OPT_USER_STATS;
  } else if (strcmp(prev_cmd, "subuser") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_SUBUSER_CREATE;
    if (strcmp(cmd, "modify") == 0)
      return OPT_SUBUSER_MODIFY;
    if (strcmp(cmd, "rm") == 0)
      return OPT_SUBUSER_RM;
  } else if (strcmp(prev_cmd, "key") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_KEY_CREATE;
    if (strcmp(cmd, "rm") == 0)
      return OPT_KEY_RM;
  } else if (strcmp(prev_cmd, "buckets") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BUCKETS_LIST;
  } else if (strcmp(prev_cmd, "bucket") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BUCKETS_LIST;
    if (strcmp(cmd, "link") == 0)
      return OPT_BUCKET_LINK;
    if (strcmp(cmd, "unlink") == 0)
      return OPT_BUCKET_UNLINK;
    if (strcmp(cmd, "stats") == 0)
      return OPT_BUCKET_STATS;
    if (strcmp(cmd, "rm") == 0)
      return OPT_BUCKET_RM;
    if (strcmp(cmd, "rewrite") == 0)
      return OPT_BUCKET_REWRITE;
    if (strcmp(cmd, "check") == 0)
      return OPT_BUCKET_CHECK;
  } else if (strcmp(prev_cmd, "log") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_LOG_LIST;
    if (strcmp(cmd, "show") == 0)
      return OPT_LOG_SHOW;
    if (strcmp(cmd, "rm") == 0)
      return OPT_LOG_RM;
  } else if (strcmp(prev_cmd, "usage") == 0) {
    if (strcmp(cmd, "show") == 0)
      return OPT_USAGE_SHOW;
    if (strcmp(cmd, "trim") == 0)
      return OPT_USAGE_TRIM;
  } else if (strcmp(prev_cmd, "caps") == 0) {
    if (strcmp(cmd, "add") == 0)
      return OPT_CAPS_ADD;
    if (strcmp(cmd, "rm") == 0)
      return OPT_CAPS_RM;
  } else if (strcmp(prev_cmd, "pool") == 0) {
    if (strcmp(cmd, "add") == 0)
      return OPT_POOL_ADD;
    if (strcmp(cmd, "rm") == 0)
      return OPT_POOL_RM;
    if (strcmp(cmd, "list") == 0)
      return OPT_POOLS_LIST;
  } else if (strcmp(prev_cmd, "pools") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_POOLS_LIST;
  } else if (strcmp(prev_cmd, "object") == 0) {
    if (strcmp(cmd, "rm") == 0)
      return OPT_OBJECT_RM;
    if (strcmp(cmd, "unlink") == 0)
      return OPT_OBJECT_UNLINK;
    if (strcmp(cmd, "stat") == 0)
      return OPT_OBJECT_STAT;
    if (strcmp(cmd, "rewrite") == 0)
      return OPT_OBJECT_REWRITE;
  } else if (strcmp(prev_cmd, "objects") == 0) {
    if (strcmp(cmd, "expire") == 0)
      return OPT_OBJECTS_EXPIRE;
  } else if (strcmp(prev_cmd, "olh") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_OLH_GET;
    if (strcmp(cmd, "readlog") == 0)
      return OPT_OLH_READLOG;
  } else if (strcmp(prev_cmd, "bi") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_BI_GET;
    if (strcmp(cmd, "put") == 0)
      return OPT_BI_PUT;
    if (strcmp(cmd, "list") == 0)
      return OPT_BI_LIST;
  } else if (strcmp(prev_cmd, "region") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_REGION_GET;
    if (strcmp(cmd, "list") == 0)
      return OPT_REGION_LIST;
    if (strcmp(cmd, "set") == 0)
      return OPT_REGION_SET;
    if (strcmp(cmd, "default") == 0)
      return OPT_REGION_DEFAULT;
  } else if (strcmp(prev_cmd, "quota") == 0) {
    if (strcmp(cmd, "set") == 0)
      return OPT_QUOTA_SET;
    if (strcmp(cmd, "enable") == 0)
      return OPT_QUOTA_ENABLE;
    if (strcmp(cmd, "disable") == 0)
      return OPT_QUOTA_DISABLE;
  } else if (strcmp(prev_cmd, "regions") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_REGION_LIST;
  } else if (strcmp(prev_cmd, "region-map") == 0 ||
             strcmp(prev_cmd, "regionmap") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_REGIONMAP_GET;
    if (strcmp(cmd, "set") == 0)
      return OPT_REGIONMAP_SET;
    if (strcmp(cmd, "update") == 0)
      return OPT_REGIONMAP_UPDATE;
  } else if (strcmp(prev_cmd, "zone") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_ZONE_GET;
    if (strcmp(cmd, "set") == 0)
      return OPT_ZONE_SET;
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONE_LIST;
  } else if (strcmp(prev_cmd, "zones") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONE_LIST;
  } else if (strcmp(prev_cmd, "gc") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_GC_LIST;
    if (strcmp(cmd, "process") == 0)
      return OPT_GC_PROCESS;
  } else if (strcmp(prev_cmd, "orphans") == 0) {
    if (strcmp(cmd, "find") == 0)
      return OPT_ORPHANS_FIND;
    if (strcmp(cmd, "finish") == 0)
      return OPT_ORPHANS_FINISH;
  } else if (strcmp(prev_cmd, "metadata") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_METADATA_GET;
    if (strcmp(cmd, "put") == 0)
      return OPT_METADATA_PUT;
    if (strcmp(cmd, "rm") == 0)
      return OPT_METADATA_RM;
    if (strcmp(cmd, "list") == 0)
      return OPT_METADATA_LIST;
  } else if (strcmp(prev_cmd, "mdlog") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_MDLOG_LIST;
    if (strcmp(cmd, "trim") == 0)
      return OPT_MDLOG_TRIM;
  } else if (strcmp(prev_cmd, "bilog") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BILOG_LIST;
    if (strcmp(cmd, "trim") == 0)
      return OPT_BILOG_TRIM;
  } else if (strcmp(prev_cmd, "datalog") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_DATALOG_LIST;
    if (strcmp(cmd, "trim") == 0)
      return OPT_DATALOG_TRIM;
  } else if (strcmp(prev_cmd, "opstate") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_OPSTATE_LIST;
    if (strcmp(cmd, "set") == 0)
      return OPT_OPSTATE_SET;
    if (strcmp(cmd, "renew") == 0)
      return OPT_OPSTATE_RENEW;
    if (strcmp(cmd, "rm") == 0)
      return OPT_OPSTATE_RM;
  } else if (strcmp(prev_cmd, "replicalog") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_REPLICALOG_GET;
    if (strcmp(cmd, "update") == 0)
      return OPT_REPLICALOG_UPDATE;
    if (strcmp(cmd, "delete") == 0)
      return OPT_REPLICALOG_DELETE;
  }

  return -EINVAL;
}

enum ReplicaLogType {
  ReplicaLog_Invalid = 0,
  ReplicaLog_Metadata,
  ReplicaLog_Data,
  ReplicaLog_Bucket,
};

ReplicaLogType get_replicalog_type(const string& name) {
  if (name == "md" || name == "meta" || name == "metadata")
    return ReplicaLog_Metadata;
  if (name == "data")
    return ReplicaLog_Data;
  if (name == "bucket")
    return ReplicaLog_Bucket;

  return ReplicaLog_Invalid;
}

BIIndexType get_bi_index_type(const string& type_str) {
  if (type_str == "plain")
    return PlainIdx;
  if (type_str == "instance")
    return InstanceIdx;
  if (type_str == "olh")
    return OLHIdx;

  return InvalidIdx;
}

void dump_bi_entry(bufferlist& bl, BIIndexType index_type, Formatter *formatter)
{
  bufferlist::iterator iter = bl.begin();
  switch (index_type) {
    case PlainIdx:
    case InstanceIdx:
      {
        rgw_bucket_dir_entry entry;
        ::decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    case OLHIdx:
      {
        rgw_bucket_olh_entry entry;
        ::decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    default:
      assert(0);
      break;
  }
}

static void show_user_info(RGWUserInfo& info, Formatter *formatter)
{
  encode_json("user_info", info, formatter);
  formatter->flush(cout);
  cout << std::endl;
}

static void dump_bucket_usage(map<RGWObjCategory, RGWStorageStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWStorageStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWStorageStats& s = iter->second;
    const char *cat_name = rgw_obj_category_name(iter->first);
    formatter->open_object_section(cat_name);
    formatter->dump_int("size_kb", s.num_kb);
    formatter->dump_int("size_kb_actual", s.num_kb_rounded);
    formatter->dump_int("num_objects", s.num_objects);
    formatter->close_section();
    formatter->flush(cout);
  }
  formatter->close_section();
}

int bucket_stats(rgw_bucket& bucket, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  time_t mtime;
  RGWObjectCtx obj_ctx(store);
  int r = store->get_bucket_info(obj_ctx, bucket.tenant, bucket.name, bucket_info, &mtime);
  if (r < 0)
    return r;

  map<RGWObjCategory, RGWStorageStats> stats;
  string bucket_ver, master_ver;
  string max_marker;
  int ret = store->get_bucket_stats(bucket, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }
  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_string("pool", bucket.data_pool);
  formatter->dump_string("index_pool", bucket.index_pool);
  
  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  ::encode_json("owner", bucket_info.owner, formatter);
  formatter->dump_int("mtime", mtime);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  formatter->close_section();

  return 0;
}

class StoreDestructor {
  RGWRados *store;
public:
  StoreDestructor(RGWRados *_s) : store(_s) {}
  ~StoreDestructor() {
    RGWStoreManager::close_storage(store);
  }
};

static int init_bucket(const string& tenant_name, const string& bucket_name, const string& bucket_id,
                       RGWBucketInfo& bucket_info, rgw_bucket& bucket)
{
  if (!bucket_name.empty()) {
    RGWObjectCtx obj_ctx(store);
    int r;
    if (bucket_id.empty()) {
      r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, NULL);
    } else {
      string bucket_instance_id = bucket_name + ":" + bucket_id;
      r = store->get_bucket_instance_info(obj_ctx, bucket_instance_id, bucket_info, NULL, NULL);
    }
    if (r < 0) {
      cerr << "could not get bucket info for bucket=" << bucket_name << std::endl;
      return r;
    }
    bucket = bucket_info.bucket;
  }
  return 0;
}

static int read_input(const string& infile, bufferlist& bl)
{
  int fd = 0;
  if (infile.size()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      cerr << "error reading input file " << infile << std::endl;
      return err;
    }
  }

#define READ_CHUNK 8196
  int r;
  int err;

  do {
    char buf[READ_CHUNK];

    r = read(fd, buf, READ_CHUNK);
    if (r < 0) {
      err = -errno;
      cerr << "error while reading input" << std::endl;
      goto out;
    }
    bl.append(buf, r);
  } while (r > 0);
  err = 0;

 out:
  if (infile.size()) {
    close(fd);
  }
  return err;
}

template <class T>
static int read_decode_json(const string& infile, T& t)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  ret = p.parse(bl.c_str(), bl.length());
  if (ret < 0) {
    cout << "failed to parse JSON" << std::endl;
    return ret;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}
    
template <class T, class K>
static int read_decode_json(const string& infile, T& t, K *k)
{
  bufferlist bl;
  int ret = read_input(infile, bl);
  if (ret < 0) {
    cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  JSONParser p;
  ret = p.parse(bl.c_str(), bl.length());
  if (ret < 0) {
    cout << "failed to parse JSON" << std::endl;
    return ret;
  }

  try {
    t.decode_json(&p, k);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}

static int parse_date_str(const string& date_str, utime_t& ut)
{
  uint64_t epoch = 0;
  uint64_t nsec = 0;

  if (!date_str.empty()) {
    int ret = utime_t::parse_date(date_str, &epoch, &nsec);
    if (ret < 0) {
      cerr << "ERROR: failed to parse date: " << date_str << std::endl;
      return -EINVAL;
    }
  }

  ut = utime_t(epoch, nsec);

  return 0;
}

template <class T>
static bool decode_dump(const char *field_name, bufferlist& bl, Formatter *f)
{
  T t;

  bufferlist::iterator iter = bl.begin();

  try {
    ::decode(t, iter);
  } catch (buffer::error& err) {
    return false;
  }

  encode_json(field_name, t, f);

  return true;
}

static bool dump_string(const char *field_name, bufferlist& bl, Formatter *f)
{
  string val;
  if (bl.length() > 0) {
    val.assign(bl.c_str(), bl.length());
  }
  f->dump_string(field_name, val);

  return true;
}

void set_quota_info(RGWQuotaInfo& quota, int opt_cmd, int64_t max_size, int64_t max_objects,
                    bool have_max_size, bool have_max_objects)
{
  switch (opt_cmd) {
    case OPT_QUOTA_ENABLE:
      quota.enabled = true;

      // falling through on purpose

    case OPT_QUOTA_SET:
      if (have_max_objects) {
        quota.max_objects = max_objects;
      }
      if (have_max_size) {
        if (max_size < 0) {
          quota.max_size_kb = -1;
        } else {
          quota.max_size_kb = rgw_rounded_kb(max_size);
        }
      }
      break;
    case OPT_QUOTA_DISABLE:
      quota.enabled = false;
      break;
  }
}

int set_bucket_quota(RGWRados *store, int opt_cmd,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_size, int64_t max_objects,
                     bool have_max_size, bool have_max_objects)
{
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);
  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, NULL, &attrs);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket_info.quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

   r = store->put_bucket_instance_info(bucket_info, false, 0, &attrs);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_user_bucket_quota(int opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                          bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.bucket_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_bucket_quota(user_info.bucket_quota);

  string err;
  int r = user.modify(op_state, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int set_user_quota(int opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                   bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.user_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_user_quota(user_info.user_quota);

  string err;
  int r = user.modify(op_state, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

static bool bucket_object_check_filter(const string& name)
{
  string ns;
  string obj = name;
  string instance;
  return rgw_obj::translate_raw_obj_to_obj_in_ns(obj, instance, ns);
}

int check_min_obj_stripe_size(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, uint64_t min_stripe_size, bool *need_rewrite)
{
  map<string, bufferlist> attrs;
  uint64_t obj_size;

  RGWObjectCtx obj_ctx(store);
  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare(NULL, NULL);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<string, bufferlist>::iterator iter;
  iter = attrs.find(RGW_ATTR_MANIFEST);
  if (iter == attrs.end()) {
    *need_rewrite = (obj_size >= min_stripe_size);
    return 0;
  }

  RGWObjManifest manifest;

  try {
    bufferlist& bl = iter->second;
    bufferlist::iterator biter = bl.begin();
    ::decode(manifest, biter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode manifest" << dendl;
    return -EIO;
  }

  map<uint64_t, RGWObjManifestPart>& objs = manifest.get_explicit_objs();
  map<uint64_t, RGWObjManifestPart>::iterator oiter;
  for (oiter = objs.begin(); oiter != objs.end(); ++oiter) {
    RGWObjManifestPart& part = oiter->second;

    if (part.size >= min_stripe_size) {
      *need_rewrite = true;
      return 0;
    }
  }
  *need_rewrite = false;

  return 0;
}


int check_obj_locator_underscore(RGWBucketInfo& bucket_info, rgw_obj& obj, rgw_obj_key& key, bool fix, bool remove_bad, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "head");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  string oid;
  string locator;

  get_obj_bucket_and_oid_loc(obj, obj.bucket, oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);

  
  RGWObjectCtx obj_ctx(store);

  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  int ret = read_op.prepare(NULL, NULL);
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = store->fix_head_obj_locator(obj.bucket, needs_fixing, remove_bad, key);
    if (ret < 0) {
      cerr << "ERROR: fix_head_object_locator() returned ret=" << ret << std::endl;
      goto done;
    }
    status = "fixed";
  }

done:
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

int check_obj_tail_locator_underscore(RGWBucketInfo& bucket_info, rgw_obj& obj, rgw_obj_key& key, bool fix, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "tail");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  string oid;
  string locator;

  bool needs_fixing;
  string status;

  int ret = store->fix_tail_obj_locator(obj.bucket, key, fix, &needs_fixing);
  if (ret < 0) {
    cerr << "ERROR: fix_tail_object_locator_underscore() returned ret=" << ret << std::endl;
    status = "failed";
  } else {
    status = (needs_fixing && !fix ? "needs_fixing" : "ok");
  }

  f->dump_bool("needs_fixing", needs_fixing);
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

int do_check_object_locator(const string& tenant_name, const string& bucket_name,
                            bool fix, bool remove_bad, Formatter *f)
{
  if (remove_bad && !fix) {
    cerr << "ERROR: can't have remove_bad specified without fix" << std::endl;
    return -EINVAL;
  }

  RGWBucketInfo bucket_info;
  rgw_bucket bucket;
  string bucket_id;

  f->open_object_section("bucket");
  f->dump_string("bucket", bucket_name);
  int ret = init_bucket(tenant_name, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  bool truncated;
  int count = 0;

  int max_entries = 1000;

  string prefix;
  string delim;
  vector<RGWObjEnt> result;
  map<string, bool> common_prefixes;
  string ns;

  RGWRados::Bucket target(store, bucket);
  RGWRados::Bucket::List list_op(&target);

  string marker;

  list_op.params.prefix = prefix;
  list_op.params.delim = delim;
  list_op.params.marker = rgw_obj_key(marker);
  list_op.params.ns = ns;
  list_op.params.enforce_ns = true;
  list_op.params.list_versions = true;
  
  f->open_array_section("check_objects");
  do {
    ret = list_op.list_objects(max_entries - count, &result, &common_prefixes, &truncated);
    if (ret < 0) {
      cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += result.size();

    list<rgw_obj> objs;
    for (vector<RGWObjEnt>::iterator iter = result.begin(); iter != result.end(); ++iter) {
      rgw_obj_key key = iter->key;
      rgw_obj obj(bucket, key);

      if (key.name[0] == '_') {
        ret = check_obj_locator_underscore(bucket_info, obj, key, fix, remove_bad, f);
	
	if (ret >= 0) {
          ret = check_obj_tail_locator_underscore(bucket_info, obj, key, fix, f);
	}
      }
    }
    f->flush(cout);
  } while (truncated && count < max_entries);
  f->close_section();
  f->close_section();

  f->flush(cout);

  return 0;
}

int main(int argc, char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  rgw_user user_id;
  string tenant;
  std::string access_key, secret_key, user_email, display_name;
  std::string bucket_name, pool_name, object;
  std::string date, subuser, access, format;
  std::string start_date, end_date;
  std::string key_type_str;
  int key_type = KEY_TYPE_UNDEFINED;
  rgw_bucket bucket;
  uint32_t perm_mask = 0;
  RGWUserInfo info;
  int opt_cmd = OPT_NO_CMD;
  bool need_more;
  int gen_access_key = 0;
  int gen_secret_key = 0;
  bool set_perm = false;
  bool set_temp_url_key = false;
  map<int, string> temp_url_keys;
  string bucket_id;
  Formatter *formatter = NULL;
  int purge_data = false;
  RGWBucketInfo bucket_info;
  int pretty_format = false;
  int show_log_entries = true;
  int show_log_sum = true;
  int skip_zero_entries = false;  // log show
  int purge_keys = false;
  int yes_i_really_mean_it = false;
  int delete_child_objects = false;
  int fix = false;
  int remove_bad = false;
  int check_head_obj_locator = false;
  int max_buckets = -1;
  map<string, bool> categories;
  string caps;
  int check_objects = false;
  RGWUserAdminOpState user_op;
  RGWBucketAdminOpState bucket_op;
  string infile;
  string metadata_key;
  RGWObjVersionTracker objv_tracker;
  string marker;
  string start_marker;
  string end_marker;
  int max_entries = -1;
  int system = false;
  bool system_specified = false;
  int shard_id = -1;
  bool specified_shard_id = false;
  string daemon_id;
  bool specified_daemon_id = false;
  string client_id;
  string op_id;
  string state_str;
  string replica_log_type_str;
  ReplicaLogType replica_log_type = ReplicaLog_Invalid;
  string op_mask_str;
  string quota_scope;
  string object_version;

  int64_t max_objects = -1;
  int64_t max_size = -1;
  bool have_max_objects = false;
  bool have_max_size = false;
  int include_all = false;

  int sync_stats = false;
  int reset_regions = false;

  uint64_t min_rewrite_size = 4 * 1024 * 1024;
  uint64_t max_rewrite_size = ULLONG_MAX;
  uint64_t min_rewrite_stripe_size = 0;

  BIIndexType bi_index_type = PlainIdx;

  string job_id;
  int num_shards = 0;
  int max_concurrent_ios = 32;
  uint64_t orphan_stale_secs = (24 * 3600);

  std::string val;
  std::ostringstream errs;
  string err;
  long long tmp = 0;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--uid", (char*)NULL)) {
      user_id.from_str(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--tenant", (char*)NULL)) {
      tenant = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--access-key", (char*)NULL)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--subuser", (char*)NULL)) {
      subuser = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--secret", "--secret-key", (char*)NULL)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-e", "--email", (char*)NULL)) {
      user_email = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-n", "--display-name", (char*)NULL)) {
      display_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", "--bucket", (char*)NULL)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      pool_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--object", (char*)NULL)) {
      object = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--object-version", (char*)NULL)) {
      object_version = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--client-id", (char*)NULL)) {
      client_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--op-id", (char*)NULL)) {
      op_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--state", (char*)NULL)) {
      state_str = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--op-mask", (char*)NULL)) {
      op_mask_str = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--key-type", (char*)NULL)) {
      key_type_str = val;
      if (key_type_str.compare("swift") == 0) {
        key_type = KEY_TYPE_SWIFT;
      } else if (key_type_str.compare("s3") == 0) {
        key_type = KEY_TYPE_S3;
      } else {
        cerr << "bad key type: " << key_type_str << std::endl;
        return usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--job-id", (char*)NULL)) {
      job_id = val;
    } else if (ceph_argparse_binary_flag(args, i, &gen_access_key, NULL, "--gen-access-key", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &gen_secret_key, NULL, "--gen-secret", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &show_log_entries, NULL, "--show_log_entries", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &show_log_sum, NULL, "--show_log_sum", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &skip_zero_entries, NULL, "--skip_zero_entries", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &system, NULL, "--system", (char*)NULL)) {
      system_specified = true;
    } else if (ceph_argparse_witharg(args, i, &tmp, errs, "-a", "--auth-uid", (char*)NULL)) {
      if (!errs.str().empty()) {
	cerr << errs.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--min-rewrite-size", (char*)NULL)) {
      min_rewrite_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--max-rewrite-size", (char*)NULL)) {
      max_rewrite_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--min-rewrite-stripe-size", (char*)NULL)) {
      min_rewrite_stripe_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--max-buckets", (char*)NULL)) {
      max_buckets = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max buckets: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-entries", (char*)NULL)) {
      max_entries = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max entries: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-size", (char*)NULL)) {
      max_size = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max size: " << err << std::endl;
        return EINVAL;
      }
      have_max_size = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-objects", (char*)NULL)) {
      max_objects = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max objects: " << err << std::endl;
        return EINVAL;
      }
      have_max_objects = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--date", "--time", (char*)NULL)) {
      date = val;
      if (end_date.empty())
        end_date = date;
    } else if (ceph_argparse_witharg(args, i, &val, "--start-date", "--start-time", (char*)NULL)) {
      start_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--end-date", "--end-time", (char*)NULL)) {
      end_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--num-shards", (char*)NULL)) {
      num_shards = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse num shards: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-concurrent-ios", (char*)NULL)) {
      max_concurrent_ios = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max concurrent ios: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--orphan-stale-secs", (char*)NULL)) {
      orphan_stale_secs = (uint64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse orphan stale secs: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--shard-id", (char*)NULL)) {
      shard_id = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse shard id: " << err << std::endl;
        return EINVAL;
      }
      specified_shard_id = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--daemon-id", (char*)NULL)) {
      daemon_id = val;
      specified_daemon_id = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--access", (char*)NULL)) {
      access = val;
      perm_mask = rgw_str_to_perm(access.c_str());
      set_perm = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--temp-url-key", (char*)NULL)) {
      temp_url_keys[0] = val;
      set_temp_url_key = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--temp-url-key2", "--temp-url-key-2", (char*)NULL)) {
      temp_url_keys[1] = val;
      set_temp_url_key = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--bucket-id", (char*)NULL)) {
      bucket_id = val;
      if (bucket_id.empty()) {
        cerr << "bad bucket-id" << std::endl;
        return usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      format = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--categories", (char*)NULL)) {
      string cat_str = val;
      list<string> cat_list;
      list<string>::iterator iter;
      get_str_list(cat_str, cat_list);
      for (iter = cat_list.begin(); iter != cat_list.end(); ++iter) {
	categories[*iter] = true;
      }
    } else if (ceph_argparse_binary_flag(args, i, &delete_child_objects, NULL, "--purge-objects", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &pretty_format, NULL, "--pretty-format", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &purge_data, NULL, "--purge-data", (char*)NULL)) {
      delete_child_objects = purge_data;
    } else if (ceph_argparse_binary_flag(args, i, &purge_keys, NULL, "--purge-keys", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &yes_i_really_mean_it, NULL, "--yes-i-really-mean-it", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &fix, NULL, "--fix", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &remove_bad, NULL, "--remove-bad", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &check_head_obj_locator, NULL, "--check-head-obj-locator", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &check_objects, NULL, "--check-objects", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &sync_stats, NULL, "--sync-stats", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &include_all, NULL, "--include-all", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &reset_regions, NULL, "--reset-regions", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--caps", (char*)NULL)) {
      caps = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--infile", (char*)NULL)) {
      infile = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--metadata-key", (char*)NULL)) {
      metadata_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--marker", (char*)NULL)) {
      marker = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--start-marker", (char*)NULL)) {
      start_marker = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--end-marker", (char*)NULL)) {
      end_marker = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--quota-scope", (char*)NULL)) {
      quota_scope = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--replica-log-type", (char*)NULL)) {
      replica_log_type_str = val;
      replica_log_type = get_replicalog_type(replica_log_type_str);
      if (replica_log_type == ReplicaLog_Invalid) {
        cerr << "ERROR: invalid replica log type" << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--index-type", (char*)NULL)) {
      string index_type_str = val;
      bi_index_type = get_bi_index_type(index_type_str);
      if (bi_index_type == InvalidIdx) {
        cerr << "ERROR: invalid bucket index entry type" << std::endl;
        return EINVAL;
      }
    } else if (strncmp(*i, "-", 1) == 0) {
      cerr << "ERROR: invalid flag " << *i << std::endl;
      return EINVAL;
    } else {
      ++i;
    }
  }
  if (tenant.empty()) {
    tenant = user_id.tenant;
  } else {
    if (user_id.empty()) {
      cerr << "ERROR: --tennant is set, but there's no user ID" << std::endl;
      return EINVAL;
    }
    user_id.tenant = tenant;
  }

  if (args.empty()) {
    return usage();
  }
  else {
    const char *prev_cmd = NULL;
    std::vector<const char*>::iterator i ;
    for (i = args.begin(); i != args.end(); ++i) {
      opt_cmd = get_cmd(*i, prev_cmd, &need_more);
      if (opt_cmd < 0) {
	cerr << "unrecognized arg " << *i << std::endl;
	return usage();
      }
      if (!need_more) {
	++i;
	break;
      }
      prev_cmd = *i;
    }

    if (opt_cmd == OPT_NO_CMD)
      return usage();

    /* some commands may have an optional extra param */
    if (i != args.end()) {
      switch (opt_cmd) {
        case OPT_METADATA_GET:
        case OPT_METADATA_PUT:
        case OPT_METADATA_RM:
        case OPT_METADATA_LIST:
          metadata_key = *i;
          break;
        default:
          break;
      }
    }

    /* check key parameter conflict */
    if ((!access_key.empty()) && gen_access_key) {
        cerr << "ERROR: key parameter conflict, --access-key & --gen-access-key" << std::endl;
        return -EINVAL;
    }
    if ((!secret_key.empty()) && gen_secret_key) {
        cerr << "ERROR: key parameter conflict, --secret & --gen-secret" << std::endl;
        return -EINVAL;
    }
  }

  // default to pretty json
  if (format.empty()) {
    format = "json";
    pretty_format = true;
  }

  if (format ==  "xml")
    formatter = new XMLFormatter(pretty_format);
  else if (format == "json")
    formatter = new JSONFormatter(pretty_format);
  else {
    cerr << "unrecognized format: " << format << std::endl;
    return usage();
  }

  RGWStreamFlusher f(formatter, cout);

  bool raw_storage_op = (opt_cmd == OPT_REGION_GET || opt_cmd == OPT_REGION_LIST ||
                         opt_cmd == OPT_REGION_SET || opt_cmd == OPT_REGION_DEFAULT ||
                         opt_cmd == OPT_REGIONMAP_GET || opt_cmd == OPT_REGIONMAP_SET ||
                         opt_cmd == OPT_REGIONMAP_UPDATE ||
                         opt_cmd == OPT_ZONE_GET || opt_cmd == OPT_ZONE_SET ||
                         opt_cmd == OPT_ZONE_LIST);


  if (raw_storage_op) {
    store = RGWStoreManager::get_raw_storage(g_ceph_context);
  } else {
    store = RGWStoreManager::get_storage(g_ceph_context, false, false);
  }
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);

  StoreDestructor store_destructor(store);

  if (raw_storage_op) {
    if (opt_cmd == OPT_REGION_GET) {
      RGWRegion region;
      int ret = region.init(g_ceph_context, store);
      if (ret < 0) {
        cerr << "failed to init region: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      encode_json("region", region, formatter);
      formatter->flush(cout);
      cout << std::endl;
    }
    if (opt_cmd == OPT_REGION_LIST) {
      RGWRegion region;
      int ret = region.init(g_ceph_context, store, false);
      if (ret < 0) {
	cerr << "failed to init region: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      list<string> regions;
      ret = store->list_regions(regions);
      if (ret < 0) {
        cerr << "failed to list regions: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
      RGWDefaultRegionInfo default_region;
      ret = region.read_default(default_region);
      if (ret < 0 && ret != -ENOENT) {
	cerr << "could not determine default region: " << cpp_strerror(-ret) << std::endl;
      }
      formatter->open_object_section("regions_list");
      encode_json("default_info", default_region, formatter);
      encode_json("regions", regions, formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
    if (opt_cmd == OPT_REGION_SET) {
      RGWRegion region;
      int ret = region.init(g_ceph_context, store, false);
      if (ret < 0) {
	cerr << "failed to init region: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
      ret = read_decode_json(infile, region);
      if (ret < 0) {
        return 1;
      }

      ret = region.store_info(false);
      if (ret < 0) {
        cerr << "ERROR: couldn't store zone info: " << cpp_strerror(-ret) << std::endl;
        return 1;
      }

      encode_json("region", region, formatter);
      formatter->flush(cout);
    }
    if (opt_cmd == OPT_REGION_DEFAULT) {
      RGWRegion region;
      int ret = region.init(g_ceph_context, store);
      if (ret < 0) {
	cerr << "failed to init region: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      ret = region.set_as_default();
      if (ret < 0) {
	cerr << "failed to set region as default: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }

    if (opt_cmd == OPT_REGIONMAP_GET) {
      RGWRegionMap regionmap;
      int ret = regionmap.read(g_ceph_context, store);
      if (ret < 0) {
	cerr << "failed to read region map: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
      encode_json("region-map", regionmap, formatter);
      formatter->flush(cout);
    }

    if (opt_cmd == OPT_REGIONMAP_SET) {
      RGWRegionMap regionmap;
      int ret = read_decode_json(infile, regionmap);
      if (ret < 0) {
        return 1;
      }

      ret = regionmap.store(g_ceph_context, store);
      if (ret < 0) {
        cerr << "ERROR: couldn't store region map info: " << cpp_strerror(-ret) << std::endl;
        return 1;
      }

      encode_json("region-map", regionmap, formatter);
      formatter->flush(cout);
    }

    if (opt_cmd == OPT_REGIONMAP_UPDATE) {
      RGWRegionMap regionmap;
      int ret = regionmap.read(g_ceph_context, store);
      if (ret < 0 && ret != -ENOENT) {
	cerr << "failed to read region map: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      RGWRegion region;
      ret = region.init(g_ceph_context, store, false);
      if (ret < 0) {
	cerr << "failed to init region: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      list<string> regions;
      ret = store->list_regions(regions);
      if (ret < 0) {
        cerr << "failed to list regions: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      if (reset_regions) {
        regionmap.regions.clear();
      }

      for (list<string>::iterator iter = regions.begin(); iter != regions.end(); ++iter) {
        ret = region.read_info(*iter);
        if (ret < 0) {
        cerr << "failed to read region info (name=" << *iter << "): " << cpp_strerror(-ret) << std::endl;
	return -ret;
        }
        regionmap.update(region);
      }

      ret = regionmap.store(g_ceph_context, store);
      if (ret < 0) {
        cerr << "ERROR: couldn't store region map info: " << cpp_strerror(-ret) << std::endl;
        return 1;
      }

      encode_json("region-map", regionmap, formatter);
      formatter->flush(cout);
    }

    if (opt_cmd == OPT_ZONE_GET) {
      RGWRegion region;
      int ret = region.init(g_ceph_context, store);
      if (ret < 0) {
        cerr << "WARNING: failed to initialize region" << std::endl;
      }
      RGWZoneParams zone;
      ret = zone.init(g_ceph_context, store, region);
      if (ret < 0) {
	cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
      encode_json("zone", zone, formatter);
      formatter->flush(cout);
    }

    if (opt_cmd == OPT_ZONE_SET) {
      RGWRegion region;
      int ret = region.init(g_ceph_context, store);
      if (ret < 0) {
        cerr << "WARNING: failed to initialize region" << std::endl;
      }
      RGWZoneParams zone;
      zone.init_default(store);
      ret = read_decode_json(infile, zone);
      if (ret < 0) {
        return 1;
      }

      ret = zone.store_info(g_ceph_context, store, region);
      if (ret < 0) {
        cerr << "ERROR: couldn't store zone info: " << cpp_strerror(-ret) << std::endl;
        return 1;
      }

      encode_json("zone", zone, formatter);
      formatter->flush(cout);
    }
    if (opt_cmd == OPT_ZONE_LIST) {
      list<string> zones;
      int ret = store->list_zones(zones);
      if (ret < 0) {
        cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
      formatter->open_object_section("zones_list");
      encode_json("zones", zones, formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
    return 0;
  }

  if (!user_id.empty()) {
    user_op.set_user_id(user_id);
    bucket_op.set_user_id(user_id);
  }

  if (!display_name.empty())
    user_op.set_display_name(display_name);

  if (!user_email.empty())
    user_op.set_user_email(user_email);

  if (!access_key.empty())
    user_op.set_access_key(access_key);

  if (!secret_key.empty())
    user_op.set_secret_key(secret_key);

  if (!subuser.empty())
    user_op.set_subuser(subuser);

  if (!caps.empty())
    user_op.set_caps(caps);

  user_op.set_purge_data(purge_data);

  if (purge_keys)
    user_op.set_purge_keys();

  if (gen_access_key)
    user_op.set_generate_key();

  if (gen_secret_key)
    user_op.set_gen_secret(); // assume that a key pair should be created

  if (max_buckets >= 0)
    user_op.set_max_buckets(max_buckets);

  if (system_specified)
    user_op.set_system(system);

  if (set_perm)
    user_op.set_perm(perm_mask);

  if (set_temp_url_key) {
    map<int, string>::iterator iter = temp_url_keys.begin();
    for (; iter != temp_url_keys.end(); ++iter) {
      user_op.set_temp_url_key(iter->second, iter->first);
    }
  }

  if (!op_mask_str.empty()) {
    uint32_t op_mask;
    int ret = rgw_parse_op_type_list(op_mask_str, &op_mask);
    if (ret < 0) {
      cerr << "failed to parse op_mask: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    user_op.set_op_mask(op_mask);
  }

  if (key_type != KEY_TYPE_UNDEFINED)
    user_op.set_key_type(key_type);

  // set suspension operation parameters
  if (opt_cmd == OPT_USER_ENABLE)
    user_op.set_suspension(false);
  else if (opt_cmd == OPT_USER_SUSPEND)
    user_op.set_suspension(true);

  // RGWUser to use for user operations
  RGWUser user;
  int ret = 0;
  if (!user_id.empty() || !subuser.empty()) {
    ret = user.init(store, user_op);
    if (ret < 0) {
      cerr << "user.init failed: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  /* populate bucket operation */
  bucket_op.set_bucket_name(bucket_name);
  bucket_op.set_object(object);
  bucket_op.set_check_objects(check_objects);
  bucket_op.set_delete_children(delete_child_objects);

  // required to gather errors from operations
  std::string err_msg;

  bool output_user_info = true;

  switch (opt_cmd) {
  case OPT_USER_INFO:
    break;
  case OPT_USER_CREATE:
    if (!user_op.has_existing_user()) {
      user_op.set_generate_key(); // generate a new key by default
    }
    ret = user.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      return -ret;
    }
    if (!subuser.empty()) {
      ret = user.subusers.add(user_op, &err_msg);
      if (ret < 0) {
        cerr << "could not create subuser: " << err_msg << std::endl;
        return -ret;
      }
    }
    break;
  case OPT_USER_RM:
    ret = user.remove(user_op, &err_msg);
    if (ret == -ENOENT) {
      cerr << err_msg << std::endl;
    } else if (ret < 0) {
      cerr << "could not remove user: " << err_msg << std::endl;
      return -ret;
    }

    output_user_info = false;
    break;
  case OPT_USER_ENABLE:
  case OPT_USER_SUSPEND:
  case OPT_USER_MODIFY:
    ret = user.modify(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not modify user: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_SUBUSER_CREATE:
    ret = user.subusers.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_SUBUSER_MODIFY:
    ret = user.subusers.modify(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not modify subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_SUBUSER_RM:
    ret = user.subusers.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not remove subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_CAPS_ADD:
    ret = user.caps.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not add caps: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_CAPS_RM:
    ret = user.caps.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not add remove caps: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_KEY_CREATE:
    ret = user.keys.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create key: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT_KEY_RM:
    ret = user.keys.remove(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not remove key: " << err_msg << std::endl;
      return -ret;
    }

    break;
  default:
    output_user_info = false;
  }

  // output the result of a user operation
  if (output_user_info) {
    ret = user.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }
    show_user_info(info, formatter);
  }

  if (opt_cmd == OPT_POLICY) {
    int ret = RGWBucketAdminOp::get_policy(store, bucket_op, cout);
    if (ret >= 0) {
      cout << std::endl;
    }
  }

  if (opt_cmd == OPT_BUCKETS_LIST) {
    if (bucket_name.empty()) {
      RGWBucketAdminOp::info(store, bucket_op, f);
    } else {
      RGWBucketInfo bucket_info;
      int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      formatter->open_array_section("entries");
      bool truncated;
      int count = 0;
      if (max_entries < 0)
        max_entries = 1000;

      string prefix;
      string delim;
      vector<RGWObjEnt> result;
      map<string, bool> common_prefixes;
      string ns;

      RGWRados::Bucket target(store, bucket);
      RGWRados::Bucket::List list_op(&target);

      list_op.params.prefix = prefix;
      list_op.params.delim = delim;
      list_op.params.marker = rgw_obj_key(marker);
      list_op.params.ns = ns;
      list_op.params.enforce_ns = false;
      list_op.params.list_versions = true;
      
      do {
        ret = list_op.list_objects(max_entries - count, &result, &common_prefixes, &truncated);
        if (ret < 0) {
          cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        count += result.size();

        for (vector<RGWObjEnt>::iterator iter = result.begin(); iter != result.end(); ++iter) {
          RGWObjEnt& entry = *iter;
          encode_json("entry", entry, formatter);
        }
        formatter->flush(cout);
      } while (truncated && count < max_entries);

      formatter->close_section();
      formatter->flush(cout);
    }
  }

  if (opt_cmd == OPT_BUCKET_STATS) {
    bucket_op.set_fetch_stats(true);

    RGWBucketAdminOp::info(store, bucket_op, f);
  }

  if (opt_cmd == OPT_BUCKET_LINK) {
    bucket_op.set_bucket_id(bucket_id);
    string err;
    int r = RGWBucketAdminOp::link(store, bucket_op, &err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    int r = RGWBucketAdminOp::unlink(store, bucket_op);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT_LOG_LIST) {
    // filter by date?
    if (date.size() && date.size() != 10) {
      cerr << "bad date format for '" << date << "', expect YYYY-MM-DD" << std::endl;
      return -EINVAL;
    }

    formatter->reset();
    formatter->open_array_section("logs");
    RGWAccessHandle h;
    int r = store->log_list_init(date, &h);
    if (r == -ENOENT) {
      // no logs.
    } else {
      if (r < 0) {
	cerr << "log list: error " << r << std::endl;
	return r;
      }
      while (true) {
	string name;
	int r = store->log_list_next(h, &name);
	if (r == -ENOENT)
	  break;
	if (r < 0) {
	  cerr << "log list: error " << r << std::endl;
	  return r;
	}
	formatter->dump_string("object", name);
      }
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_LOG_SHOW || opt_cmd == OPT_LOG_RM) {
    if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id.empty())) {
      cerr << "specify an object or a date, bucket and bucket-id" << std::endl;
      return usage();
    }

    string oid;
    if (!object.empty()) {
      oid = object;
    } else {
      oid = date;
      oid += "-";
      oid += bucket_id;
      oid += "-";
      oid += bucket_name;
    }

    if (opt_cmd == OPT_LOG_SHOW) {
      RGWAccessHandle h;

      int r = store->log_show_init(oid, &h);
      if (r < 0) {
	cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }

      formatter->reset();
      formatter->open_object_section("log");

      struct rgw_log_entry entry;
      
      // peek at first entry to get bucket metadata
      r = store->log_show_next(h, &entry);
      if (r < 0) {
	cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      formatter->dump_string("bucket_id", entry.bucket_id);
      formatter->dump_string("bucket_owner", entry.bucket_owner.to_str());
      formatter->dump_string("bucket", entry.bucket);

      uint64_t agg_time = 0;
      uint64_t agg_bytes_sent = 0;
      uint64_t agg_bytes_received = 0;
      uint64_t total_entries = 0;

      if (show_log_entries)
        formatter->open_array_section("log_entries");

      do {
	uint64_t total_time =  entry.total_time.sec() * 1000000LL * entry.total_time.usec();

        agg_time += total_time;
        agg_bytes_sent += entry.bytes_sent;
        agg_bytes_received += entry.bytes_received;
        total_entries++;

        if (skip_zero_entries && entry.bytes_sent == 0 &&
            entry.bytes_received == 0)
          goto next;

        if (show_log_entries) {

	  rgw_format_ops_log_entry(entry, formatter);
	  formatter->flush(cout);
        }
next:
	r = store->log_show_next(h, &entry);
      } while (r > 0);

      if (r < 0) {
      	cerr << "error reading log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      if (show_log_entries)
        formatter->close_section();

      if (show_log_sum) {
        formatter->open_object_section("log_sum");
	formatter->dump_int("bytes_sent", agg_bytes_sent);
	formatter->dump_int("bytes_received", agg_bytes_received);
	formatter->dump_int("total_time", agg_time);
	formatter->dump_int("total_entries", total_entries);
        formatter->close_section();
      }
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
    if (opt_cmd == OPT_LOG_RM) {
      int r = store->log_remove(oid);
      if (r < 0) {
	cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
    }
  }
  
  if (opt_cmd == OPT_POOL_ADD) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to add!" << std::endl;
      return usage();
    }

    int ret = store->add_bucket_placement(pool_name);
    if (ret < 0)
      cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOL_RM) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to remove!" << std::endl;
      return usage();
    }

    int ret = store->remove_bucket_placement(pool_name);
    if (ret < 0)
      cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOLS_LIST) {
    set<string> pools;
    int ret = store->list_placement_set(pools);
    if (ret < 0) {
      cerr << "could not list placement set: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    formatter->reset();
    formatter->open_array_section("pools");
    set<string>::iterator siter;
    for (siter = pools.begin(); siter != pools.end(); ++siter) {
      formatter->open_object_section("pool");
      formatter->dump_string("name",  *siter);
      formatter->close_section();
    }
    formatter->close_section();
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_USAGE_SHOW) {
    uint64_t start_epoch = 0;
    uint64_t end_epoch = (uint64_t)-1;

    int ret;
    
    if (!start_date.empty()) {
      ret = utime_t::parse_date(start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }
    if (!end_date.empty()) {
      ret = utime_t::parse_date(end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }


    ret = RGWUsage::show(store, user_id, start_epoch, end_epoch,
			 show_log_entries, show_log_sum, &categories,
			 f);
    if (ret < 0) {
      cerr << "ERROR: failed to show usage" << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_USAGE_TRIM) {
    if (user_id.empty() && !yes_i_really_mean_it) {
      cerr << "usage trim without user specified will remove *all* users data" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }
    int ret;
    uint64_t start_epoch = 0;
    uint64_t end_epoch = (uint64_t)-1;


    if (!start_date.empty()) {
      ret = utime_t::parse_date(start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }

    if (!end_date.empty()) {
      ret = utime_t::parse_date(end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return 1;
      }
    }

    ret = RGWUsage::trim(store, user_id, start_epoch, end_epoch);
    if (ret < 0) {
      cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
      return 1;
    }   
  }

  if (opt_cmd == OPT_OLH_GET || opt_cmd == OPT_OLH_READLOG) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_OLH_GET) {
    RGWOLHInfo olh;
    rgw_obj obj(bucket, object);
    int ret = store->get_olh(obj, &olh);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("olh", olh, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_OLH_READLOG) {
    map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
    bool is_truncated;

    RGWObjectCtx rctx(store);
    rgw_obj obj(bucket, object);

    RGWObjState *state;

    int ret = store->get_obj_state(&rctx, obj, &state, NULL, false); /* don't follow olh */
    if (ret < 0) {
      return ret;
    }

    ret = store->bucket_index_read_olh_log(*state, obj, 0, &log, &is_truncated);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("is_truncated", is_truncated, formatter);
    encode_json("log", log, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_GET) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket, object);
    if (!object_version.empty()) {
      obj.set_instance(object_version);
    }

    rgw_cls_bi_entry entry;

    ret = store->bi_get(bucket, obj, bi_index_type, &entry);
    if (ret < 0) {
      cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("entry", entry, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_PUT) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_cls_bi_entry entry;
    cls_rgw_obj_key key;
    ret = read_decode_json(infile, entry, &key);
    if (ret < 0) {
      return 1;
    }

    rgw_obj obj(bucket, key.name);
    obj.set_instance(key.instance);

    ret = store->bi_put(bucket, obj, entry);
    if (ret < 0) {
      cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BI_LIST) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    if (max_entries < 0) {
      max_entries = 1000;
    }


    formatter->open_array_section("entries");

    do {
      entries.clear();
      ret = store->bi_list(bucket, object, marker, max_entries, &entries, &is_truncated);
      if (ret < 0) {
        cerr << "ERROR: bi_list(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      list<rgw_cls_bi_entry>::iterator iter;
      for (iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_cls_bi_entry& entry = *iter;
        encode_json("entry", entry, formatter);
        marker = entry.idx;
      }
      formatter->flush(cout);
    } while (is_truncated);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_OBJECT_RM) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj_key key(object, object_version);
    ret = rgw_remove_object(store, bucket_info, bucket, key);

    if (ret < 0) {
      cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }

    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_obj obj(bucket, object);
    obj.set_instance(object_version);
    bool need_rewrite = true;
    if (min_rewrite_stripe_size > 0) {
      ret = check_min_obj_stripe_size(store, bucket_info, obj, min_rewrite_stripe_size, &need_rewrite);
      if (ret < 0) {
        ldout(store->ctx(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << ret << dendl;
      }
    }
    if (need_rewrite) {
      ret = store->rewrite_obj(bucket_info, obj);
      if (ret < 0) {
        cerr << "ERROR: object rewrite returned: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ldout(store->ctx(), 20) << "skipped object" << dendl;
    }
  }

  if (opt_cmd == OPT_OBJECTS_EXPIRE) {
    int ret = store->process_expire_objects();
    if (ret < 0) {
      cerr << "ERROR: process_expire_objects() processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_BUCKET_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    uint64_t start_epoch = 0;
    uint64_t end_epoch = 0;

    if (!end_date.empty()) {
      int ret = utime_t::parse_date(end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return EINVAL;
      }
    }
    if (!start_date.empty()) {
      int ret = utime_t::parse_date(start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return EINVAL;
      }
    }

    bool is_truncated = true;

    rgw_obj_key marker;
    string prefix;

    formatter->open_object_section("result");
    formatter->dump_string("bucket", bucket_name);
    formatter->open_array_section("objects");
    while (is_truncated) {
      map<string, RGWObjEnt> result;
      int r = store->cls_bucket_list(bucket, marker, prefix, 1000, true,
                                     result, &is_truncated, &marker,
                                     bucket_object_check_filter);

      if (r < 0 && r != -ENOENT) {
        cerr << "ERROR: failed operation r=" << r << std::endl;
      }

      if (r == -ENOENT)
        break;

      map<string, RGWObjEnt>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
        rgw_obj_key key = iter->second.key;
        RGWObjEnt& entry = iter->second;

        formatter->open_object_section("object");
        formatter->dump_string("name", key.name);
        formatter->dump_string("instance", key.instance);
        formatter->dump_int("size", entry.size);
        utime_t ut(entry.mtime, 0);
        ut.gmtime(formatter->dump_stream("mtime"));

        if ((entry.size < min_rewrite_size) ||
            (entry.size > max_rewrite_size) ||
            (start_epoch > 0 && start_epoch > (uint64_t)ut.sec()) ||
            (end_epoch > 0 && end_epoch < (uint64_t)ut.sec())) {
          formatter->dump_string("status", "Skipped");
        } else {
          rgw_obj obj(bucket, key.name);
          obj.set_instance(key.instance);

          bool need_rewrite = true;
          if (min_rewrite_stripe_size > 0) {
            r = check_min_obj_stripe_size(store, bucket_info, obj, min_rewrite_stripe_size, &need_rewrite);
            if (r < 0) {
              ldout(store->ctx(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
            }
          }
          if (!need_rewrite) {
            formatter->dump_string("status", "Skipped");
          } else {
            r = store->rewrite_obj(bucket_info, obj);
            if (r == 0) {
              formatter->dump_string("status", "Success");
            } else {
              formatter->dump_string("status", cpp_strerror(-r));
            }
          }
        }
        formatter->dump_int("flags", entry.flags);

        formatter->close_section();
        formatter->flush(cout);
      }
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_OBJECT_UNLINK) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<rgw_obj_key> oid_list;
    rgw_obj_key key(object, object_version);
    oid_list.push_back(key);
    ret = store->remove_objs_from_index(bucket, oid_list);
    if (ret < 0) {
      cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_OBJECT_STAT) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket, object);
    obj.set_instance(object_version);

    uint64_t obj_size;
    map<string, bufferlist> attrs;
    RGWObjectCtx obj_ctx(store);
    RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
    RGWRados::Object::Read read_op(&op_target);

    read_op.params.attrs = &attrs;
    read_op.params.obj_size = &obj_size;

    ret = read_op.prepare(NULL, NULL);
    if (ret < 0) {
      cerr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
    formatter->open_object_section("object_metadata");
    formatter->dump_string("name", object);
    formatter->dump_unsigned("size", obj_size);

    map<string, bufferlist>::iterator iter;
    map<string, bufferlist> other_attrs;
    for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
      bufferlist& bl = iter->second;
      bool handled = false;
      if (iter->first == RGW_ATTR_MANIFEST) {
        handled = decode_dump<RGWObjManifest>("manifest", bl, formatter);
      } else if (iter->first == RGW_ATTR_ACL) {
        handled = decode_dump<RGWAccessControlPolicy>("policy", bl, formatter);
      } else if (iter->first == RGW_ATTR_ID_TAG) {
        handled = dump_string("tag", bl, formatter);
      } else if (iter->first == RGW_ATTR_ETAG) {
        handled = dump_string("etag", bl, formatter);
      }

      if (!handled)
        other_attrs[iter->first] = bl;
    }

    formatter->open_object_section("attrs");
    for (iter = other_attrs.begin(); iter != other_attrs.end(); ++iter) {
      dump_string(iter->first.c_str(), iter->second, formatter);
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BUCKET_CHECK) {
    if (check_head_obj_locator) {
      if (bucket_name.empty()) {
        cerr << "ERROR: need to specify bucket name" << std::endl;
        return EINVAL;
      }
      do_check_object_locator(tenant, bucket_name, fix, remove_bad, formatter);
    } else {
      RGWBucketAdminOp::check_index(store, bucket_op, f);
    }
  }

  if (opt_cmd == OPT_BUCKET_RM) {
    RGWBucketAdminOp::remove_bucket(store, bucket_op);
  }

  if (opt_cmd == OPT_GC_LIST) {
    int index = 0;
    bool truncated;
    formatter->open_array_section("entries");

    do {
      list<cls_rgw_gc_obj_info> result;
      int ret = store->list_gc_objs(&index, marker, 1000, !include_all, result, &truncated);
      if (ret < 0) {
	cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
	return 1;
      }


      list<cls_rgw_gc_obj_info>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
	cls_rgw_gc_obj_info& info = *iter;
	formatter->open_object_section("chain_info");
	formatter->dump_string("tag", info.tag);
	formatter->dump_stream("time") << info.time;
	formatter->open_array_section("objs");
        list<cls_rgw_obj>::iterator liter;
	cls_rgw_obj_chain& chain = info.chain;
	for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
	  cls_rgw_obj& obj = *liter;
          encode_json("obj", obj, formatter);
	}
	formatter->close_section(); // objs
	formatter->close_section(); // obj_chain
	formatter->flush(cout);
      }
    } while (truncated);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_GC_PROCESS) {
    int ret = store->process_gc();
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_ORPHANS_FIND) {
    RGWOrphanSearch search(store, max_concurrent_ios, orphan_stale_secs);

    if (job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    if (pool_name.empty()) {
      cerr << "ERROR: --pool not specified" << std::endl;
      return EINVAL;
    }

    RGWOrphanSearchInfo info;

    info.pool = pool_name;
    info.job_name = job_id;
    info.num_shards = num_shards;

    int ret = search.init(job_id, &info);
    if (ret < 0) {
      cerr << "could not init search, ret=" << ret << std::endl;
      return -ret;
    }
    ret = search.run();
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT_ORPHANS_FINISH) {
    RGWOrphanSearch search(store, max_concurrent_ios, orphan_stale_secs);

    if (job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    int ret = search.init(job_id, NULL);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "job not found" << std::endl;
      }
      return -ret;
    }
    ret = search.finish();
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT_USER_CHECK) {
    check_bad_user_bucket_mapping(store, user_id, fix);
  }

  if (opt_cmd == OPT_USER_STATS) {
    if (sync_stats) {
      if (!bucket_name.empty()) {
        int ret = rgw_bucket_sync_user_stats(store, tenant, bucket_name);
        if (ret < 0) {
          cerr << "ERROR: could not sync bucket stats: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      } else {
        int ret = rgw_user_sync_all_stats(store, user_id);
        if (ret < 0) {
          cerr << "ERROR: failed to sync user stats: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      }
    }

    if (user_id.empty()) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    cls_user_header header;
    string user_str = user_id.to_str();
    int ret = store->cls_user_get_header(user_str, &header);
    if (ret < 0) {
      cerr << "ERROR: can't read user header: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("header", header, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_METADATA_GET) {
    int ret = store->meta_mgr->get(metadata_key, formatter);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    formatter->flush(cout);
  }

  if (opt_cmd == OPT_METADATA_PUT) {
    bufferlist bl;
    int ret = read_input(infile, bl);
    if (ret < 0) {
      cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    ret = store->meta_mgr->put(metadata_key, bl, RGWMetadataHandler::APPLY_ALWAYS);
    if (ret < 0) {
      cerr << "ERROR: can't put key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_METADATA_RM) {
    int ret = store->meta_mgr->remove(metadata_key);
    if (ret < 0) {
      cerr << "ERROR: can't remove key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_METADATA_LIST) {
    void *handle;
    int max = 1000;
    int ret = store->meta_mgr->list_keys_init(metadata_key, &handle);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool truncated;

    formatter->open_array_section("keys");

    do {
      list<string> keys;
      ret = store->meta_mgr->list_keys_next(handle, max, keys, &truncated);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      } if (ret != -ENOENT) {
	for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
	  formatter->dump_string("key", *iter);
	}
	formatter->flush(cout);
      }
    } while (truncated);

    formatter->close_section();
    formatter->flush(cout);

    store->meta_mgr->list_keys_complete(handle);
  }

  if (opt_cmd == OPT_MDLOG_LIST) {
    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    int i = (specified_shard_id ? shard_id : 0);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      RGWMetadataLog *meta_log = store->meta_mgr->get_log();
      void *handle;
      list<cls_log_entry> entries;


      meta_log->init_list_entries(i, start_time, end_time, marker, &handle);

      bool truncated;
      do {
	  int ret = meta_log->list_entries(handle, 1000, entries, NULL, &truncated);
        if (ret < 0) {
          cerr << "ERROR: meta_log->list_entries(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        for (list<cls_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
          cls_log_entry& entry = *iter;
          store->meta_mgr->dump_log_entry(entry, formatter);
        }
        formatter->flush(cout);
      } while (truncated);

      meta_log->complete_list_entries(handle);

      if (specified_shard_id)
        break;
    }
  

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_MDLOG_TRIM) {
    utime_t start_time, end_time;

    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for trim operation" << std::endl;
      return EINVAL;
    }

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    RGWMetadataLog *meta_log = store->meta_mgr->get_log();

    ret = meta_log->trim(shard_id, start_time, end_time, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: meta_log->trim(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }
  
  if (opt_cmd == OPT_BILOG_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return -EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    do {
      list<rgw_bi_log_entry> entries;
      ret = store->list_bi_log_entries(bucket, shard_id, marker, max_entries - count, entries, &truncated);
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (list<rgw_bi_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_bi_log_entry& entry = *iter;
        encode_json("entry", entry, formatter);

        marker = entry.id;
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BILOG_TRIM) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return -EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = store->trim_bi_log_entries(bucket, shard_id, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_DATALOG_LIST) {
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    RGWDataChangesLog *log = store->data_log;
    RGWDataChangesLog::LogMarker marker;

    do {
      list<rgw_data_change> entries;
      ret = log->list_entries(start_time, end_time, max_entries - count, entries, marker, &truncated);
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (list<rgw_data_change>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_data_change& entry = *iter;
        encode_json("entry", entry, formatter);
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }
  
  if (opt_cmd == OPT_DATALOG_TRIM) {
    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    RGWDataChangesLog *log = store->data_log;
    ret = log->trim_entries(start_time, end_time, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_OPSTATE_LIST) {
    RGWOpState oc(store);

    int max = 1000;

    void *handle;
    oc.init_list_entries(client_id, op_id, object, &handle);
    list<cls_statelog_entry> entries;
    bool done;
    formatter->open_array_section("entries");
    do {
      int ret = oc.list_entries(handle, max, entries, &done);
      if (ret < 0) {
        cerr << "oc.list_entries returned " << cpp_strerror(-ret) << std::endl;
        oc.finish_list_entries(handle);
        return -ret;
      }

      for (list<cls_statelog_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        oc.dump_entry(*iter, formatter);
      }

      formatter->flush(cout);
    } while (!done);
    formatter->close_section();
    formatter->flush(cout);
    oc.finish_list_entries(handle);
  }

  if (opt_cmd == OPT_OPSTATE_SET || opt_cmd == OPT_OPSTATE_RENEW) {
    RGWOpState oc(store);

    RGWOpState::OpState state;
    if (object.empty() || client_id.empty() || op_id.empty()) {
      cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
      return EINVAL;
    }
    if (state_str.empty()) {
      cerr << "ERROR: state was not specified" << std::endl;
      return EINVAL;
    }
    int ret = oc.state_from_str(state_str, &state);
    if (ret < 0) {
      cerr << "ERROR: invalid state: " << state_str << std::endl;
      return -ret;
    }

    if (opt_cmd == OPT_OPSTATE_SET) {
      ret = oc.set_state(client_id, op_id, object, state);
      if (ret < 0) {
        cerr << "ERROR: failed to set state: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ret = oc.renew_state(client_id, op_id, object, state);
      if (ret < 0) {
        cerr << "ERROR: failed to renew state: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }
  if (opt_cmd == OPT_OPSTATE_RM) {
    RGWOpState oc(store);

    if (object.empty() || client_id.empty() || op_id.empty()) {
      cerr << "ERROR: need to specify client_id, op_id, and object" << std::endl;
      return EINVAL;
    }
    ret = oc.remove_entry(client_id, op_id, object);
    if (ret < 0) {
      cerr << "ERROR: failed to set state: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_GET || opt_cmd == OPT_REPLICALOG_UPDATE ||
      opt_cmd == OPT_REPLICALOG_DELETE) {
    if (replica_log_type_str.empty()) {
      cerr << "ERROR: need to specify --replica-log-type=<metadata | data | bucket>" << std::endl;
      return EINVAL;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_GET) {
    RGWReplicaBounds bounds;
    if (replica_log_type == ReplicaLog_Metadata) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }

      RGWReplicaObjectLogger logger(store, pool_name, META_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.get_bounds(shard_id, bounds);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Data) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool_name, DATA_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.get_bounds(shard_id, bounds);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Bucket) {
      if (bucket_name.empty()) {
        cerr << "ERROR: bucket not specified" << std::endl;
        return -EINVAL;
      }
      RGWBucketInfo bucket_info;
      int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      RGWReplicaBucketLogger logger(store);
      ret = logger.get_bounds(bucket, shard_id, bounds);
      if (ret < 0)
        return -ret;
    } else { // shouldn't get here
      assert(0);
    }
    encode_json("bounds", bounds, formatter);
    formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT_REPLICALOG_DELETE) {
    if (replica_log_type == ReplicaLog_Metadata) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      if (!specified_daemon_id) {
        cerr << "ERROR: daemon-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool_name, META_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.delete_bound(shard_id, daemon_id, false);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Data) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      if (!specified_daemon_id) {
        cerr << "ERROR: daemon-id must be specified for delete operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool_name, DATA_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.delete_bound(shard_id, daemon_id, false);
      if (ret < 0)
        return -ret;
    } else if (replica_log_type == ReplicaLog_Bucket) {
      if (bucket_name.empty()) {
        cerr << "ERROR: bucket not specified" << std::endl;
        return -EINVAL;
      }
      RGWBucketInfo bucket_info;
      int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      RGWReplicaBucketLogger logger(store);
      ret = logger.delete_bound(bucket, shard_id, daemon_id, false);
      if (ret < 0)
        return -ret;
    }
  }

  if (opt_cmd == OPT_REPLICALOG_UPDATE) {
    if (marker.empty()) {
      cerr << "ERROR: marker was not specified" <<std::endl;
      return EINVAL;
    }
    utime_t time = ceph_clock_now(NULL);
    if (!date.empty()) {
      ret = parse_date_str(date, time);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return EINVAL;
      }
    }
    list<RGWReplicaItemMarker> entries;
    int ret = read_decode_json(infile, entries);
    if (ret < 0) {
      cerr << "ERROR: failed to decode entries" << std::endl;
      return EINVAL;
    }
    RGWReplicaBounds bounds;
    if (replica_log_type == ReplicaLog_Metadata) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }

      RGWReplicaObjectLogger logger(store, pool_name, META_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.update_bound(shard_id, daemon_id, marker, time, &entries);
      if (ret < 0) {
        cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else if (replica_log_type == ReplicaLog_Data) {
      if (!specified_shard_id) {
        cerr << "ERROR: shard-id must be specified for get operation" << std::endl;
        return EINVAL;
      }
      RGWReplicaObjectLogger logger(store, pool_name, DATA_REPLICA_LOG_OBJ_PREFIX);
      int ret = logger.update_bound(shard_id, daemon_id, marker, time, &entries);
      if (ret < 0) {
        cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else if (replica_log_type == ReplicaLog_Bucket) {
      if (bucket_name.empty()) {
        cerr << "ERROR: bucket not specified" << std::endl;
        return -EINVAL;
      }
      RGWBucketInfo bucket_info;
      int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      RGWReplicaBucketLogger logger(store);
      ret = logger.update_bound(bucket, shard_id, daemon_id, marker, time, &entries);
      if (ret < 0) {
        cerr << "ERROR: failed to update bounds: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  bool quota_op = (opt_cmd == OPT_QUOTA_SET || opt_cmd == OPT_QUOTA_ENABLE || opt_cmd == OPT_QUOTA_DISABLE);

  if (quota_op) {
    if (bucket_name.empty() && user_id.empty()) {
      cerr << "ERROR: bucket name or uid is required for quota operation" << std::endl;
      return EINVAL;
    }

    if (!bucket_name.empty()) {
      if (!quota_scope.empty() && quota_scope != "bucket") {
        cerr << "ERROR: invalid quota scope specification." << std::endl;
        return EINVAL;
      }
      set_bucket_quota(store, opt_cmd, tenant, bucket_name,
                       max_size, max_objects, have_max_size, have_max_objects);
    } else if (!user_id.empty()) {
      if (quota_scope == "bucket") {
        set_user_bucket_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else if (quota_scope == "user") {
        set_user_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else {
        cerr << "ERROR: invalid quota scope specification. Please specify either --quota-scope=bucket, or --quota-scope=user" << std::endl;
        return EINVAL;
      }
    }
  }

  return 0;
}
