// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

#include <boost/optional.hpp>

extern "C" {
#include <liboath/oath.h>
}

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"
#include "common/safe_io.h"

#include "include/util.h"

#include "cls/rgw/cls_rgw_client.h"

#include "global/global_init.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_otp.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_lc.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_orphan.h"
#include "rgw_sync.h"
#include "rgw_sync_log_trim.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"
#include "rgw_role.h"
#include "rgw_reshard.h"
#include "rgw_http_client_curl.h"
#include "rgw_zone.h"
#include "rgw_pubsub.h"
#include "rgw_sync_module_pubsub.h"

#include "services/svc_sync_modules.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

static RGWRados *store = NULL;

static const DoutPrefixProvider* dpp() {
  struct GlobalPrefix : public DoutPrefixProvider {
    CephContext *get_cct() const override { return store->ctx(); }
    unsigned get_subsys() const override { return dout_subsys; }
    std::ostream& gen_prefix(std::ostream& out) const override { return out; }
  };
  static GlobalPrefix global_dpp;
  return &global_dpp;
}

void usage()
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
  cout << "  user list                  list users\n";
  cout << "  caps add                   add user capabilities\n";
  cout << "  caps rm                    remove user capabilities\n";
  cout << "  subuser create             create a new subuser\n" ;
  cout << "  subuser modify             modify subuser\n";
  cout << "  subuser rm                 remove subuser\n";
  cout << "  key create                 create access key\n";
  cout << "  key rm                     remove access key\n";
  cout << "  bucket list                list buckets\n";
  cout << "  bucket limit check         show bucket sharding stats\n";
  cout << "  bucket link                link bucket to specified user\n";
  cout << "  bucket unlink              unlink bucket from specified user\n";
  cout << "  bucket stats               returns bucket statistics\n";
  cout << "  bucket rm                  remove bucket\n";
  cout << "  bucket check               check bucket index\n";
  cout << "  bucket reshard             reshard bucket\n";
  cout << "  bucket rewrite             rewrite all objects in the specified bucket\n";
  cout << "  bucket sync disable        disable bucket sync\n";
  cout << "  bucket sync enable         enable bucket sync\n";
  cout << "  bi get                     retrieve bucket index object entries\n";
  cout << "  bi put                     store bucket index object entries\n";
  cout << "  bi list                    list raw bucket index entries\n";
  cout << "  bi purge                   purge bucket index entries\n";
  cout << "  object rm                  remove object\n";
  cout << "  object put                 put object\n";
  cout << "  object stat                stat an object for its metadata\n";
  cout << "  object unlink              unlink object from bucket index\n";
  cout << "  object rewrite             rewrite the specified object\n";
  cout << "  objects expire             run expired objects cleanup\n";
  cout << "  period rm                  remove a period\n";
  cout << "  period get                 get period info\n";
  cout << "  period get-current         get current period info\n";
  cout << "  period pull                pull a period\n";
  cout << "  period push                push a period\n";
  cout << "  period list                list all periods\n";
  cout << "  period update              update the staging period\n";
  cout << "  period commit              commit the staging period\n";
  cout << "  quota set                  set quota params\n";
  cout << "  quota enable               enable quota\n";
  cout << "  quota disable              disable quota\n";
  cout << "  global quota get           view global quota params\n";
  cout << "  global quota set           set global quota params\n";
  cout << "  global quota enable        enable a global quota\n";
  cout << "  global quota disable       disable a global quota\n";
  cout << "  realm create               create a new realm\n";
  cout << "  realm rm                   remove a realm\n";
  cout << "  realm get                  show realm info\n";
  cout << "  realm get-default          get default realm name\n";
  cout << "  realm list                 list realms\n";
  cout << "  realm list-periods         list all realm periods\n";
  cout << "  realm rename               rename a realm\n";
  cout << "  realm set                  set realm info (requires infile)\n";
  cout << "  realm default              set realm as default\n";
  cout << "  realm pull                 pull a realm and its current period\n";
  cout << "  zonegroup add              add a zone to a zonegroup\n";
  cout << "  zonegroup create           create a new zone group info\n";
  cout << "  zonegroup default          set default zone group\n";
  cout << "  zonegroup rm               remove a zone group info\n";
  cout << "  zonegroup get              show zone group info\n";
  cout << "  zonegroup modify           modify an existing zonegroup\n";
  cout << "  zonegroup set              set zone group info (requires infile)\n";
  cout << "  zonegroup rm               remove a zone from a zonegroup\n";
  cout << "  zonegroup rename           rename a zone group\n";
  cout << "  zonegroup list             list all zone groups set on this cluster\n";
  cout << "  zonegroup placement list   list zonegroup's placement targets\n";
  cout << "  zonegroup placement add    add a placement target id to a zonegroup\n";
  cout << "  zonegroup placement modify modify a placement target of a specific zonegroup\n";
  cout << "  zonegroup placement rm     remove a placement target from a zonegroup\n";
  cout << "  zonegroup placement default  set a zonegroup's default placement target\n";
  cout << "  zone create                create a new zone\n";
  cout << "  zone rm                    remove a zone\n";
  cout << "  zone get                   show zone cluster params\n";
  cout << "  zone modify                modify an existing zone\n";
  cout << "  zone set                   set zone cluster params (requires infile)\n";
  cout << "  zone list                  list all zones set on this cluster\n";
  cout << "  zone rename                rename a zone\n";
  cout << "  zone placement list        list zone's placement targets\n";
  cout << "  zone placement add         add a zone placement target\n";
  cout << "  zone placement modify      modify a zone placement target\n";
  cout << "  zone placement rm          remove a zone placement target\n";
  cout << "  metadata sync status       get metadata sync status\n";
  cout << "  metadata sync init         init metadata sync\n";
  cout << "  metadata sync run          run metadata sync\n";
  cout << "  data sync status           get data sync status of the specified source zone\n";
  cout << "  data sync init             init data sync for the specified source zone\n";
  cout << "  data sync run              run data sync for the specified source zone\n";
  cout << "  pool add                   add an existing pool for data placement\n";
  cout << "  pool rm                    remove an existing pool from data placement set\n";
  cout << "  pools list                 list placement active set\n";
  cout << "  policy                     read bucket/object policy\n";
  cout << "  log list                   list log objects\n";
  cout << "  log show                   dump a log from specific object or (bucket + date\n";
  cout << "                             + bucket-id)\n";
  cout << "                             (NOTE: required to specify formatting of date\n";
  cout << "                             to \"YYYY-MM-DD-hh\")\n";
  cout << "  log rm                     remove log object\n";
  cout << "  usage show                 show usage (by user, by bucket, date range)\n";
  cout << "  usage trim                 trim usage (by user, by bucket, date range)\n";
  cout << "  usage clear                reset all the usage stats for the cluster\n";
  cout << "  gc list                    dump expired garbage collection objects (specify\n";
  cout << "                             --include-all to list all entries, including unexpired)\n";
  cout << "  gc process                 manually process garbage (specify\n";
  cout << "                             --include-all to process all entries, including unexpired)\n";
  cout << "  lc list                    list all bucket lifecycle progress\n";
  cout << "  lc get                     get a lifecycle bucket configuration\n";
  cout << "  lc process                 manually process lifecycle\n";
  cout << "  metadata get               get metadata info\n";
  cout << "  metadata put               put metadata info\n";
  cout << "  metadata rm                remove metadata info\n";
  cout << "  metadata list              list metadata info\n";
  cout << "  mdlog list                 list metadata log\n";
  cout << "  mdlog trim                 trim metadata log (use start-date, end-date or\n";
  cout << "                             start-marker, end-marker)\n";
  cout << "  mdlog status               read metadata log status\n";
  cout << "  bilog list                 list bucket index log\n";
  cout << "  bilog trim                 trim bucket index log (use start-marker, end-marker)\n";
  cout << "  datalog list               list data log\n";
  cout << "  datalog trim               trim data log\n";
  cout << "  datalog status             read data log status\n";
  cout << "  orphans find               init and run search for leaked rados objects (use job-id, pool)\n";
  cout << "  orphans finish             clean up search for leaked rados objects\n";
  cout << "  orphans list-jobs          list the current job-ids for orphans search\n";
  cout << "  role create                create a AWS role for use with STS\n";
  cout << "  role rm                    remove a role\n";
  cout << "  role get                   get a role\n";
  cout << "  role list                  list roles with specified path prefix\n";
  cout << "  role modify                modify the assume role policy of an existing role\n";
  cout << "  role-policy put            add/update permission policy to role\n";
  cout << "  role-policy list           list policies attached to a role\n";
  cout << "  role-policy get            get the specified inline policy document embedded with the given role\n";
  cout << "  role-policy rm             remove policy attached to a role\n";
  cout << "  reshard add                schedule a resharding of a bucket\n";
  cout << "  reshard list               list all bucket resharding or scheduled to be resharded\n";
  cout << "  reshard status             read bucket resharding status\n";
  cout << "  reshard process            process of scheduled reshard jobs\n";
  cout << "  reshard cancel             cancel resharding a bucket\n";
  cout << "  reshard stale-instances list list stale-instances from bucket resharding\n";
  cout << "  reshard stale-instances rm   cleanup stale-instances from bucket resharding\n";
  cout << "  sync error list            list sync error\n";
  cout << "  sync error trim            trim sync error\n";
  cout << "  mfa create                 create a new MFA TOTP token\n";
  cout << "  mfa list                   list MFA TOTP tokens\n";
  cout << "  mfa get                    show MFA TOTP token\n";
  cout << "  mfa remove                 delete MFA TOTP token\n";
  cout << "  mfa check                  check MFA TOTP token\n";
  cout << "  mfa resync                 re-sync MFA TOTP token\n";
  cout << "options:\n";
  cout << "   --tenant=<tenant>         tenant name\n";
  cout << "   --uid=<id>                user id\n";
  cout << "   --subuser=<name>          subuser name\n";
  cout << "   --access-key=<key>        S3 access key\n";
  cout << "   --email=<email>           user's email address\n";
  cout << "   --secret/--secret-key=<key>\n";
  cout << "                             specify secret key\n";
  cout << "   --gen-access-key          generate random access key (for S3)\n";
  cout << "   --gen-secret              generate random secret key\n";
  cout << "   --key-type=<type>         key type, options are: swift, s3\n";
  cout << "   --temp-url-key[-2]=<key>  temp url key\n";
  cout << "   --access=<access>         Set access permissions for sub-user, should be one\n";
  cout << "                             of read, write, readwrite, full\n";
  cout << "   --display-name=<name>     user's display name\n";
  cout << "   --max-buckets             max number of buckets for a user\n";
  cout << "   --admin                   set the admin flag on the user\n";
  cout << "   --system                  set the system flag on the user\n";
  cout << "   --op-mask                 set the op mask on the user\n";
  cout << "   --bucket=<bucket>         Specify the bucket name. Also used by the quota command.\n";
  cout << "   --pool=<pool>             Specify the pool name. Also used to scan for leaked rados objects.\n";
  cout << "   --object=<object>         object name\n";
  cout << "   --date=<date>             date in the format yyyy-mm-dd\n";
  cout << "   --start-date=<date>       start date in the format yyyy-mm-dd\n";
  cout << "   --end-date=<date>         end date in the format yyyy-mm-dd\n";
  cout << "   --bucket-id=<bucket-id>   bucket id\n";
  cout << "   --shard-id=<shard-id>     optional for: \n";
  cout << "                               mdlog list\n";
  cout << "                               data sync status\n";
  cout << "                             required for: \n";
  cout << "                               mdlog trim\n";
  cout << "   --max-entries=<entries>   max entries for listing operations\n";
  cout << "   --metadata-key=<key>      key to retrieve metadata from with metadata get\n";
  cout << "   --remote=<remote>         zone or zonegroup id of remote gateway\n";
  cout << "   --period=<id>             period id\n";
  cout << "   --url=<url>               url for pushing/pulling period/realm\n";
  cout << "   --epoch=<number>          period epoch\n";
  cout << "   --commit                  commit the period during 'period update'\n";
  cout << "   --staging                 get staging period info\n";
  cout << "   --master                  set as master\n";
  cout << "   --master-zone=<id>        master zone id\n";
  cout << "   --rgw-realm=<name>        realm name\n";
  cout << "   --realm-id=<id>           realm id\n";
  cout << "   --realm-new-name=<name>   realm new name\n";
  cout << "   --rgw-zonegroup=<name>    zonegroup name\n";
  cout << "   --zonegroup-id=<id>       zonegroup id\n";
  cout << "   --zonegroup-new-name=<name>\n";
  cout << "                             zonegroup new name\n";
  cout << "   --rgw-zone=<name>         name of zone in which radosgw is running\n";
  cout << "   --zone-id=<id>            zone id\n";
  cout << "   --zone-new-name=<name>    zone new name\n";
  cout << "   --source-zone             specify the source zone (for data sync)\n";
  cout << "   --default                 set entity (realm, zonegroup, zone) as default\n";
  cout << "   --read-only               set zone as read-only (when adding to zonegroup)\n";
  cout << "   --redirect-zone           specify zone id to redirect when response is 404 (not found)\n";
  cout << "   --placement-id            placement id for zonegroup placement commands\n";
  cout << "   --storage-class           storage class for zonegroup placement commands\n";
  cout << "   --tags=<list>             list of tags for zonegroup placement add and modify commands\n";
  cout << "   --tags-add=<list>         list of tags to add for zonegroup placement modify command\n";
  cout << "   --tags-rm=<list>          list of tags to remove for zonegroup placement modify command\n";
  cout << "   --endpoints=<list>        zone endpoints\n";
  cout << "   --index-pool=<pool>       placement target index pool\n";
  cout << "   --data-pool=<pool>        placement target data pool\n";
  cout << "   --data-extra-pool=<pool>  placement target data extra (non-ec) pool\n";
  cout << "   --placement-index-type=<type>\n";
  cout << "                             placement target index type (normal, indexless, or #id)\n";
  cout << "   --compression=<type>      placement target compression type (plugin name or empty/none)\n";
  cout << "   --tier-type=<type>        zone tier type\n";
  cout << "   --tier-config=<k>=<v>[,...]\n";
  cout << "                             set zone tier config keys, values\n";
  cout << "   --tier-config-rm=<k>[,...]\n";
  cout << "                             unset zone tier config keys\n";
  cout << "   --sync-from-all[=false]   set/reset whether zone syncs from all zonegroup peers\n";
  cout << "   --sync-from=[zone-name][,...]\n";
  cout << "                             set list of zones to sync from\n";
  cout << "   --sync-from-rm=[zone-name][,...]\n";
  cout << "                             remove zones from list of zones to sync from\n";
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
  cout << "   --reset-stats             option to 'user stats', reset stats in accordance with user buckets\n";
  cout << "   --show-log-entries=<flag> enable/disable dump of log entries on log show\n";
  cout << "   --show-log-sum=<flag>     enable/disable dump of log summation on log show\n";
  cout << "   --skip-zero-entries       log show only dumps entries that don't have zero value\n";
  cout << "                             in one of the numeric field\n";
  cout << "   --infile=<file>           specify a file to read in when setting data\n";
  cout << "   --categories=<list>       comma separated list of categories, used in usage show\n";
  cout << "   --caps=<caps>             list of caps (e.g., \"usage=read, write; user=read\")\n";
  cout << "   --yes-i-really-mean-it    required for certain operations\n";
  cout << "   --warnings-only           when specified with bucket limit check, list\n";
  cout << "                             only buckets nearing or over the current max\n";
  cout << "                             objects per shard value\n";
  cout << "   --bypass-gc               when specified with bucket deletion, triggers\n";
  cout << "                             object deletions by not involving GC\n";
  cout << "   --inconsistent-index      when specified with bucket deletion and bypass-gc set to true,\n";
  cout << "                             ignores bucket index consistency\n";
  cout << "   --min-rewrite-size        min object size for bucket rewrite (default 4M)\n";
  cout << "   --max-rewrite-size        max object size for bucket rewrite (default ULLONG_MAX)\n";
  cout << "   --min-rewrite-stripe-size min stripe size for object rewrite (default 0)\n";
  cout << "   --trim-delay-ms           time interval in msec to limit the frequency of sync error log entries trimming operations,\n";
  cout << "                             the trimming process will sleep the specified msec for every 1000 entries trimmed\n";
  cout << "\n";
  cout << "<date> := \"YYYY-MM-DD[ hh:mm:ss]\"\n";
  cout << "\nQuota options:\n";
  cout << "   --max-objects             specify max objects (negative value to disable)\n";
  cout << "   --max-size                specify max size (in B/K/M/G/T, negative value to disable)\n";
  cout << "   --quota-scope             scope of quota (bucket, user)\n";
  cout << "\nOrphans search options:\n";
  cout << "   --num-shards              num of shards to use for keeping the temporary scan info\n";
  cout << "   --orphan-stale-secs       num of seconds to wait before declaring an object to be an orphan (default: 86400)\n";
  cout << "   --job-id                  set the job id (for orphans find)\n";
  cout << "   --max-concurrent-ios      maximum concurrent ios for orphans find (default: 32)\n";
  cout << "\nOrphans list-jobs options:\n";
  cout << "   --extra-info              provide extra info in job list\n";
  cout << "\nRole options:\n";
  cout << "   --role-name               name of the role to create\n";
  cout << "   --path                    path to the role\n";
  cout << "   --assume-role-policy-doc  the trust relationship policy document that grants an entity permission to assume the role\n";
  cout << "   --policy-name             name of the policy document\n";
  cout << "   --policy-doc              permission policy document\n";
  cout << "   --path-prefix             path prefix for filtering roles\n";
  cout << "\nMFA options:\n";
  cout << "   --totp-serial             a string that represents the ID of a TOTP token\n";
  cout << "   --totp-seed               the secret seed that is used to calculate the TOTP\n";
  cout << "   --totp-seconds            the time resolution that is being used for TOTP generation\n";
  cout << "   --totp-window             the number of TOTP tokens that are checked before and after the current token when validating token\n";
  cout << "   --totp-pin                the valid value of a TOTP token at a certain time\n";
  cout << "\n";
  generic_client_usage();
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
  OPT_BUCKET_SYNC_MARKERS,
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
  OPT_OBJECT_PUT,
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
  OPT_LC_GET,
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
  OPT_MFA_CREATE,
  OPT_MFA_REMOVE,
  OPT_MFA_GET,
  OPT_MFA_LIST,
  OPT_MFA_CHECK,
  OPT_MFA_RESYNC,
  OPT_RESHARD_STALE_INSTANCES_LIST,
  OPT_RESHARD_STALE_INSTANCES_DELETE,
  OPT_PUBSUB_TOPICS_LIST,
  OPT_PUBSUB_TOPIC_CREATE,
  OPT_PUBSUB_TOPIC_GET,
  OPT_PUBSUB_TOPIC_RM,
  OPT_PUBSUB_NOTIFICATION_CREATE,
  OPT_PUBSUB_NOTIFICATION_RM,
  OPT_PUBSUB_SUB_GET,
  OPT_PUBSUB_SUB_CREATE,
  OPT_PUBSUB_SUB_RM,
  OPT_PUBSUB_SUB_PULL,
  OPT_PUBSUB_EVENT_RM,
};

static int get_cmd(const char *cmd, const char *prev_cmd, const char *prev_prev_cmd, bool *need_more)
{
  using ceph::util::match_str;

  *need_more = false;
  // NOTE: please keep the checks in alphabetical order !!!
  if (strcmp(cmd, "bi") == 0 ||
      strcmp(cmd, "bilog") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "caps") == 0 ||
      strcmp(cmd, "data") == 0 ||
      strcmp(cmd, "datalog") == 0 ||
      strcmp(cmd, "error") == 0 ||
      strcmp(cmd, "event") == 0 ||
      strcmp(cmd, "gc") == 0 ||
      strcmp(cmd, "global") == 0 ||
      strcmp(cmd, "key") == 0 ||
      strcmp(cmd, "log") == 0 ||
      strcmp(cmd, "lc") == 0 ||
      strcmp(cmd, "mdlog") == 0 ||
      strcmp(cmd, "metadata") == 0 ||
      strcmp(cmd, "mfa") == 0 ||
      strcmp(cmd, "notification") == 0 ||
      strcmp(cmd, "object") == 0 ||
      strcmp(cmd, "objects") == 0 ||
      strcmp(cmd, "olh") == 0 ||
      strcmp(cmd, "orphans") == 0 ||
      strcmp(cmd, "period") == 0 ||
      strcmp(cmd, "placement") == 0 ||
      strcmp(cmd, "pool") == 0 ||
      strcmp(cmd, "pools") == 0 ||
      strcmp(cmd, "pubsub") == 0 ||
      strcmp(cmd, "quota") == 0 ||
      strcmp(cmd, "realm") == 0 ||
      strcmp(cmd, "role") == 0 ||
      strcmp(cmd, "role-policy") == 0 ||
      strcmp(cmd, "stale-instances") == 0 ||
      strcmp(cmd, "sub") == 0 ||
      strcmp(cmd, "subuser") == 0 ||
      strcmp(cmd, "sync") == 0 ||
      strcmp(cmd, "topic") == 0 ||
      strcmp(cmd, "topics") == 0 ||
      strcmp(cmd, "usage") == 0 ||
      strcmp(cmd, "user") == 0 ||
      strcmp(cmd, "zone") == 0 ||
      strcmp(cmd, "zonegroup") == 0 ||
      strcmp(cmd, "zonegroups") == 0) {
    *need_more = true;
    return 0;
  }

  /*
   * can do both radosgw-admin bucket reshard, and radosgw-admin reshard bucket
   */
  if (strcmp(cmd, "reshard") == 0 &&
      !(prev_cmd && strcmp(prev_cmd, "bucket") == 0)) {
    *need_more = true;
    return 0;
  }
  if (strcmp(cmd, "bucket") == 0 &&
      !(prev_cmd && strcmp(prev_cmd, "reshard") == 0)) {
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
    if (strcmp(cmd, "list") == 0)
      return OPT_USER_LIST;
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
    if (strcmp(cmd, "reshard") == 0)
      return OPT_BUCKET_RESHARD;
    if (strcmp(cmd, "check") == 0)
      return OPT_BUCKET_CHECK;
    if (strcmp(cmd, "sync") == 0) {
      *need_more = true;
      return 0;
    }
    if (strcmp(cmd, "limit") == 0) {
      *need_more = true;
      return 0;
    }
  } else if (prev_prev_cmd && strcmp(prev_prev_cmd, "bucket") == 0) {
    if (strcmp(prev_cmd, "sync") == 0) {
      if (strcmp(cmd, "status") == 0)
        return OPT_BUCKET_SYNC_STATUS;
      if (strcmp(cmd, "markers") == 0)
        return OPT_BUCKET_SYNC_MARKERS;
      if (strcmp(cmd, "init") == 0)
        return OPT_BUCKET_SYNC_INIT;
      if (strcmp(cmd, "run") == 0)
        return OPT_BUCKET_SYNC_RUN;
      if (strcmp(cmd, "disable") == 0)
        return OPT_BUCKET_SYNC_DISABLE;
      if (strcmp(cmd, "enable") == 0)
        return OPT_BUCKET_SYNC_ENABLE;
    } else if ((strcmp(prev_cmd, "limit") == 0) &&
	       (strcmp(cmd, "check") == 0)) {
      return OPT_BUCKET_LIMIT_CHECK;
    }
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
    if (strcmp(cmd, "clear") == 0)
      return OPT_USAGE_CLEAR;
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
    if (strcmp(cmd, "put") == 0)
      return OPT_OBJECT_PUT;
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
    if (strcmp(cmd, "purge") == 0)
      return OPT_BI_PURGE;
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "global") == 0) &&
	     (strcmp(prev_cmd, "quota") == 0)) {
    if (strcmp(cmd, "get") == 0)
      return OPT_GLOBAL_QUOTA_GET;
    if (strcmp(cmd, "set") == 0)
      return OPT_GLOBAL_QUOTA_SET;
    if (strcmp(cmd, "enable") == 0)
      return OPT_GLOBAL_QUOTA_ENABLE;
    if (strcmp(cmd, "disable") == 0)
      return OPT_GLOBAL_QUOTA_DISABLE;
  } else if (strcmp(prev_cmd, "period") == 0) {
    if (match_str(cmd, "rm", "delete"))
      return OPT_PERIOD_DELETE;
    if (strcmp(cmd, "get") == 0)
      return OPT_PERIOD_GET;
    if (strcmp(cmd, "get-current") == 0)
      return OPT_PERIOD_GET_CURRENT;
    if (strcmp(cmd, "pull") == 0)
      return OPT_PERIOD_PULL;
    if (strcmp(cmd, "push") == 0)
      return OPT_PERIOD_PUSH;
    if (strcmp(cmd, "list") == 0)
      return OPT_PERIOD_LIST;
    if (strcmp(cmd, "update") == 0)
      return OPT_PERIOD_UPDATE;
    if (strcmp(cmd, "commit") == 0)
      return OPT_PERIOD_COMMIT;
  } else if (strcmp(prev_cmd, "realm") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_REALM_CREATE;
    if (match_str(cmd, "rm", "delete"))
      return OPT_REALM_DELETE;
    if (strcmp(cmd, "get") == 0)
      return OPT_REALM_GET;
    if (strcmp(cmd, "get-default") == 0)
      return OPT_REALM_GET_DEFAULT;
    if (strcmp(cmd, "list") == 0)
      return OPT_REALM_LIST;
    if (strcmp(cmd, "list-periods") == 0)
      return OPT_REALM_LIST_PERIODS;
    if (strcmp(cmd, "rename") == 0)
      return OPT_REALM_RENAME;
    if (strcmp(cmd, "set") == 0)
      return OPT_REALM_SET;
    if (strcmp(cmd, "default") == 0)
      return OPT_REALM_DEFAULT;
    if (strcmp(cmd, "pull") == 0)
      return OPT_REALM_PULL;
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "zonegroup") == 0) &&
	     (strcmp(prev_cmd, "placement") == 0)) {
    if (strcmp(cmd, "add") == 0)
      return OPT_ZONEGROUP_PLACEMENT_ADD;
    if (strcmp(cmd, "modify") == 0)
      return OPT_ZONEGROUP_PLACEMENT_MODIFY;
    if (strcmp(cmd, "rm") == 0)
      return OPT_ZONEGROUP_PLACEMENT_RM;
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONEGROUP_PLACEMENT_LIST;
    if (strcmp(cmd, "default") == 0)
      return OPT_ZONEGROUP_PLACEMENT_DEFAULT;
  } else if (strcmp(prev_cmd, "zonegroup") == 0) {
    if (strcmp(cmd, "add") == 0)
      return OPT_ZONEGROUP_ADD;
    if (strcmp(cmd, "create")== 0)
      return OPT_ZONEGROUP_CREATE;
    if (strcmp(cmd, "default") == 0)
      return OPT_ZONEGROUP_DEFAULT;
    if (strcmp(cmd, "delete") == 0)
      return OPT_ZONEGROUP_DELETE;
    if (strcmp(cmd, "get") == 0)
      return OPT_ZONEGROUP_GET;
    if (strcmp(cmd, "modify") == 0)
      return OPT_ZONEGROUP_MODIFY;
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONEGROUP_LIST;
    if (strcmp(cmd, "set") == 0)
      return OPT_ZONEGROUP_SET;
    if (match_str(cmd, "rm", "remove"))
      return OPT_ZONEGROUP_REMOVE;
    if (strcmp(cmd, "rename") == 0)
      return OPT_ZONEGROUP_RENAME;
  } else if (strcmp(prev_cmd, "quota") == 0) {
    if (strcmp(cmd, "set") == 0)
      return OPT_QUOTA_SET;
    if (strcmp(cmd, "enable") == 0)
      return OPT_QUOTA_ENABLE;
    if (strcmp(cmd, "disable") == 0)
      return OPT_QUOTA_DISABLE;
  } else if (strcmp(prev_cmd, "zonegroups") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONEGROUP_LIST;
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "zone") == 0) &&
	     (strcmp(prev_cmd, "placement") == 0)) {
    if (strcmp(cmd, "add") == 0)
      return OPT_ZONE_PLACEMENT_ADD;
    if (strcmp(cmd, "modify") == 0)
      return OPT_ZONE_PLACEMENT_MODIFY;
    if (strcmp(cmd, "rm") == 0)
      return OPT_ZONE_PLACEMENT_RM;
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONE_PLACEMENT_LIST;
  } else if (strcmp(prev_cmd, "zone") == 0) {
    if (strcmp(cmd, "delete") == 0)
      return OPT_ZONE_DELETE;
    if (strcmp(cmd, "create") == 0)
      return OPT_ZONE_CREATE;
    if (strcmp(cmd, "get") == 0)
      return OPT_ZONE_GET;
    if (strcmp(cmd, "set") == 0)
      return OPT_ZONE_SET;
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONE_LIST;
    if (strcmp(cmd, "modify") == 0)
      return OPT_ZONE_MODIFY;
    if (strcmp(cmd, "rename") == 0)
      return OPT_ZONE_RENAME;
    if (strcmp(cmd, "default") == 0)
      return OPT_ZONE_DEFAULT;
  } else if (strcmp(prev_cmd, "zones") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONE_LIST;
  } else if (strcmp(prev_cmd, "gc") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_GC_LIST;
    if (strcmp(cmd, "process") == 0)
      return OPT_GC_PROCESS;
  } else if (strcmp(prev_cmd, "lc") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_LC_LIST;
    if (strcmp(cmd, "get") == 0)
      return OPT_LC_GET;
    if (strcmp(cmd, "process") == 0)
      return OPT_LC_PROCESS;
  } else if (strcmp(prev_cmd, "orphans") == 0) {
    if (strcmp(cmd, "find") == 0)
      return OPT_ORPHANS_FIND;
    if (strcmp(cmd, "finish") == 0)
      return OPT_ORPHANS_FINISH;
    if (strcmp(cmd, "list-jobs") == 0)
      return OPT_ORPHANS_LIST_JOBS;
  } else if (strcmp(prev_cmd, "metadata") == 0) {
    if (strcmp(cmd, "get") == 0)
      return OPT_METADATA_GET;
    if (strcmp(cmd, "put") == 0)
      return OPT_METADATA_PUT;
    if (strcmp(cmd, "rm") == 0)
      return OPT_METADATA_RM;
    if (strcmp(cmd, "list") == 0)
      return OPT_METADATA_LIST;
    if (strcmp(cmd, "sync") == 0) {
      *need_more = true;
      return 0;
    }
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "metadata") == 0) &&
	     (strcmp(prev_cmd, "sync") == 0)) {
    if (strcmp(cmd, "status") == 0)
      return OPT_METADATA_SYNC_STATUS;
    if (strcmp(cmd, "init") == 0)
      return OPT_METADATA_SYNC_INIT;
    if (strcmp(cmd, "run") == 0)
      return OPT_METADATA_SYNC_RUN;
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "sync") == 0) &&
	     (strcmp(prev_cmd, "error") == 0)) {
    if (strcmp(cmd, "list") == 0)
      return OPT_SYNC_ERROR_LIST;
    if (strcmp(cmd, "trim") == 0)
      return OPT_SYNC_ERROR_TRIM; 
  } else if (strcmp(prev_cmd, "mdlog") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_MDLOG_LIST;
    if (strcmp(cmd, "autotrim") == 0)
      return OPT_MDLOG_AUTOTRIM;
    if (strcmp(cmd, "trim") == 0)
      return OPT_MDLOG_TRIM;
    if (strcmp(cmd, "fetch") == 0)
      return OPT_MDLOG_FETCH;
    if (strcmp(cmd, "status") == 0)
      return OPT_MDLOG_STATUS;
  } else if (strcmp(prev_cmd, "bilog") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BILOG_LIST;
    if (strcmp(cmd, "trim") == 0)
      return OPT_BILOG_TRIM;
    if (strcmp(cmd, "status") == 0)
      return OPT_BILOG_STATUS;
    if (strcmp(cmd, "autotrim") == 0)
      return OPT_BILOG_AUTOTRIM;
  } else if (strcmp(prev_cmd, "data") == 0) {
    if (strcmp(cmd, "sync") == 0) {
      *need_more = true;
      return 0;
    }
  } else if (strcmp(prev_cmd, "datalog") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_DATALOG_LIST;
    if (strcmp(cmd, "trim") == 0)
      return OPT_DATALOG_TRIM;
    if (strcmp(cmd, "status") == 0)
      return OPT_DATALOG_STATUS;
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "data") == 0) &&
	     (strcmp(prev_cmd, "sync") == 0)) {
    if (strcmp(cmd, "status") == 0)
      return OPT_DATA_SYNC_STATUS;
    if (strcmp(cmd, "init") == 0)
      return OPT_DATA_SYNC_INIT;
    if (strcmp(cmd, "run") == 0)
      return OPT_DATA_SYNC_RUN;
  } else if (strcmp(prev_cmd, "sync") == 0) {
    if (strcmp(cmd, "status") == 0)
      return OPT_SYNC_STATUS;
  } else if (strcmp(prev_cmd, "role") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_ROLE_CREATE;
    if (strcmp(cmd, "rm") == 0)
      return OPT_ROLE_DELETE;
    if (strcmp(cmd, "get") == 0)
      return OPT_ROLE_GET;
    if (strcmp(cmd, "modify") == 0)
      return OPT_ROLE_MODIFY;
    if (strcmp(cmd, "list") == 0)
      return OPT_ROLE_LIST;
  } else if (strcmp(prev_cmd, "role-policy") == 0) {
    if (strcmp(cmd, "put") == 0)
      return OPT_ROLE_POLICY_PUT;
    if (strcmp(cmd, "list") == 0)
      return OPT_ROLE_POLICY_LIST;
    if (strcmp(cmd, "get") == 0)
      return OPT_ROLE_POLICY_GET;
    if (match_str(cmd, "rm", "delete"))
      return OPT_ROLE_POLICY_DELETE;
  } else if (strcmp(prev_cmd, "reshard") == 0) {
    if (strcmp(cmd, "bucket") == 0)
      return OPT_BUCKET_RESHARD;
    if (strcmp(cmd, "add") == 0)
      return OPT_RESHARD_ADD;
    if (strcmp(cmd, "list") == 0)
      return OPT_RESHARD_LIST;
    if (strcmp(cmd, "status") == 0)
      return OPT_RESHARD_STATUS;
    if (strcmp(cmd, "process") == 0)
      return OPT_RESHARD_PROCESS;
    if (strcmp(cmd, "cancel") == 0)
      return OPT_RESHARD_CANCEL;
  } else if (strcmp(prev_cmd, "mfa") == 0) {
    if (strcmp(cmd, "create") == 0)
      return OPT_MFA_CREATE;
    if (strcmp(cmd, "remove") == 0)
      return OPT_MFA_REMOVE;
    if (strcmp(cmd, "get") == 0)
      return OPT_MFA_GET;
    if (strcmp(cmd, "list") == 0)
      return OPT_MFA_LIST;
    if (strcmp(cmd, "check") == 0)
      return OPT_MFA_CHECK;
    if (strcmp(cmd, "resync") == 0)
      return OPT_MFA_RESYNC;
  } else if ((prev_prev_cmd && strcmp(prev_prev_cmd, "reshard") == 0) &&
	     (strcmp(prev_cmd, "stale-instances") == 0)) {
    if (strcmp(cmd, "list") == 0)
      return OPT_RESHARD_STALE_INSTANCES_LIST;
    if (match_str(cmd, "rm", "delete"))
      return OPT_RESHARD_STALE_INSTANCES_DELETE;
  } else if (prev_prev_cmd && strcmp(prev_prev_cmd, "pubsub") == 0) {
    if (strcmp(prev_cmd, "topics") == 0) {
      if (strcmp(cmd, "list") == 0)
        return OPT_PUBSUB_TOPICS_LIST;
    } else if (strcmp(prev_cmd, "topic") == 0) {
      if (strcmp(cmd, "create") == 0)
        return OPT_PUBSUB_TOPIC_CREATE;
      if (strcmp(cmd, "get") == 0)
        return OPT_PUBSUB_TOPIC_GET;
      if (strcmp(cmd, "rm") == 0)
        return OPT_PUBSUB_TOPIC_RM;
    } else if (strcmp(prev_cmd, "notification") == 0) {
      if (strcmp(cmd, "create") == 0)
        return OPT_PUBSUB_NOTIFICATION_CREATE;
      if (strcmp(cmd, "rm") == 0)
        return OPT_PUBSUB_NOTIFICATION_RM;
    } else if (strcmp(prev_cmd, "sub") == 0) {
      if (strcmp(cmd, "get") == 0)
        return OPT_PUBSUB_SUB_GET;
      if (strcmp(cmd, "create") == 0)
        return OPT_PUBSUB_SUB_CREATE;
      if (strcmp(cmd, "rm") == 0)
        return OPT_PUBSUB_SUB_RM;
      if (strcmp(cmd, "pull") == 0)
        return OPT_PUBSUB_SUB_PULL;
    } else if (strcmp(prev_cmd, "event") == 0) {
      if (strcmp(cmd, "rm") == 0)
        return OPT_PUBSUB_EVENT_RM;
    }
  }
  return -EINVAL;
}

BIIndexType get_bi_index_type(const string& type_str) {
  if (type_str == "plain")
    return BIIndexType::Plain;
  if (type_str == "instance")
    return BIIndexType::Instance;
  if (type_str == "olh")
    return BIIndexType::OLH;

  return BIIndexType::Invalid;
}

void dump_bi_entry(bufferlist& bl, BIIndexType index_type, Formatter *formatter)
{
  auto iter = bl.cbegin();
  switch (index_type) {
    case BIIndexType::Plain:
    case BIIndexType::Instance:
      {
        rgw_bucket_dir_entry entry;
        decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    case BIIndexType::OLH:
      {
        rgw_bucket_olh_entry entry;
        decode(entry, iter);
        encode_json("entry", entry, formatter);
      }
      break;
    default:
      ceph_abort();
      break;
  }
}

static void show_user_info(RGWUserInfo& info, Formatter *formatter)
{
  encode_json("user_info", info, formatter);
  formatter->flush(cout);
  cout << std::endl;
}

static void show_perm_policy(string perm_policy, Formatter* formatter)
{
  formatter->open_object_section("role");
  formatter->dump_string("Permission policy", perm_policy);
  formatter->close_section();
  formatter->flush(cout);
}

static void show_policy_names(std::vector<string> policy_names, Formatter* formatter)
{
  formatter->open_array_section("PolicyNames");
  for (const auto& it : policy_names) {
    formatter->dump_string("policyname", it);
  }
  formatter->close_section();
  formatter->flush(cout);
}

static void show_role_info(RGWRole& role, Formatter* formatter)
{
  formatter->open_object_section("role");
  role.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
}

static void show_roles_info(vector<RGWRole>& roles, Formatter* formatter)
{
  formatter->open_array_section("Roles");
  for (const auto& it : roles) {
    formatter->open_object_section("role");
    it.dump(formatter);
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
}

static void show_reshard_status(
  const list<cls_rgw_bucket_instance_entry>& status, Formatter *formatter)
{
  formatter->open_array_section("status");
  for (const auto& entry : status) {
    formatter->open_object_section("entry");
    formatter->dump_string("reshard_status", to_string(entry.reshard_status));
    formatter->dump_string("new_bucket_instance_id",
			   entry.new_bucket_instance_id);
    formatter->dump_unsigned("num_shards", entry.num_shards);
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
}

class StoreDestructor {
  RGWRados *store;
public:
  explicit StoreDestructor(RGWRados *_s) : store(_s) {}
  ~StoreDestructor() {
    RGWStoreManager::close_storage(store);
    rgw_http_client_cleanup();
  }
};

static int init_bucket(const string& tenant_name, const string& bucket_name, const string& bucket_id,
                       RGWBucketInfo& bucket_info, rgw_bucket& bucket, map<string, bufferlist> *pattrs = nullptr)
{
  if (!bucket_name.empty()) {
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();
    int r;
    if (bucket_id.empty()) {
      r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, nullptr, pattrs);
    } else {
      string bucket_instance_id = bucket_name + ":" + bucket_id;
      r = store->get_bucket_instance_info(obj_ctx, bucket_instance_id, bucket_info, NULL, pattrs);
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

    r = safe_read(fd, buf, READ_CHUNK);
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
  if (!p.parse(bl.c_str(), bl.length())) {
    cout << "failed to parse JSON" << std::endl;
    return -EINVAL;
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

  auto iter = bl.cbegin();

  try {
    decode(t, iter);
  } catch (buffer::error& err) {
    return false;
  }

  encode_json(field_name, t, f);

  return true;
}

static bool dump_string(const char *field_name, bufferlist& bl, Formatter *f)
{
  string val = bl.to_str();
  f->dump_string(field_name, val.c_str() /* hide encoded null termination chars */);

  return true;
}

void set_quota_info(RGWQuotaInfo& quota, int opt_cmd, int64_t max_size, int64_t max_objects,
                    bool have_max_size, bool have_max_objects)
{
  switch (opt_cmd) {
    case OPT_QUOTA_ENABLE:
    case OPT_GLOBAL_QUOTA_ENABLE:
      quota.enabled = true;

      // falling through on purpose

    case OPT_QUOTA_SET:
    case OPT_GLOBAL_QUOTA_SET:
      if (have_max_objects) {
        if (max_objects < 0) {
          quota.max_objects = -1;
        } else {
          quota.max_objects = max_objects;
        }
      }
      if (have_max_size) {
        if (max_size < 0) {
          quota.max_size = -1;
        } else {
          quota.max_size = rgw_rounded_kb(max_size) * 1024;
        }
      }
      break;
    case OPT_QUOTA_DISABLE:
    case OPT_GLOBAL_QUOTA_DISABLE:
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
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, NULL, &attrs);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket_info.quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

   r = store->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
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
  rgw_obj_key k;
  string ns; /* empty namespace */
  return rgw_obj_key::oid_to_key_in_ns(name, &k, ns);
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

  int ret = read_op.prepare();
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
    auto biter = bl.cbegin();
    decode(manifest, biter);
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

  get_obj_bucket_and_oid_loc(obj, oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);


  RGWObjectCtx obj_ctx(store);

  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  int ret = read_op.prepare();
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = store->fix_head_obj_locator(bucket_info, needs_fixing, remove_bad, key);
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

  bool needs_fixing;
  string status;

  int ret = store->fix_tail_obj_locator(bucket_info, key, fix, &needs_fixing);
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
  vector<rgw_bucket_dir_entry> result;
  map<string, bool> common_prefixes;
  string ns;

  RGWRados::Bucket target(store, bucket_info);
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

    for (vector<rgw_bucket_dir_entry>::iterator iter = result.begin(); iter != result.end(); ++iter) {
      rgw_obj_key key = iter->key;
      rgw_obj obj(bucket, key);

      if (key.name[0] == '_') {
        ret = check_obj_locator_underscore(bucket_info, obj, key, fix, remove_bad, f);

	if (ret >= 0) {
          ret = check_obj_tail_locator_underscore(bucket_info, obj, key, fix, f);
          if (ret < 0) {
              cerr << "ERROR: check_obj_tail_locator_underscore(): " << cpp_strerror(-ret) << std::endl;
              return -ret;
          }
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

int set_bucket_sync_enabled(RGWRados *store, int opt_cmd, const string& tenant_name, const string& bucket_name)
{
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, NULL, &attrs);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  if (opt_cmd == OPT_BUCKET_SYNC_ENABLE) {
    bucket_info.flags &= ~BUCKET_DATASYNC_DISABLED;
  } else if (opt_cmd == OPT_BUCKET_SYNC_DISABLE) {
    bucket_info.flags |= BUCKET_DATASYNC_DISABLED;
  }

  r = store->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  int shards_num = bucket_info.num_shards? bucket_info.num_shards : 1;
  int shard_id = bucket_info.num_shards? 0 : -1;

  if (opt_cmd == OPT_BUCKET_SYNC_DISABLE) {
    r = store->stop_bi_log_entries(bucket_info, -1);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing stop bilog" << dendl;
      return r;
    }
  } else {
    r = store->resync_bi_log_entries(bucket_info, -1);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing resync bilog" << dendl;
      return r;
    }
  }

  for (int i = 0; i < shards_num; ++i, ++shard_id) {
    r = store->data_log->add_entry(bucket_info.bucket, shard_id);
    if (r < 0) {
      lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
      return r;
    }
  }

  return 0;
}


/// search for a matching zone/zonegroup id and return a connection if found
static boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store,
                                                    const RGWZoneGroup& zonegroup,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  if (remote == zonegroup.get_id()) {
    conn.emplace(store->ctx(), store->svc.zone, remote, zonegroup.endpoints);
  } else {
    for (const auto& z : zonegroup.zones) {
      const auto& zone = z.second;
      if (remote == zone.id) {
        conn.emplace(store->ctx(), store->svc.zone, remote, zone.endpoints);
        break;
      }
    }
  }
  return conn;
}

/// search each zonegroup for a connection
static boost::optional<RGWRESTConn> get_remote_conn(RGWRados *store,
                                                    const RGWPeriodMap& period_map,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  for (const auto& zg : period_map.zonegroups) {
    conn = get_remote_conn(store, zg.second, remote);
    if (conn) {
      break;
    }
  }
  return conn;
}

// we expect a very small response
static constexpr size_t MAX_REST_RESPONSE = 128 * 1024;

static int send_to_remote_gateway(RGWRESTConn* conn, req_info& info,
                                  bufferlist& in_data, JSONParser& parser)
{
  if (!conn) {
    return -EINVAL;
  }

  ceph::bufferlist response;
  rgw_user user;
  int ret = conn->forward(user, info, nullptr, MAX_REST_RESPONSE, &in_data, &response);

  int parse_ret = parser.parse(response.c_str(), response.length());
  if (parse_ret < 0) {
    cerr << "failed to parse response" << std::endl;
    return parse_ret;
  }
  return ret;
}

static int send_to_url(const string& url, const string& access,
                       const string& secret, req_info& info,
                       bufferlist& in_data, JSONParser& parser)
{
  if (access.empty() || secret.empty()) {
    cerr << "An --access-key and --secret must be provided with --url." << std::endl;
    return -EINVAL;
  }
  RGWAccessKey key;
  key.id = access;
  key.key = secret;

  param_vec_t params;
  RGWRESTSimpleRequest req(g_ceph_context, info.method, url, NULL, &params);

  bufferlist response;
  int ret = req.forward_request(key, info, MAX_REST_RESPONSE, &in_data, &response);

  int parse_ret = parser.parse(response.c_str(), response.length());
  if (parse_ret < 0) {
    cout << "failed to parse response" << std::endl;
    return parse_ret;
  }
  return ret;
}

static int send_to_remote_or_url(RGWRESTConn *conn, const string& url,
                                 const string& access, const string& secret,
                                 req_info& info, bufferlist& in_data,
                                 JSONParser& parser)
{
  if (url.empty()) {
    return send_to_remote_gateway(conn, info, in_data, parser);
  }
  return send_to_url(url, access, secret, info, in_data, parser);
}

static int commit_period(RGWRealm& realm, RGWPeriod& period,
                         string remote, const string& url,
                         const string& access, const string& secret,
                         bool force)
{
  const string& master_zone = period.get_master_zone();
  if (master_zone.empty()) {
    cerr << "cannot commit period: period does not have a master zone of a master zonegroup" << std::endl;
    return -EINVAL;
  }
  // are we the period's master zone?
  if (store->svc.zone->get_zone_params().get_id() == master_zone) {
    // read the current period
    RGWPeriod current_period;
    int ret = current_period.init(g_ceph_context, store->svc.sysobj, realm.get_id());
    if (ret < 0) {
      cerr << "Error initializing current period: "
          << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    // the master zone can commit locally
    ret = period.commit(store, realm, current_period, cerr, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
    }
    return ret;
  }

  if (remote.empty() && url.empty()) {
    // use the new master zone's connection
    remote = master_zone;
    cout << "Sending period to new master zone " << remote << std::endl;
  }
  boost::optional<RGWRESTConn> conn;
  RGWRESTConn *remote_conn = nullptr;
  if (!remote.empty()) {
    conn = get_remote_conn(store, period.get_map(), remote);
    if (!conn) {
      cerr << "failed to find a zone or zonegroup for remote "
          << remote << std::endl;
      return -ENOENT;
    }
    remote_conn = &*conn;
  }

  // push period to the master with an empty period id
  period.set_id("");

  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "POST";
  info.request_uri = "/admin/realm/period";

  // json format into a bufferlist
  JSONFormatter jf(false);
  encode_json("period", period, &jf);
  bufferlist bl;
  jf.flush(bl);

  JSONParser p;
  int ret = send_to_remote_or_url(remote_conn, url, access, secret, info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;

    // did we parse an error message?
    auto message = p.find_obj("Message");
    if (message) {
      cerr << "Reason: " << message->get_data() << std::endl;
    }
    return ret;
  }

  // decode the response and store it back
  try {
    decode_json_obj(period, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  if (period.get_id().empty()) {
    cerr << "Period commit got back an empty period id" << std::endl;
    return -EINVAL;
  }
  // the master zone gave us back the period that it committed, so it's
  // safe to save it as our latest epoch
  ret = period.store_info(false);
  if (ret < 0) {
    cerr << "Error storing committed period " << period.get_id() << ": "
        << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = period.set_latest_epoch(period.get_epoch());
  if (ret < 0) {
    cerr << "Error updating period epoch: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = period.reflect();
  if (ret < 0) {
    cerr << "Error updating local objects: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  realm.notify_new_period(period);
  return ret;
}

static int update_period(const string& realm_id, const string& realm_name,
                         const string& period_id, const string& period_epoch,
                         bool commit, const string& remote, const string& url,
                         const string& access, const string& secret,
                         Formatter *formatter, bool force)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(g_ceph_context, store->svc.sysobj);
  if (ret < 0 ) {
    cerr << "Error initializing realm " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  epoch_t epoch = 0;
  if (!period_epoch.empty()) {
    epoch = atoi(period_epoch.c_str());
  }
  RGWPeriod period(period_id, epoch);
  ret = period.init(g_ceph_context, store->svc.sysobj, realm.get_id());
  if (ret < 0) {
    cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  period.fork();
  ret = period.update();
  if(ret < 0) {
    // Dropping the error message here, as both the ret codes were handled in
    // period.update()
    return ret;
  }
  ret = period.store_info(false);
  if (ret < 0) {
    cerr << "failed to store period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  if (commit) {
    ret = commit_period(realm, period, remote, url, access, secret, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}

static int init_bucket_for_sync(const string& tenant, const string& bucket_name,
                                const string& bucket_id, rgw_bucket& bucket)
{
  RGWBucketInfo bucket_info;

  int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  return 0;
}

static int do_period_pull(RGWRESTConn *remote_conn, const string& url,
                          const string& access_key, const string& secret_key,
                          const string& realm_id, const string& realm_name,
                          const string& period_id, const string& period_epoch,
                          RGWPeriod *period)
{
  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "GET";
  info.request_uri = "/admin/realm/period";

  map<string, string> &params = info.args.get_params();
  if (!realm_id.empty())
    params["realm_id"] = realm_id;
  if (!realm_name.empty())
    params["realm_name"] = realm_name;
  if (!period_id.empty())
    params["period_id"] = period_id;
  if (!period_epoch.empty())
    params["epoch"] = period_epoch;

  bufferlist bl;
  JSONParser p;
  int ret = send_to_remote_or_url(remote_conn, url, access_key, secret_key,
                                  info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  ret = period->init(g_ceph_context, store->svc.sysobj, false);
  if (ret < 0) {
    cerr << "faile to init period " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  try {
    decode_json_obj(*period, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  ret = period->store_info(false);
  if (ret < 0) {
    cerr << "Error storing period " << period->get_id() << ": " << cpp_strerror(ret) << std::endl;
  }
  // store latest epoch (ignore errors)
  period->update_latest_epoch(period->get_epoch());
  return 0;
}

static int read_current_period_id(RGWRados* store, const std::string& realm_id,
                                  const std::string& realm_name,
                                  std::string* period_id)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(g_ceph_context, store->svc.sysobj);
  if (ret < 0) {
    std::cerr << "failed to read realm: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  *period_id = realm.get_current_period();
  return 0;
}

void flush_ss(stringstream& ss, list<string>& l)
{
  if (!ss.str().empty()) {
    l.push_back(ss.str());
  }
  ss.str("");
}

stringstream& push_ss(stringstream& ss, list<string>& l, int tab = 0)
{
  flush_ss(ss, l);
  if (tab > 0) {
    ss << setw(tab) << "" << setw(1);
  }
  return ss;
}

static void get_md_sync_status(list<string>& status)
{
  RGWMetaSyncStatusManager sync(store, store->get_async_rados());

  int ret = sync.init();
  if (ret < 0) {
    status.push_back(string("failed to retrieve sync info: sync.init() failed: ") + cpp_strerror(-ret));
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0) {
    status.push_back(string("failed to read sync status: ") + cpp_strerror(-ret));
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_meta_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_meta_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_meta_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  status.push_back(status_str);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;
  set<int> shards_behind_set;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
      int shard_id = marker_iter.first;
      shards_behind_set.insert(shard_id);
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  stringstream ss;
  push_ss(ss, status) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status) << "full sync: " << full_total - full_complete << " entries to sync";
  }

  push_ss(ss, status) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  map<int, RGWMetadataLogInfo> master_shards_info;
  string master_period = store->svc.zone->get_current_period_id();

  ret = sync.read_master_log_shards_info(master_period, &master_shards_info);
  if (ret < 0) {
    status.push_back(string("failed to fetch master sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, string> shards_behind;
  if (sync_status.sync_info.period != master_period) {
    status.push_back(string("master is on a different period: master_period=" +
                            master_period + " local_period=" + sync_status.sync_info.period));
  } else {
    for (auto local_iter : sync_status.sync_markers) {
      int shard_id = local_iter.first;
      auto iter = master_shards_info.find(shard_id);

      if (iter == master_shards_info.end()) {
        /* huh? */
        derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
        continue;
      }
      auto master_marker = iter->second.marker;
      if (local_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync &&
          master_marker > local_iter.second.marker) {
        shards_behind[shard_id] = local_iter.second.marker;
        shards_behind_set.insert(shard_id);
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status) << "metadata is caught up with master";
  } else {
    push_ss(ss, status) << "metadata is behind on " << total_behind << " shards";
    
    push_ss(ss, status) << "behind shards: " << "[" << shards_behind_set << "]";

    map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(sync_status.sync_info.period, shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch master next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      ceph::real_time oldest;
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (ceph::real_clock::is_zero(oldest)) {
            oldest = entry.timestamp;
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest) {
            oldest = entry.timestamp;
          }
        }
      }

      if (!ceph::real_clock::is_zero(oldest)) {
        push_ss(ss, status) << "oldest incremental change not applied: " << oldest;
      }
    }
  }

  flush_ss(ss, status);
}

static void get_data_sync_status(const string& source_zone, list<string>& status, int tab)
{
  stringstream ss;

  RGWZone *sz;

  if (!store->svc.zone->find_zone_by_id(source_zone, &sz)) {
    push_ss(ss, status, tab) << string("zone not found");
    flush_ss(ss, status);
    return;
  }

  if (!store->svc.zone->zone_syncs_from(store->svc.zone->get_zone(), *sz)) {
    push_ss(ss, status, tab) << string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

  int ret = sync.init();
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(&sync_status);
  if (ret < 0 && ret != -ENOENT) {
    push_ss(ss, status, tab) << string("failed read sync status: ") + cpp_strerror(-ret);
    return;
  }

  set<int> recovering_shards;
  ret = sync.read_recovering_shards(sync_status.sync_info.num_shards, recovering_shards);
  if (ret < 0 && ret != ENOENT) {
    push_ss(ss, status, tab) << string("failed read recovering shards: ") + cpp_strerror(-ret);
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_data_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_data_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_data_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  push_ss(ss, status, tab) << status_str;

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;
  set<int> shards_behind_set;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
      int shard_id = marker_iter.first;
      shards_behind_set.insert(shard_id);
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  push_ss(ss, status, tab) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status, tab) << "full sync: " << full_total - full_complete << " buckets to sync";
  }

  push_ss(ss, status, tab) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  map<int, RGWDataChangesLogInfo> source_shards_info;

  ret = sync.read_source_log_shards_info(&source_shards_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to fetch source sync status: ") + cpp_strerror(-ret);
    return;
  }

  map<int, string> shards_behind;

  for (auto local_iter : sync_status.sync_markers) {
    int shard_id = local_iter.first;
    auto iter = source_shards_info.find(shard_id);

    if (iter == source_shards_info.end()) {
      /* huh? */
      derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
      continue;
    }
    auto master_marker = iter->second.marker;
    if (local_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync &&
        master_marker > local_iter.second.marker) {
      shards_behind[shard_id] = local_iter.second.marker;
      shards_behind_set.insert(shard_id);
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  int total_recovering = recovering_shards.size();
  if (total_behind == 0 && total_recovering == 0) {
    push_ss(ss, status, tab) << "data is caught up with source";
  } else if (total_behind > 0) {
    push_ss(ss, status, tab) << "data is behind on " << total_behind << " shards";

    push_ss(ss, status, tab) << "behind shards: " << "[" << shards_behind_set << "]" ;

    map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      ceph::real_time oldest;
      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (ceph::real_clock::is_zero(oldest)) {
            oldest = entry.timestamp;
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest) {
            oldest = entry.timestamp;
          }
        }
      }

      if (!ceph::real_clock::is_zero(oldest)) {
        push_ss(ss, status, tab) << "oldest incremental change not applied: " << oldest;
      }
    }
  }

  if (total_recovering > 0) {
    push_ss(ss, status, tab) << total_recovering << " shards are recovering";
    push_ss(ss, status, tab) << "recovering shards: " << "[" << recovering_shards << "]";
  }

  flush_ss(ss, status);
}

static void tab_dump(const string& header, int width, const list<string>& entries)
{
  string s = header;

  for (auto e : entries) {
    cout << std::setw(width) << s << std::setw(1) << " " << e << std::endl;
    s.clear();
  }
}


static void sync_status(Formatter *formatter)
{
  const RGWRealm& realm = store->svc.zone->get_realm();
  const RGWZoneGroup& zonegroup = store->svc.zone->get_zonegroup();
  const RGWZone& zone = store->svc.zone->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << realm.get_id() << " (" << realm.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone.id << " (" << zone.name << ")" << std::endl;

  list<string> md_status;

  if (store->svc.zone->is_meta_master()) {
    md_status.push_back("no sync (zone is master)");
  } else {
    get_md_sync_status(md_status);
  }

  tab_dump("metadata sync", width, md_status);

  list<string> data_status;

  auto& zone_conn_map = store->svc.zone->get_zone_conn_map();

  for (auto iter : zone_conn_map) {
    const string& source_id = iter.first;
    string source_str = "source: ";
    string s = source_str + source_id;
    RGWZone *sz;
    if (store->svc.zone->find_zone_by_id(source_id, &sz)) {
      s += string(" (") + sz->name + ")";
    }
    data_status.push_back(s);
    get_data_sync_status(source_id, data_status, source_str.size());
  }

  tab_dump("data sync", width, data_status);
}

struct indented {
  int w; // indent width
  std::string_view header;
  indented(int w, std::string_view header = "") : w(w), header(header) {}
};
std::ostream& operator<<(std::ostream& out, const indented& h) {
  return out << std::setw(h.w) << h.header << std::setw(1) << ' ';
}

static int remote_bilog_markers(RGWRados *store, const RGWZone& source,
                                RGWRESTConn *conn, const RGWBucketInfo& info,
                                BucketIndexShardsManager *markers)
{
  const auto instance_key = info.bucket.get_key();
  const rgw_http_param_pair params[] = {
    { "type" , "bucket-index" },
    { "bucket-instance", instance_key.c_str() },
    { "info" , nullptr },
    { nullptr, nullptr }
  };
  rgw_bucket_index_marker_info result;
  int r = conn->get_json_resource("/admin/log/", params, result);
  if (r < 0) {
    lderr(store->ctx()) << "failed to fetch remote log markers: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = markers->from_string(result.max_marker, -1);
  if (r < 0) {
    lderr(store->ctx()) << "failed to decode remote log markers" << dendl;
    return r;
  }
  return 0;
}

static int bucket_source_sync_status(RGWRados *store, const RGWZone& zone,
                                     const RGWZone& source, RGWRESTConn *conn,
                                     const RGWBucketInfo& bucket_info,
                                     int width, std::ostream& out)
{
  out << indented{width, "source zone"} << source.id << " (" << source.name << ")\n";

  // syncing from this zone?
  if (!zone.syncs_from(source.id)) {
    out << indented{width} << "not in sync_from\n";
    return 0;
  }
  std::vector<rgw_bucket_shard_sync_info> status;
  int r = rgw_bucket_sync_status(dpp(), store, source.id, bucket_info, &status);
  if (r < 0) {
    lderr(store->ctx()) << "failed to read bucket sync status: " << cpp_strerror(r) << dendl;
    return r;
  }

  int num_full = 0;
  int num_inc = 0;
  uint64_t full_complete = 0;
  const size_t total_shards = status.size();

  using BucketSyncState = rgw_bucket_shard_sync_info::SyncState;
  for (size_t shard_id = 0; shard_id < total_shards; shard_id++) {
    auto& m = status[shard_id];
    if (m.state == BucketSyncState::StateFullSync) {
      num_full++;
      full_complete += m.full_marker.count;
    } else if (m.state == BucketSyncState::StateIncrementalSync) {
      num_inc++;
    }
  }

  out << indented{width} << "full sync: " << num_full << "/" << total_shards << " shards\n";
  if (num_full > 0) {
    out << indented{width} << "full sync: " << full_complete << " objects completed\n";
  }
  out << indented{width} << "incremental sync: " << num_inc << "/" << total_shards << " shards\n";

  BucketIndexShardsManager remote_markers;
  r = remote_bilog_markers(store, source, conn, bucket_info, &remote_markers);
  if (r < 0) {
    lderr(store->ctx()) << "failed to read remote log: " << cpp_strerror(r) << dendl;
    return r;
  }

  std::set<int> shards_behind;
  for (auto& r : remote_markers.get()) {
    auto shard_id = r.first;
    auto& m = status[shard_id];
    if (r.second.empty()) {
      continue; // empty bucket index shard
    }
    auto pos = BucketIndexShardsManager::get_shard_marker(m.inc_marker.position);
    if (m.state != BucketSyncState::StateIncrementalSync || pos != r.second) {
      shards_behind.insert(shard_id);
    }
  }
  if (shards_behind.empty()) {
    out << indented{width} << "bucket is caught up with source\n";
  } else {
    out << indented{width} << "bucket is behind on " << shards_behind.size() << " shards\n";
    out << indented{width} << "behind shards: [" << shards_behind << "]\n" ;
  }
  return 0;
}

static int bucket_sync_status(RGWRados *store, const RGWBucketInfo& info,
                              const std::string& source_zone_id,
                              std::ostream& out)
{
  const RGWRealm& realm = store->svc.zone->get_realm();
  const RGWZoneGroup& zonegroup = store->svc.zone->get_zonegroup();
  const RGWZone& zone = store->svc.zone->get_zone();
  constexpr int width = 15;

  out << indented{width, "realm"} << realm.get_id() << " (" << realm.get_name() << ")\n";
  out << indented{width, "zonegroup"} << zonegroup.get_id() << " (" << zonegroup.get_name() << ")\n";
  out << indented{width, "zone"} << zone.id << " (" << zone.name << ")\n";
  out << indented{width, "bucket"} << info.bucket << "\n\n";

  if (!info.datasync_flag_enabled()) {
    out << "Sync is disabled for bucket " << info.bucket.name << '\n';
    return 0;
  }

  auto& zone_conn_map = store->svc.zone->get_zone_conn_map();
  if (!source_zone_id.empty()) {
    auto z = zonegroup.zones.find(source_zone_id);
    if (z == zonegroup.zones.end()) {
      lderr(store->ctx()) << "Source zone not found in zonegroup "
          << zonegroup.get_name() << dendl;
      return -EINVAL;
    }
    auto c = zone_conn_map.find(source_zone_id);
    if (c == zone_conn_map.end()) {
      lderr(store->ctx()) << "No connection to zone " << z->second.name << dendl;
      return -EINVAL;
    }
    return bucket_source_sync_status(store, zone, z->second, c->second,
                                     info, width, out);
  }

  for (const auto& z : zonegroup.zones) {
    auto c = zone_conn_map.find(z.second.id);
    if (c != zone_conn_map.end()) {
      bucket_source_sync_status(store, zone, z.second, c->second,
                                info, width, out);
    }
  }
  return 0;
}

static void parse_tier_config_param(const string& s, map<string, string, ltstr_nocase>& out)
{
  int level = 0;
  string cur_conf;
  list<string> confs;
  for (auto c : s) {
    if (c == ',') {
      if (level == 0) {
        confs.push_back(cur_conf);
        cur_conf.clear();
        continue;
      }
    }
    if (c == '{') {
      ++level;
    } else if (c == '}') {
      --level;
    }
    cur_conf += c;
  }
  if (!cur_conf.empty()) {
    confs.push_back(cur_conf);
  }

  for (auto c : confs) {
    ssize_t pos = c.find("=");
    if (pos < 0) {
      out[c] = "";
    } else {
      out[c.substr(0, pos)] = c.substr(pos + 1);
    }
  }
}

static int check_pool_support_omap(const rgw_pool& pool)
{
  librados::IoCtx io_ctx;
  int ret = store->get_rados_handle()->ioctx_create(pool.to_str().c_str(), io_ctx);
  if (ret < 0) {
     // the pool may not exist at this moment, we have no way to check if it supports omap.
     return 0;
  }

  ret = io_ctx.omap_clear("__omap_test_not_exist_oid__");
  if (ret == -EOPNOTSUPP) {
    io_ctx.close();
    return ret;
  }
  io_ctx.close();
  return 0;
}

int check_reshard_bucket_params(RGWRados *store,
				const string& bucket_name,
				const string& tenant,
				const string& bucket_id,
				bool num_shards_specified,
				int num_shards,
				int yes_i_really_mean_it,
				rgw_bucket& bucket,
				RGWBucketInfo& bucket_info,
				map<string, bufferlist>& attrs)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return -EINVAL;
  }

  if (!num_shards_specified) {
    cerr << "ERROR: --num-shards not specified" << std::endl;
    return -EINVAL;
  }

  if (num_shards > (int)store->get_max_bucket_shards()) {
    cerr << "ERROR: num_shards too high, max value: " << store->get_max_bucket_shards() << std::endl;
    return -EINVAL;
  }

  int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  if (num_shards <= num_source_shards && !yes_i_really_mean_it) {
    cerr << "num shards is less or equal to current shards count" << std::endl
	 << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int create_new_bucket_instance(RGWRados *store,
			       int new_num_shards,
			       const RGWBucketInfo& bucket_info,
			       map<string, bufferlist>& attrs,
			       RGWBucketInfo& new_bucket_info)
{

  store->create_bucket_id(&new_bucket_info.bucket.bucket_id);
  new_bucket_info.bucket.oid.clear();

  new_bucket_info.num_shards = new_num_shards;
  new_bucket_info.objv_tracker.clear();

  int ret = store->init_bucket_index(new_bucket_info, new_bucket_info.num_shards);
  if (ret < 0) {
    cerr << "ERROR: failed to init new bucket indexes: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  ret = store->put_bucket_instance_info(new_bucket_info, true, real_time(), &attrs);
  if (ret < 0) {
    cerr << "ERROR: failed to store new bucket instance info: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  return 0;
}

static int scan_totp(CephContext *cct, ceph::real_time& now, rados::cls::otp::otp_info_t& totp, vector<string>& pins,
                     time_t *pofs)
{
#define MAX_TOTP_SKEW_HOURS (24 * 7)
  ceph_assert(pins.size() == 2);

  time_t start_time = ceph::real_clock::to_time_t(now);
  time_t time_ofs = 0, time_ofs_abs = 0;
  time_t step_size = totp.step_size;
  if (step_size == 0) {
    step_size = OATH_TOTP_DEFAULT_TIME_STEP_SIZE;
  }
  uint32_t count = 0;
  int sign = 1;

  uint32_t max_skew = MAX_TOTP_SKEW_HOURS * 3600;

  while (time_ofs_abs < max_skew) {
    int rc = oath_totp_validate2(totp.seed_bin.c_str(), totp.seed_bin.length(),
                             start_time, 
                             step_size,
                             time_ofs,
                             1,
                             nullptr,
                             pins[0].c_str());
    if (rc != OATH_INVALID_OTP) {
      rc = oath_totp_validate2(totp.seed_bin.c_str(), totp.seed_bin.length(),
                               start_time, 
                               step_size,
                               time_ofs - step_size, /* smaller time_ofs moves time forward */
                               1,
                               nullptr,
                               pins[1].c_str());
      if (rc != OATH_INVALID_OTP) {
        *pofs = time_ofs - step_size + step_size * totp.window / 2;
        ldout(cct, 20) << "found at time=" << start_time - time_ofs << " time_ofs=" << time_ofs << dendl;
        return 0;
      }
    }
    sign = -sign;
    time_ofs_abs = (++count) * step_size;
    time_ofs = sign * time_ofs_abs;
  }

  return -ENOENT;
}

static int trim_sync_error_log(int shard_id, const ceph::real_time& start_time,
                               const ceph::real_time& end_time,
                               const string& start_marker, const string& end_marker,
                               int delay_ms)
{
  auto oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX,
                                               shard_id);
  // call cls_log_trim() until it returns -ENODATA
  for (;;) {
    int ret = store->time_log_trim(oid, start_time, end_time,
                                   start_marker, end_marker);
    if (ret == -ENODATA) {
      return 0;
    }
    if (ret < 0) {
      return ret;
    }
    if (delay_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
  }
  // unreachable
}

const string& get_tier_type(RGWRados *store) {
  return store->svc.zone->get_zone().tier_type;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);

  // for region -> zonegroup conversion (must happen before common_init_finish())
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }

  common_init_finish(g_ceph_context);

  rgw_user user_id;
  string tenant;
  std::string access_key, secret_key, user_email, display_name;
  std::string bucket_name, pool_name, object;
  rgw_pool pool;
  std::string date, subuser, access, format;
  std::string start_date, end_date;
  std::string key_type_str;
  std::string period_id, period_epoch, remote, url;
  std::string master_zone;
  std::string realm_name, realm_id, realm_new_name;
  std::string zone_name, zone_id, zone_new_name;
  std::string zonegroup_name, zonegroup_id, zonegroup_new_name;
  std::string api_name;
  std::string role_name, path, assume_role_doc, policy_name, perm_policy_doc, path_prefix;
  std::string redirect_zone;
  bool redirect_zone_set = false;
  list<string> endpoints;
  int tmp_int;
  int sync_from_all_specified = false;
  bool sync_from_all = false;
  list<string> sync_from;
  list<string> sync_from_rm;
  int is_master_int;
  int set_default = 0;
  bool is_master = false;
  bool is_master_set = false;
  int read_only_int;
  bool read_only = false;
  int is_read_only_set = false;
  int commit = false;
  int staging = false;
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
  bool max_buckets_specified = false;
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
  bool max_entries_specified = false;
  int admin = false;
  bool admin_specified = false;
  int system = false;
  bool system_specified = false;
  int shard_id = -1;
  bool specified_shard_id = false;
  string client_id;
  string op_id;
  string op_mask_str;
  string quota_scope;
  string object_version;
  string placement_id;
  string storage_class;
  list<string> tags;
  list<string> tags_add;
  list<string> tags_rm;

  int64_t max_objects = -1;
  int64_t max_size = -1;
  bool have_max_objects = false;
  bool have_max_size = false;
  int include_all = false;

  int sync_stats = false;
  int reset_stats = false;
  int bypass_gc = false;
  int warnings_only = false;
  int inconsistent_index = false;

  int verbose = false;

  int extra_info = false;

  uint64_t min_rewrite_size = 4 * 1024 * 1024;
  uint64_t max_rewrite_size = ULLONG_MAX;
  uint64_t min_rewrite_stripe_size = 0;

  BIIndexType bi_index_type = BIIndexType::Plain;

  string job_id;
  int num_shards = 0;
  bool num_shards_specified = false;
  int max_concurrent_ios = 32;
  uint64_t orphan_stale_secs = (24 * 3600);

  std::string val;
  std::ostringstream errs;
  string err;

  string source_zone_name;
  string source_zone; /* zone id */

  string tier_type;
  bool tier_type_specified = false;

  map<string, string, ltstr_nocase> tier_config_add;
  map<string, string, ltstr_nocase> tier_config_rm;

  boost::optional<string> index_pool;
  boost::optional<string> data_pool;
  boost::optional<string> data_extra_pool;
  RGWBucketIndexType placement_index_type = RGWBIType_Normal;
  bool index_type_specified = false;

  boost::optional<std::string> compression_type;

  string totp_serial;
  string totp_seed;
  string totp_seed_type = "hex";
  vector<string> totp_pin;
  int totp_seconds = 0;
  int totp_window = 0;
  int trim_delay_ms = 0;

  string topic_name;
  string sub_name;
  string sub_oid_prefix;
  string sub_dest_bucket;
  string sub_push_endpoint;
  string event_id;
  set<string, ltstr_nocase> event_types;

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
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
      user_op.user_email_specified=true;
    } else if (ceph_argparse_witharg(args, i, &val, "-n", "--display-name", (char*)NULL)) {
      display_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", "--bucket", (char*)NULL)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      pool_name = val;
      pool = rgw_pool(pool_name);
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--object", (char*)NULL)) {
      object = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--object-version", (char*)NULL)) {
      object_version = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--client-id", (char*)NULL)) {
      client_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--op-id", (char*)NULL)) {
      op_id = val;
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
        exit(1);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--job-id", (char*)NULL)) {
      job_id = val;
    } else if (ceph_argparse_binary_flag(args, i, &gen_access_key, NULL, "--gen-access-key", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &gen_secret_key, NULL, "--gen-secret", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &show_log_entries, NULL, "--show-log-entries", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &show_log_sum, NULL, "--show-log-sum", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &skip_zero_entries, NULL, "--skip-zero-entries", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &admin, NULL, "--admin", (char*)NULL)) {
      admin_specified = true;
    } else if (ceph_argparse_binary_flag(args, i, &system, NULL, "--system", (char*)NULL)) {
      system_specified = true;
    } else if (ceph_argparse_binary_flag(args, i, &verbose, NULL, "--verbose", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &staging, NULL, "--staging", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &commit, NULL, "--commit", (char*)NULL)) {
      // do nothing
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
      max_buckets_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-entries", (char*)NULL)) {
      max_entries = (int)strict_strtol(val.c_str(), 10, &err);
      max_entries_specified = true;
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max entries: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-size", (char*)NULL)) {
      max_size = strict_iec_cast<long long>(val.c_str(), &err);
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
      num_shards_specified = true;
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
        exit(1);
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
    } else if (ceph_argparse_binary_flag(args, i, &reset_stats, NULL, "--reset-stats", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &include_all, NULL, "--include-all", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &extra_info, NULL, "--extra-info", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &bypass_gc, NULL, "--bypass-gc", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &warnings_only, NULL, "--warnings-only", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &inconsistent_index, NULL, "--inconsistent-index", (char*)NULL)) {
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
    } else if (ceph_argparse_witharg(args, i, &val, "--index-type", (char*)NULL)) {
      string index_type_str = val;
      bi_index_type = get_bi_index_type(index_type_str);
      if (bi_index_type == BIIndexType::Invalid) {
        cerr << "ERROR: invalid bucket index entry type" << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_binary_flag(args, i, &is_master_int, NULL, "--master", (char*)NULL)) {
      is_master = (bool)is_master_int;
      is_master_set = true;
    } else if (ceph_argparse_binary_flag(args, i, &set_default, NULL, "--default", (char*)NULL)) {
      /* do nothing */
    } else if (ceph_argparse_witharg(args, i, &val, "--redirect-zone", (char*)NULL)) {
      redirect_zone = val;
      redirect_zone_set = true;
    } else if (ceph_argparse_binary_flag(args, i, &read_only_int, NULL, "--read-only", (char*)NULL)) {
      read_only = (bool)read_only_int;
      is_read_only_set = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--master-zone", (char*)NULL)) {
      master_zone = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--period", (char*)NULL)) {
      period_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--epoch", (char*)NULL)) {
      period_epoch = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--remote", (char*)NULL)) {
      remote = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--url", (char*)NULL)) {
      url = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--realm-id", (char*)NULL)) {
      realm_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--realm-new-name", (char*)NULL)) {
      realm_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zonegroup-id", (char*)NULL)) {
      zonegroup_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zonegroup-new-name", (char*)NULL)) {
      zonegroup_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--placement-id", (char*)NULL)) {
      placement_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--storage-class", (char*)NULL)) {
      storage_class = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--tags", (char*)NULL)) {
      get_str_list(val, tags);
    } else if (ceph_argparse_witharg(args, i, &val, "--tags-add", (char*)NULL)) {
      get_str_list(val, tags_add);
    } else if (ceph_argparse_witharg(args, i, &val, "--tags-rm", (char*)NULL)) {
      get_str_list(val, tags_rm);
    } else if (ceph_argparse_witharg(args, i, &val, "--api-name", (char*)NULL)) {
      api_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zone-id", (char*)NULL)) {
      zone_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zone-new-name", (char*)NULL)) {
      zone_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--endpoints", (char*)NULL)) {
      get_str_list(val, endpoints);
    } else if (ceph_argparse_witharg(args, i, &val, "--sync-from", (char*)NULL)) {
      get_str_list(val, sync_from);
    } else if (ceph_argparse_witharg(args, i, &val, "--sync-from-rm", (char*)NULL)) {
      get_str_list(val, sync_from_rm);
    } else if (ceph_argparse_binary_flag(args, i, &tmp_int, NULL, "--sync-from-all", (char*)NULL)) {
      sync_from_all = (bool)tmp_int;
      sync_from_all_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--source-zone", (char*)NULL)) {
      source_zone_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--tier-type", (char*)NULL)) {
      tier_type = val;
      tier_type_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--tier-config", (char*)NULL)) {
      parse_tier_config_param(val, tier_config_add);
    } else if (ceph_argparse_witharg(args, i, &val, "--tier-config-rm", (char*)NULL)) {
      parse_tier_config_param(val, tier_config_rm);
    } else if (ceph_argparse_witharg(args, i, &val, "--index-pool", (char*)NULL)) {
      index_pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--data-pool", (char*)NULL)) {
      data_pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--data-extra-pool", (char*)NULL)) {
      data_extra_pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--placement-index-type", (char*)NULL)) {
      if (val == "normal") {
        placement_index_type = RGWBIType_Normal;
      } else if (val == "indexless") {
        placement_index_type = RGWBIType_Indexless;
      } else {
        placement_index_type = (RGWBucketIndexType)strict_strtol(val.c_str(), 10, &err);
        if (!err.empty()) {
          cerr << "ERROR: failed to parse index type index: " << err << std::endl;
          return EINVAL;
        }
      }
      index_type_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--compression", (char*)NULL)) {
      compression_type = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--role-name", (char*)NULL)) {
      role_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--path", (char*)NULL)) {
      path = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--assume-role-policy-doc", (char*)NULL)) {
      assume_role_doc = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--policy-name", (char*)NULL)) {
      policy_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--policy-doc", (char*)NULL)) {
      perm_policy_doc = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--path-prefix", (char*)NULL)) {
      path_prefix = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-serial", (char*)NULL)) {
      totp_serial = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-pin", (char*)NULL)) {
      totp_pin.push_back(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-seed", (char*)NULL)) {
      totp_seed = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-seed-type", (char*)NULL)) {
      totp_seed_type = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-seconds", (char*)NULL)) {
      totp_seconds = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-window", (char*)NULL)) {
      totp_window = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--trim-delay-ms", (char*)NULL)) {
      trim_delay_ms = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--topic", (char*)NULL)) {
      topic_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--sub-name", (char*)NULL)) {
      sub_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--sub-oid-prefix", (char*)NULL)) {
      sub_oid_prefix = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--sub-dest-bucket", (char*)NULL)) {
      sub_dest_bucket = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--sub-push-endpoint", (char*)NULL)) {
      sub_push_endpoint = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--event-id", (char*)NULL)) {
      event_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--event-type", "--event-types", (char*)NULL)) {
      get_str_set(val, ",", event_types);
    } else if (strncmp(*i, "-", 1) == 0) {
      cerr << "ERROR: invalid flag " << *i << std::endl;
      return EINVAL;
    } else {
      ++i;
    }
  }

  if (args.empty()) {
    usage();
    exit(1);
  }
  else {
    const char *prev_cmd = NULL;
    const char *prev_prev_cmd = NULL;
    std::vector<const char*>::iterator i ;
    for (i = args.begin(); i != args.end(); ++i) {
      opt_cmd = get_cmd(*i, prev_cmd, prev_prev_cmd, &need_more);
      if (opt_cmd < 0) {
	cerr << "unrecognized arg " << *i << std::endl;
	exit(1);
      }
      if (!need_more) {
	++i;
	break;
      }
      prev_prev_cmd = prev_cmd;
      prev_cmd = *i;
    }

    if (opt_cmd == OPT_NO_CMD) {
      cerr << "no command" << std::endl;
      exit(1);
    }

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

    if (tenant.empty()) {
      tenant = user_id.tenant;
    } else {
      if (user_id.empty() && opt_cmd != OPT_ROLE_CREATE
                          && opt_cmd != OPT_ROLE_DELETE
                          && opt_cmd != OPT_ROLE_GET
                          && opt_cmd != OPT_ROLE_MODIFY
                          && opt_cmd != OPT_ROLE_LIST
                          && opt_cmd != OPT_ROLE_POLICY_PUT
                          && opt_cmd != OPT_ROLE_POLICY_LIST
                          && opt_cmd != OPT_ROLE_POLICY_GET
                          && opt_cmd != OPT_ROLE_POLICY_DELETE
                          && opt_cmd != OPT_RESHARD_ADD
                          && opt_cmd != OPT_RESHARD_CANCEL
                          && opt_cmd != OPT_RESHARD_STATUS) {
        cerr << "ERROR: --tenant is set, but there's no user ID" << std::endl;
        return EINVAL;
      }
      user_id.tenant = tenant;
    }
    /* check key parameter conflict */
    if ((!access_key.empty()) && gen_access_key) {
        cerr << "ERROR: key parameter conflict, --access-key & --gen-access-key" << std::endl;
        return EINVAL;
    }
    if ((!secret_key.empty()) && gen_secret_key) {
        cerr << "ERROR: key parameter conflict, --secret & --gen-secret" << std::endl;
        return EINVAL;
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
    exit(1);
  }

  realm_name = g_conf()->rgw_realm;
  zone_name = g_conf()->rgw_zone;
  zonegroup_name = g_conf()->rgw_zonegroup;

  RGWStreamFlusher f(formatter, cout);

  // not a raw op if 'period update' needs to commit to master
  bool raw_period_update = opt_cmd == OPT_PERIOD_UPDATE && !commit;
  std::set<int> raw_storage_ops_list = {OPT_ZONEGROUP_ADD, OPT_ZONEGROUP_CREATE, OPT_ZONEGROUP_DELETE,
			 OPT_ZONEGROUP_GET, OPT_ZONEGROUP_LIST,
                         OPT_ZONEGROUP_SET, OPT_ZONEGROUP_DEFAULT,
			 OPT_ZONEGROUP_RENAME, OPT_ZONEGROUP_MODIFY,
			 OPT_ZONEGROUP_REMOVE,
			 OPT_ZONEGROUP_PLACEMENT_ADD, OPT_ZONEGROUP_PLACEMENT_RM,
			 OPT_ZONEGROUP_PLACEMENT_MODIFY, OPT_ZONEGROUP_PLACEMENT_LIST,
			 OPT_ZONEGROUP_PLACEMENT_DEFAULT,
			 OPT_ZONE_CREATE, OPT_ZONE_DELETE,
                         OPT_ZONE_GET, OPT_ZONE_SET, OPT_ZONE_RENAME,
                         OPT_ZONE_LIST, OPT_ZONE_MODIFY, OPT_ZONE_DEFAULT,
			 OPT_ZONE_PLACEMENT_ADD, OPT_ZONE_PLACEMENT_RM,
			 OPT_ZONE_PLACEMENT_MODIFY, OPT_ZONE_PLACEMENT_LIST,
			 OPT_REALM_CREATE,
			 OPT_PERIOD_DELETE, OPT_PERIOD_GET,
			 OPT_PERIOD_PULL,
			 OPT_PERIOD_GET_CURRENT, OPT_PERIOD_LIST,
			 OPT_GLOBAL_QUOTA_GET, OPT_GLOBAL_QUOTA_SET,
			 OPT_GLOBAL_QUOTA_ENABLE, OPT_GLOBAL_QUOTA_DISABLE,
			 OPT_REALM_DELETE, OPT_REALM_GET, OPT_REALM_LIST,
			 OPT_REALM_LIST_PERIODS,
			 OPT_REALM_GET_DEFAULT,
			 OPT_REALM_RENAME, OPT_REALM_SET,
			 OPT_REALM_DEFAULT, OPT_REALM_PULL};

  std::set<int> readonly_ops_list = {
                         OPT_USER_INFO,
			 OPT_USER_STATS,
			 OPT_BUCKETS_LIST,
			 OPT_BUCKET_LIMIT_CHECK,
			 OPT_BUCKET_STATS,
			 OPT_BUCKET_SYNC_STATUS,
			 OPT_BUCKET_SYNC_MARKERS,
			 OPT_LOG_LIST,
			 OPT_LOG_SHOW,
			 OPT_USAGE_SHOW,
			 OPT_OBJECT_STAT,
			 OPT_BI_GET,
			 OPT_BI_LIST,
			 OPT_OLH_GET,
			 OPT_OLH_READLOG,
			 OPT_GC_LIST,
			 OPT_LC_LIST,
			 OPT_ORPHANS_LIST_JOBS,
			 OPT_ZONEGROUP_GET,
			 OPT_ZONEGROUP_LIST,
			 OPT_ZONEGROUP_PLACEMENT_LIST,
			 OPT_ZONE_GET,
			 OPT_ZONE_LIST,
			 OPT_ZONE_PLACEMENT_LIST,
			 OPT_METADATA_GET,
			 OPT_METADATA_LIST,
			 OPT_METADATA_SYNC_STATUS,
			 OPT_MDLOG_LIST,
			 OPT_MDLOG_STATUS,
			 OPT_SYNC_ERROR_LIST,
			 OPT_BILOG_LIST,
			 OPT_BILOG_STATUS,
			 OPT_DATA_SYNC_STATUS,
			 OPT_DATALOG_LIST,
			 OPT_DATALOG_STATUS,
			 OPT_REALM_GET,
			 OPT_REALM_GET_DEFAULT,
			 OPT_REALM_LIST,
			 OPT_REALM_LIST_PERIODS,
			 OPT_PERIOD_GET,
			 OPT_PERIOD_GET_CURRENT,
			 OPT_PERIOD_LIST,
			 OPT_GLOBAL_QUOTA_GET,
			 OPT_SYNC_STATUS,
			 OPT_ROLE_GET,
			 OPT_ROLE_LIST,
			 OPT_ROLE_POLICY_LIST,
			 OPT_ROLE_POLICY_GET,
			 OPT_RESHARD_LIST,
			 OPT_RESHARD_STATUS,
  };

  bool raw_storage_op = (raw_storage_ops_list.find(opt_cmd) != raw_storage_ops_list.end() ||
                         raw_period_update);
  bool need_cache = readonly_ops_list.find(opt_cmd) == readonly_ops_list.end();

  if (raw_storage_op) {
    store = RGWStoreManager::get_raw_storage(g_ceph_context);
  } else {
    store = RGWStoreManager::get_storage(g_ceph_context, false, false, false, false, false,
      need_cache && g_conf()->rgw_cache_enabled);
  }
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  if (!source_zone_name.empty()) {
    if (!store->svc.zone->find_zone_id_by_name(source_zone_name, &source_zone)) {
      cerr << "WARNING: cannot find source zone id for name=" << source_zone_name << std::endl;
      source_zone = source_zone_name;
    }
  }

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);
  rgw_otp_init(store);

  rgw_http_client_init(g_ceph_context);

  struct rgw_curl_setup {
    rgw_curl_setup() {
      rgw::curl::setup_curl(boost::none);
    }
    ~rgw_curl_setup() {
      rgw::curl::cleanup_curl();
    }
  } curl_cleanup;

  oath_init();

  StoreDestructor store_destructor(store);

  if (raw_storage_op) {
    switch (opt_cmd) {
    case OPT_PERIOD_DELETE:
      {
	if (period_id.empty()) {
	  cerr << "missing period id" << std::endl;
	  return EINVAL;
	}
	RGWPeriod period(period_id);
	int ret = period.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "period.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = period.delete_obj();
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT_PERIOD_GET:
      {
	epoch_t epoch = 0;
	if (!period_epoch.empty()) {
	  epoch = atoi(period_epoch.c_str());
	}
        if (staging) {
          RGWRealm realm(realm_id, realm_name);
          int ret = realm.init(g_ceph_context, store->svc.sysobj);
          if (ret < 0 ) {
            cerr << "Error initializing realm " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          realm_id = realm.get_id();
          realm_name = realm.get_name();
          period_id = RGWPeriod::get_staging_id(realm_id);
          epoch = 1;
        }
	RGWPeriod period(period_id, epoch);
	int ret = period.init(g_ceph_context, store->svc.sysobj, realm_id, realm_name);
	if (ret < 0) {
	  cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("period", period, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_PERIOD_GET_CURRENT:
      {
        int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
	if (ret < 0) {
	  return -ret;
	}
	formatter->open_object_section("period_get_current");
	encode_json("current_period", period_id, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_PERIOD_LIST:
      {
	list<string> periods;
	int ret = store->svc.zone->list_periods(periods);
	if (ret < 0) {
	  cerr << "failed to list periods: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("periods_list");
	encode_json("periods", periods, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_PERIOD_UPDATE:
      {
        int ret = update_period(realm_id, realm_name, period_id, period_epoch,
                                commit, remote, url, access_key, secret_key,
                                formatter, yes_i_really_mean_it);
	if (ret < 0) {
	  return -ret;
	}
      }
      break;
    case OPT_PERIOD_PULL:
      {
        boost::optional<RGWRESTConn> conn;
        RGWRESTConn *remote_conn = nullptr;
        if (url.empty()) {
          // load current period for endpoints
          RGWRealm realm(realm_id, realm_name);
          int ret = realm.init(g_ceph_context, store->svc.sysobj);
          if (ret < 0) {
            cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          RGWPeriod current_period(realm.get_current_period());
          ret = current_period.init(g_ceph_context, store->svc.sysobj);
          if (ret < 0) {
            cerr << "failed to init current period: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (remote.empty()) {
            // use realm master zone as remote
            remote = current_period.get_master_zone();
          }
          conn = get_remote_conn(store, current_period.get_map(), remote);
          if (!conn) {
            cerr << "failed to find a zone or zonegroup for remote "
                << remote << std::endl;
            return -ENOENT;
          }
          remote_conn = &*conn;
        }

        RGWPeriod period;
        int ret = do_period_pull(remote_conn, url, access_key, secret_key,
                                 realm_id, realm_name, period_id, period_epoch,
                                 &period);
        if (ret < 0) {
          cerr << "period pull failed: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("period", period, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_GLOBAL_QUOTA_GET:
    case OPT_GLOBAL_QUOTA_SET:
    case OPT_GLOBAL_QUOTA_ENABLE:
    case OPT_GLOBAL_QUOTA_DISABLE:
      {
        if (realm_id.empty()) {
          RGWRealm realm(g_ceph_context, store->svc.sysobj);
          if (!realm_name.empty()) {
            // look up realm_id for the given realm_name
            int ret = realm.read_id(realm_name, realm_id);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default realm_id when none is given
            int ret = realm.read_default_id(realm_id);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = period_config.read(store->svc.sysobj, realm_id);
        if (ret < 0 && ret != -ENOENT) {
          cerr << "ERROR: failed to read period config: "
              << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        formatter->open_object_section("period_config");
        if (quota_scope == "bucket") {
          set_quota_info(period_config.bucket_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("bucket quota", period_config.bucket_quota, formatter);
        } else if (quota_scope == "user") {
          set_quota_info(period_config.user_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("user quota", period_config.user_quota, formatter);
        } else if (quota_scope.empty() && opt_cmd == OPT_GLOBAL_QUOTA_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket quota", period_config.bucket_quota, formatter);
          encode_json("user quota", period_config.user_quota, formatter);
        } else {
          cerr << "ERROR: invalid quota scope specification. Please specify "
              "either --quota-scope=bucket, or --quota-scope=user" << std::endl;
          return EINVAL;
        }
        formatter->close_section();

        if (opt_cmd != OPT_GLOBAL_QUOTA_GET) {
          // write the modified period config
          ret = period_config.write(store->svc.sysobj, realm_id);
          if (ret < 0) {
            cerr << "ERROR: failed to write period config: "
                << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (!realm_id.empty()) {
            cout << "Global quota changes saved. Use 'period update' to apply "
                "them to the staging period, and 'period commit' to commit the "
                "new period." << std::endl;
          } else {
            cout << "Global quota changes saved. They will take effect as "
                "the gateways are restarted." << std::endl;
          }
        }

        formatter->flush(cout);
      }
      break;
    case OPT_REALM_CREATE:
      {
	if (realm_name.empty()) {
	  cerr << "missing realm name" << std::endl;
	  return EINVAL;
	}

	RGWRealm realm(realm_name, g_ceph_context, store->svc.sysobj);
	int ret = realm.create();
	if (ret < 0) {
	  cerr << "ERROR: couldn't create realm " << realm_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = realm.set_as_default();
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_REALM_DELETE:
      {
	RGWRealm realm(realm_id, realm_name);
	if (realm_name.empty() && realm_id.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.delete_obj();
	if (ret < 0) {
	  cerr << "ERROR: couldn't : " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT_REALM_GET:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  if (ret == -ENOENT && realm_name.empty() && realm_id.empty()) {
	    cerr << "missing realm name or id, or default realm not found" << std::endl;
	  } else {
	    cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
          }
	  return -ret;
	}
	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_REALM_GET_DEFAULT:
      {
	RGWRealm realm(g_ceph_context, store->svc.sysobj);
	string default_id;
	int ret = realm.read_default_id(default_id);
	if (ret == -ENOENT) {
	  cout << "No default realm is set" << std::endl;
	  return -ret;
	} else if (ret < 0) {
	  cerr << "Error reading default realm:" << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	cout << "default realm: " << default_id << std::endl;
      }
      break;
    case OPT_REALM_LIST:
      {
	RGWRealm realm(g_ceph_context, store->svc.sysobj);
	string default_id;
	int ret = realm.read_default_id(default_id);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default realm: " << cpp_strerror(-ret) << std::endl;
	}
	list<string> realms;
	ret = store->svc.zone->list_realms(realms);
	if (ret < 0) {
	  cerr << "failed to list realms: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("realms_list");
	encode_json("default_info", default_id, formatter);
	encode_json("realms", realms, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_REALM_LIST_PERIODS:
      {
        int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
	if (ret < 0) {
	  return -ret;
	}
	list<string> periods;
	ret = store->svc.zone->list_periods(period_id, periods);
	if (ret < 0) {
	  cerr << "list periods failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("realm_periods_list");
	encode_json("current_period", period_id, formatter);
	encode_json("periods", periods, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;

    case OPT_REALM_RENAME:
      {
	RGWRealm realm(realm_id, realm_name);
	if (realm_new_name.empty()) {
	  cerr << "missing realm new name" << std::endl;
	  return EINVAL;
	}
	if (realm_name.empty() && realm_id.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.rename(realm_new_name);
	if (ret < 0) {
	  cerr << "realm.rename failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        cout << "Realm name updated. Note that this change only applies to "
            "the current cluster, so this command must be run separately "
            "on each of the realm's other clusters." << std::endl;
      }
      break;
    case OPT_REALM_SET:
      {
	if (realm_id.empty() && realm_name.empty()) {
	  cerr << "no realm name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(realm_id, realm_name);
	bool new_realm = false;
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	} else if (ret == -ENOENT) {
	  new_realm = true;
	}
	ret = read_decode_json(infile, realm);
	if (ret < 0) {
	  return 1;
	}
	if (!realm_name.empty() && realm.get_name() != realm_name) {
	  cerr << "mismatch between --rgw-realm " << realm_name << " and json input file name " <<
	    realm.get_name() << std::endl;
	  return EINVAL;
	}
	/* new realm */
	if (new_realm) {
	  cout << "clearing period and epoch for new realm" << std::endl;
	  realm.clear_current_period_and_epoch();
	  ret = realm.create();
	  if (ret < 0) {
	    cerr << "ERROR: couldn't create new realm: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	} else {
	  ret = realm.update();
	  if (ret < 0) {
	    cerr << "ERROR: couldn't store realm info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = realm.set_as_default();
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }
	encode_json("realm", realm, formatter);
	formatter->flush(cout);
      }
      break;

    case OPT_REALM_DEFAULT:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.set_as_default();
	if (ret < 0) {
	  cerr << "failed to set realm as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_REALM_PULL:
      {
        if (url.empty()) {
          cerr << "A --url must be provided." << std::endl;
          return EINVAL;
        }
        RGWEnv env;
        req_info info(g_ceph_context, &env);
        info.method = "GET";
        info.request_uri = "/admin/realm";

        map<string, string> &params = info.args.get_params();
        if (!realm_id.empty())
          params["id"] = realm_id;
        if (!realm_name.empty())
          params["name"] = realm_name;

        bufferlist bl;
        JSONParser p;
        int ret = send_to_url(url, access_key, secret_key, info, bl, p);
        if (ret < 0) {
          cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
          if (ret == -EACCES) {
            cerr << "If the realm has been changed on the master zone, the "
                "master zone's gateway may need to be restarted to recognize "
                "this user." << std::endl;
          }
          return -ret;
        }
        RGWRealm realm;
        realm.init(g_ceph_context, store->svc.sysobj, false);
        try {
          decode_json_obj(realm, &p);
        } catch (JSONDecoder::err& e) {
          cerr << "failed to decode JSON response: " << e.message << std::endl;
          return EINVAL;
        }
        RGWPeriod period;
        auto& current_period = realm.get_current_period();
        if (!current_period.empty()) {
          // pull the latest epoch of the realm's current period
          ret = do_period_pull(nullptr, url, access_key, secret_key,
                               realm_id, realm_name, current_period, "",
                               &period);
          if (ret < 0) {
            cerr << "could not fetch period " << current_period << std::endl;
            return -ret;
          }
        }
        ret = realm.create(false);
        if (ret < 0 && ret != -EEXIST) {
          cerr << "Error storing realm " << realm.get_id() << ": "
            << cpp_strerror(ret) << std::endl;
          return -ret;
        } else if (ret ==-EEXIST) {
	  ret = realm.update();
	  if (ret < 0) {
	    cerr << "Error storing realm " << realm.get_id() << ": "
		 << cpp_strerror(ret) << std::endl;
	  }
	}

        if (set_default) {
          ret = realm.set_as_default();
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("realm", realm, formatter);
        formatter->flush(cout);
      }
      break;

    case OPT_ZONEGROUP_ADD:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id,zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to initialize zonegroup " << zonegroup_name << " id " << zonegroup_id << " :"
	       << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        if (zone.realm_id != zonegroup.realm_id) {
          zone.realm_id = zonegroup.realm_id;
          ret = zone.update();
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

        string *ptier_type = (tier_type_specified ? &tier_type : nullptr);

        for (auto a : tier_config_add) {
          int r = zone.tier_config.set(a.first, a.second);
          if (r < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
        }

        bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        ret = zonegroup.add_zone(zone,
                                 (is_master_set ? &is_master : NULL),
                                 (is_read_only_set ? &read_only : NULL),
                                 endpoints, ptier_type,
                                 psync_from_all, sync_from, sync_from_rm,
                                 predirect_zone,
				 store->svc.sync_modules->get_manager());
	if (ret < 0) {
	  cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name() << ": "
	       << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_CREATE:
      {
	if (zonegroup_name.empty()) {
	  cerr << "Missing zonegroup name" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup(zonegroup_name, is_master, g_ceph_context, store->svc.sysobj, realm.get_id(), endpoints);
        zonegroup.api_name = (api_name.empty() ? zonegroup_name : api_name);
	ret = zonegroup.create();
	if (ret < 0) {
	  cerr << "failed to create zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = zonegroup.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_DEFAULT:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zonegroup.set_as_default();
	if (ret < 0) {
	  cerr << "failed to set zonegroup as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONEGROUP_DELETE:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zonegroup.delete_obj();
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONEGROUP_GET:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_LIST:
      {
	RGWZoneGroup zonegroup;
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj, false);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	list<string> zonegroups;
	ret = store->svc.zone->list_zonegroups(zonegroups);
	if (ret < 0) {
	  cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	string default_zonegroup;
	ret = zonegroup.read_default_id(default_zonegroup);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zonegroup: " << cpp_strerror(-ret) << std::endl;
	}
	formatter->open_object_section("zonegroups_list");
	encode_json("default_info", default_zonegroup, formatter);
	encode_json("zonegroups", zonegroups, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_MODIFY:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_update = false;

        if (!master_zone.empty()) {
          zonegroup.master_zone = master_zone;
          need_update = true;
        }

	if (is_master_set) {
	  zonegroup.update_master(is_master);
          need_update = true;
        }

        if (!endpoints.empty()) {
          zonegroup.endpoints = endpoints;
          need_update = true;
        }

        if (!api_name.empty()) {
          zonegroup.api_name = api_name;
          need_update = true;
        }

        if (!realm_id.empty()) {
          zonegroup.realm_id = realm_id;
          need_update = true;
        } else if (!realm_name.empty()) {
          // get realm id from name
          RGWRealm realm{g_ceph_context, store->svc.sysobj};
          ret = realm.read_id(realm_name, zonegroup.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << realm_name << std::endl;
            return -ret;
          }
          need_update = true;
        }

        if (need_update) {
	  ret = zonegroup.update();
	  if (ret < 0) {
	    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = zonegroup.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_SET:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(g_ceph_context, store->svc.sysobj);
       bool default_realm_not_exist = (ret == -ENOENT && realm_id.empty() && realm_name.empty());

	if (ret < 0 && !default_realm_not_exist ) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
	ret = zonegroup.init(g_ceph_context, store->svc.sysobj, false);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = read_decode_json(infile, zonegroup);
	if (ret < 0) {
	  return 1;
	}
	if (zonegroup.realm_id.empty() && !default_realm_not_exist) {
	  zonegroup.realm_id = realm.get_id();
	}
	ret = zonegroup.create();
	if (ret < 0 && ret != -EEXIST) {
	  cerr << "ERROR: couldn't create zonegroup info: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	} else if (ret == -EEXIST) {
	  ret = zonegroup.update();
	  if (ret < 0) {
	    cerr << "ERROR: couldn't store zonegroup info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = zonegroup.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_REMOVE:
      {
        RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
        int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
        if (ret < 0) {
          cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        if (zone_id.empty()) {
          if (zone_name.empty()) {
            cerr << "no --zone-id or --rgw-zone name provided" << std::endl;
            return EINVAL;
          }
          // look up zone id by name
          for (auto& z : zonegroup.zones) {
            if (zone_name == z.second.name) {
              zone_id = z.second.id;
              break;
            }
          }
          if (zone_id.empty()) {
            cerr << "zone name " << zone_name << " not found in zonegroup "
                << zonegroup.get_name() << std::endl;
            return ENOENT;
          }
        }

        ret = zonegroup.remove_zone(zone_id);
        if (ret < 0) {
          cerr << "failed to remove zone: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zonegroup", zonegroup, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_RENAME:
      {
	if (zonegroup_new_name.empty()) {
	  cerr << " missing zonegroup new name" << std::endl;
	  return EINVAL;
	}
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zonegroup.rename(zonegroup_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONEGROUP_PLACEMENT_LIST:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("placement_targets", zonegroup.placement_targets, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONEGROUP_PLACEMENT_ADD:
    case OPT_ZONEGROUP_PLACEMENT_MODIFY:
    case OPT_ZONEGROUP_PLACEMENT_RM:
    case OPT_ZONEGROUP_PLACEMENT_DEFAULT:
      {
        if (placement_id.empty()) {
          cerr << "ERROR: --placement-id not specified" << std::endl;
          return EINVAL;
        }

        rgw_placement_rule rule;
        rule.from_str(placement_id);

        if (!rule.storage_class.empty() && !storage_class.empty() &&
            rule.storage_class != storage_class) {
          cerr << "ERROR: provided contradicting storage class configuration" << std::endl;
          return EINVAL;
        } else if (rule.storage_class.empty()) {
          rule.storage_class = storage_class;
        }

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_ADD ||
            opt_cmd == OPT_ZONEGROUP_PLACEMENT_MODIFY) {
          RGWZoneGroupPlacementTarget& target = zonegroup.placement_targets[placement_id];
          if (!tags.empty()) {
            target.tags.clear();
            for (auto& t : tags) {
              target.tags.insert(t);
            }
          }
          target.name = placement_id;
          for (auto& t : tags_rm) {
            target.tags.erase(t);
          }
          for (auto& t : tags_add) {
            target.tags.insert(t);
          }
          target.storage_classes.insert(rule.get_storage_class());
        } else if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_RM) {
          zonegroup.placement_targets.erase(placement_id);
        } else if (opt_cmd == OPT_ZONEGROUP_PLACEMENT_DEFAULT) {
          if (!zonegroup.placement_targets.count(placement_id)) {
            cerr << "failed to find a zonegroup placement target named '"
                << placement_id << "'" << std::endl;
            return -ENOENT;
          }
          zonegroup.default_placement = rule;
        }

        zonegroup.post_process_params();
        ret = zonegroup.update();
        if (ret < 0) {
          cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("placement_targets", zonegroup.placement_targets, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONE_CREATE:
      {
        if (zone_name.empty()) {
	  cerr << "zone name not provided" << std::endl;
	  return EINVAL;
        }
	int ret;
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	/* if the user didn't provide zonegroup info , create stand alone zone */
	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
	  ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	  if (ret < 0) {
	    cerr << "unable to initialize zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  if (realm_id.empty() && realm_name.empty()) {
	    realm_id = zonegroup.realm_id;
	  }
	}

	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(g_ceph_context, store->svc.sysobj, false);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        zone.system_key.id = access_key;
        zone.system_key.key = secret_key;
	zone.realm_id = realm_id;
        for (auto a : tier_config_add) {
          int r = zone.tier_config.set(a.first, a.second);
          if (r < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
        }

	ret = zone.create();
	if (ret < 0) {
	  cerr << "failed to create zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
          string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
          bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
          string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);
	  ret = zonegroup.add_zone(zone,
                                   (is_master_set ? &is_master : NULL),
                                   (is_read_only_set ? &read_only : NULL),
                                   endpoints,
                                   ptier_type,
                                   psync_from_all,
                                   sync_from, sync_from_rm,
                                   predirect_zone,
				   store->svc.sync_modules->get_manager());
	  if (ret < 0) {
	    cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name()
		 << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = zone.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONE_DEFAULT:
      {
	RGWZoneGroup zonegroup(zonegroup_id,zonegroup_name);
	int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
	}
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.set_as_default();
	if (ret < 0) {
	  cerr << "failed to set zone as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONE_DELETE:
      {
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        list<string> zonegroups;
	ret = store->svc.zone->list_zonegroups(zonegroups);
	if (ret < 0) {
	  cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        for (list<string>::iterator iter = zonegroups.begin(); iter != zonegroups.end(); ++iter) {
          RGWZoneGroup zonegroup(string(), *iter);
          int ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
          if (ret < 0) {
            cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
            continue;
          }
          ret = zonegroup.remove_zone(zone.get_id());
          if (ret < 0 && ret != -ENOENT) {
            cerr << "failed to remove zone " << zone_name << " from zonegroup " << zonegroup.get_name() << ": "
              << cpp_strerror(-ret) << std::endl;
          }
        }

	ret = zone.delete_obj();
	if (ret < 0) {
	  cerr << "failed to delete zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT_ZONE_GET:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("zone", zone, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONE_SET:
      {
	RGWZoneParams zone(zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj, false);
	if (ret < 0) {
	  return -ret;
	}

        ret = zone.read();
        if (ret < 0 && ret != -ENOENT) {
	  cerr << "zone.read() returned ret=" << ret << std::endl;
          return -ret;
        }

        string orig_id = zone.get_id();

	ret = read_decode_json(infile, zone);
	if (ret < 0) {
	  return 1;
	}

	if(zone.realm_id.empty()) {
	  RGWRealm realm(realm_id, realm_name);
	  int ret = realm.init(g_ceph_context, store->svc.sysobj);
	  if (ret < 0 && ret != -ENOENT) {
	    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  zone.realm_id = realm.get_id();
	}

	if( !zone_name.empty() && !zone.get_name().empty() && zone.get_name() != zone_name) {
	  cerr << "Error: zone name" << zone_name << " is different than the zone name " << zone.get_name() << " in the provided json " << std::endl;
	  return EINVAL;
	}

        if (zone.get_name().empty()) {
          zone.set_name(zone_name);
          if (zone.get_name().empty()) {
            cerr << "no zone name specified" << std::endl;
            return EINVAL;
          }
        }

        zone_name = zone.get_name();

        if (zone.get_id().empty()) {
          zone.set_id(orig_id);
        }

	if (zone.get_id().empty()) {
	  cerr << "no zone name id the json provided, assuming old format" << std::endl;
	  if (zone_name.empty()) {
	    cerr << "missing zone name"  << std::endl;
	    return EINVAL;
	  }
	  zone.set_name(zone_name);
	  zone.set_id(zone_name);
	}

	cerr << "zone id " << zone.get_id();
	ret = zone.fix_pool_names();
	if (ret < 0) {
	  cerr << "ERROR: couldn't fix zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.write(false);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create zone: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	}

        if (set_default) {
          ret = zone.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone, formatter);
	formatter->flush(cout);
      }
      break;
    case OPT_ZONE_LIST:
      {
	list<string> zones;
	int ret = store->svc.zone->list_zones(zones);
	if (ret < 0) {
	  cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneParams zone;
	ret = zone.init(g_ceph_context, store->svc.sysobj, false);
	if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	string default_zone;
	ret = zone.read_default_id(default_zone);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zone: " << cpp_strerror(-ret) << std::endl;
	}
	formatter->open_object_section("zones_list");
	encode_json("default_info", default_zone, formatter);
	encode_json("zones", zones, formatter);
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT_ZONE_MODIFY:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_zone_update = false;
        if (!access_key.empty()) {
          zone.system_key.id = access_key;
          need_zone_update = true;
        }

        if (!secret_key.empty()) {
          zone.system_key.key = secret_key;
          need_zone_update = true;
        }

        if (!realm_id.empty()) {
          zone.realm_id = realm_id;
          need_zone_update = true;
        } else if (!realm_name.empty()) {
          // get realm id from name
          RGWRealm realm{g_ceph_context, store->svc.sysobj};
          ret = realm.read_id(realm_name, zone.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << realm_name << std::endl;
            return -ret;
          }
          need_zone_update = true;
        }

        if (tier_config_add.size() > 0) {
          for (auto add : tier_config_add) {
            int r = zone.tier_config.set(add.first, add.second);
            if (r < 0) {
              cerr << "ERROR: failed to set configurable: " << add << std::endl;
              return EINVAL;
            }
          }
          need_zone_update = true;
        }

        for (auto rm : tier_config_rm) {
          if (!rm.first.empty()) { /* otherwise will remove the entire config */
            zone.tier_config.erase(rm.first);
            need_zone_update = true;
          }
        }

        if (need_zone_update) {
          ret = zone.update();
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        string *ptier_type = (tier_type_specified ? &tier_type : nullptr);

        bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        ret = zonegroup.add_zone(zone,
                                 (is_master_set ? &is_master : NULL),
                                 (is_read_only_set ? &read_only : NULL),
                                 endpoints, ptier_type,
                                 psync_from_all, sync_from, sync_from_rm,
                                 predirect_zone,
				 store->svc.sync_modules->get_manager());
	if (ret < 0) {
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zonegroup.update();
	if (ret < 0) {
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = zone.set_as_default();
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zone", zone, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONE_RENAME:
      {
	if (zone_new_name.empty()) {
	  cerr << " missing zone new name" << std::endl;
	  return EINVAL;
	}
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(zone_id,zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.rename(zone_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zone " << zone_name << " to " << zone_new_name << ": " << cpp_strerror(-ret)
	       << std::endl;
	  return -ret;
	}
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
	} else {
	  ret = zonegroup.rename_zone(zone);
	  if (ret < 0) {
	    cerr << "Error in zonegroup rename for " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}
      }
      break;
    case OPT_ZONE_PLACEMENT_ADD:
    case OPT_ZONE_PLACEMENT_MODIFY:
    case OPT_ZONE_PLACEMENT_RM:
      {
        if (placement_id.empty()) {
          cerr << "ERROR: --placement-id not specified" << std::endl;
          return EINVAL;
        }
        // validate compression type
        if (compression_type && *compression_type != "random"
            && !Compressor::get_comp_alg_type(*compression_type)) {
          std::cerr << "Unrecognized compression type" << std::endl;
          return EINVAL;
        }

	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opt_cmd == OPT_ZONE_PLACEMENT_ADD ||
	    opt_cmd == OPT_ZONE_PLACEMENT_MODIFY) {
	  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	  ret = zonegroup.init(g_ceph_context, store->svc.sysobj);
	  if (ret < 0) {
	    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }

	  auto ptiter = zonegroup.placement_targets.find(placement_id);
	  if (ptiter == zonegroup.placement_targets.end()) {
	    cerr << "ERROR: placement id '" << placement_id << "' is not configured in zonegroup placement targets" << std::endl;
	    return EINVAL;
	  }

	  storage_class = rgw_placement_rule::get_canonical_storage_class(storage_class);
	  if (ptiter->second.storage_classes.find(storage_class) == ptiter->second.storage_classes.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is not defined in zonegroup '" << placement_id << "' placement target" << std::endl;
	    return EINVAL;
	  }

          RGWZonePlacementInfo& info = zone.placement_pools[placement_id];

	  string opt_index_pool = index_pool.value_or(string());
	  string opt_data_pool = data_pool.value_or(string());

	  if (!opt_index_pool.empty()) {
	    info.index_pool = opt_index_pool;
	  }

	  if (info.index_pool.empty()) {
            cerr << "ERROR: index pool not configured, need to specify --index-pool" << std::endl;
            return EINVAL;
	  }

	  if (opt_data_pool.empty()) {
	    const RGWZoneStorageClass *porig_sc{nullptr};
	    if (info.storage_classes.find(storage_class, &porig_sc)) {
	      if (porig_sc->data_pool) {
		opt_data_pool = porig_sc->data_pool->to_str();
	      }
	    }
	    if (opt_data_pool.empty()) {
	      cerr << "ERROR: data pool not configured, need to specify --data-pool" << std::endl;
	      return EINVAL;
	    }
	  }

          rgw_pool dp = opt_data_pool;
          info.storage_classes.set_storage_class(storage_class, &dp, compression_type.get_ptr());

          if (data_extra_pool) {
            info.data_extra_pool = *data_extra_pool;
          }
          if (index_type_specified) {
	    info.index_type = placement_index_type;
          }

          ret = check_pool_support_omap(info.get_data_extra_pool());
          if (ret < 0) {
             cerr << "ERROR: the data extra (non-ec) pool '" << info.get_data_extra_pool() 
                 << "' does not support omap" << std::endl;
             return ret;
          }
        } else if (opt_cmd == OPT_ZONE_PLACEMENT_RM) {
          zone.placement_pools.erase(placement_id);
        }

        ret = zone.update();
        if (ret < 0) {
          cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zone", zone, formatter);
        formatter->flush(cout);
      }
      break;
    case OPT_ZONE_PLACEMENT_LIST:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(g_ceph_context, store->svc.sysobj);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("placement_pools", zone.placement_pools, formatter);
	formatter->flush(cout);
      }
      break;
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

  if (max_buckets_specified)
    user_op.set_max_buckets(max_buckets);

  if (admin_specified)
     user_op.set_admin(admin);

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
  bucket_op.set_fix_index(fix);
  bucket_op.set_max_aio(max_concurrent_ios);

  // required to gather errors from operations
  std::string err_msg;

  bool output_user_info = true;

  switch (opt_cmd) {
  case OPT_USER_INFO:
    if (user_id.empty()) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    break;
  case OPT_USER_CREATE:
    if (!user_op.has_existing_user()) {
      user_op.set_generate_key(); // generate a new key by default
    }
    ret = user.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      if (ret == -ERR_INVALID_TENANT_NAME)
	ret = -EINVAL;

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
    if (ret < 0) {
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
      cerr << "could not remove caps: " << err_msg << std::endl;
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
  case OPT_PERIOD_PUSH:
    {
      RGWEnv env;
      req_info info(g_ceph_context, &env);
      info.method = "POST";
      info.request_uri = "/admin/realm/period";

      map<string, string> &params = info.args.get_params();
      if (!realm_id.empty())
        params["realm_id"] = realm_id;
      if (!realm_name.empty())
        params["realm_name"] = realm_name;
      if (!period_id.empty())
        params["period_id"] = period_id;
      if (!period_epoch.empty())
        params["epoch"] = period_epoch;

      // load the period
      RGWPeriod period(period_id);
      int ret = period.init(g_ceph_context, store->svc.sysobj);
      if (ret < 0) {
        cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      // json format into a bufferlist
      JSONFormatter jf(false);
      encode_json("period", period, &jf);
      bufferlist bl;
      jf.flush(bl);

      JSONParser p;
      ret = send_to_remote_or_url(nullptr, url, access_key, secret_key,
                                  info, bl, p);
      if (ret < 0) {
        cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    return 0;
  case OPT_PERIOD_UPDATE:
    {
      int ret = update_period(realm_id, realm_name, period_id, period_epoch,
                              commit, remote, url, access_key, secret_key,
                              formatter, yes_i_really_mean_it);
      if (ret < 0) {
	return -ret;
      }
    }
    return 0;
  case OPT_PERIOD_COMMIT:
    {
      // read realm and staging period
      RGWRealm realm(realm_id, realm_name);
      int ret = realm.init(g_ceph_context, store->svc.sysobj);
      if (ret < 0) {
        cerr << "Error initializing realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      RGWPeriod period(RGWPeriod::get_staging_id(realm.get_id()), 1);
      ret = period.init(g_ceph_context, store->svc.sysobj, realm.get_id());
      if (ret < 0) {
        cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      ret = commit_period(realm, period, remote, url, access_key, secret_key,
                          yes_i_really_mean_it);
      if (ret < 0) {
        cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("period", period, formatter);
      formatter->flush(cout);
    }
    return 0;
  case OPT_ROLE_CREATE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (assume_role_doc.empty()) {
        cerr << "ERROR: assume role policy document is empty" << std::endl;
        return -EINVAL;
      }
      bufferlist bl = bufferlist::static_from_string(assume_role_doc);
      try {
        const rgw::IAM::Policy p(g_ceph_context, tenant, bl);
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse policy: " << e.what() << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, path, assume_role_doc, tenant);
      ret = role.create(true);
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role, formatter);
      return 0;
    }
  case OPT_ROLE_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.delete_obj();
      if (ret < 0) {
        return -ret;
      }
      cout << "role: " << role_name << " successfully deleted" << std::endl;
      return 0;
    }
  case OPT_ROLE_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role, formatter);
      return 0;
    }
  case OPT_ROLE_MODIFY:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (assume_role_doc.empty()) {
        cerr << "ERROR: assume role policy document is empty" << std::endl;
        return -EINVAL;
      }

      bufferlist bl = bufferlist::static_from_string(assume_role_doc);
      try {
        const rgw::IAM::Policy p(g_ceph_context, tenant, bl);
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse policy: " << e.what() << std::endl;
        return -EINVAL;
      }

      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      role.update_trust_policy(assume_role_doc);
      ret = role.update();
      if (ret < 0) {
        return -ret;
      }
      cout << "Assume role policy document updated successfully for role: " << role_name << std::endl;
      return 0;
    }
  case OPT_ROLE_LIST:
    {
      vector<RGWRole> result;
      ret = RGWRole::get_roles_by_path_prefix(store, g_ceph_context, path_prefix, tenant, result);
      if (ret < 0) {
        return -ret;
      }
      show_roles_info(result, formatter);
      return 0;
    }
  case OPT_ROLE_POLICY_PUT:
    {
      if (role_name.empty()) {
        cerr << "role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "policy name is empty" << std::endl;
        return -EINVAL;
      }

      if (perm_policy_doc.empty()) {
        cerr << "permission policy document is empty" << std::endl;
        return -EINVAL;
      }

      bufferlist bl = bufferlist::static_from_string(perm_policy_doc);
      try {
        const rgw::IAM::Policy p(g_ceph_context, tenant, bl);
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse perm policy: " << e.what() << std::endl;
        return -EINVAL;
      }

      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      role.set_perm_policy(policy_name, perm_policy_doc);
      ret = role.update();
      if (ret < 0) {
        return -ret;
      }
      cout << "Permission policy attached successfully" << std::endl;
      return 0;
    }
  case OPT_ROLE_POLICY_LIST:
    {
      if (role_name.empty()) {
        cerr << "ERROR: Role name is empty" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      std::vector<string> policy_names = role.get_role_policy_names();
      show_policy_names(policy_names, formatter);
      return 0;
    }
  case OPT_ROLE_POLICY_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      int ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      string perm_policy;
      ret = role.get_role_policy(policy_name, perm_policy);
      if (ret < 0) {
        return -ret;
      }
      show_perm_policy(perm_policy, formatter);
      return 0;
    }
  case OPT_ROLE_POLICY_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      RGWRole role(g_ceph_context, store, role_name, tenant);
      ret = role.get();
      if (ret < 0) {
        return -ret;
      }
      ret = role.delete_policy(policy_name);
      if (ret < 0) {
        return -ret;
      }
      ret = role.update();
      if (ret < 0) {
        return -ret;
      }
      cout << "Policy: " << policy_name << " successfully deleted for role: "
           << role_name << std::endl;
      return 0;
  }
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
    if (format == "xml") {
      int ret = RGWBucketAdminOp::dump_s3_policy(store, bucket_op, cout);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      int ret = RGWBucketAdminOp::get_policy(store, bucket_op, f);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  if (opt_cmd == OPT_BUCKET_LIMIT_CHECK) {
    void *handle;
    std::list<std::string> user_ids;
    metadata_key = "user";
    int max = 1000;

    bool truncated;

    if (! user_id.empty()) {
      user_ids.push_back(user_id.id);
      ret =
	RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, f,
	  warnings_only);
    } else {
      /* list users in groups of max-keys, then perform user-bucket
       * limit-check on each group */
     ret = store->meta_mgr->list_keys_init(metadata_key, &handle);
      if (ret < 0) {
	cerr << "ERROR: buckets limit check can't get user metadata_key: "
	     << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      do {
	ret = store->meta_mgr->list_keys_next(handle, max, user_ids,
					      &truncated);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "ERROR: buckets limit check lists_keys_next(): "
	       << cpp_strerror(-ret) << std::endl;
	  break;
	} else {
	  /* ok, do the limit checks for this group */
	  ret =
	    RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, f,
	      warnings_only);
	  if (ret < 0)
	    break;
	}
	user_ids.clear();
      } while (truncated);
      store->meta_mgr->list_keys_complete(handle);
    }
    return -ret;
  } /* OPT_BUCKET_LIMIT_CHECK */

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
      vector<rgw_bucket_dir_entry> result;
      map<string, bool> common_prefixes;
      string ns;

      RGWRados::Bucket target(store, bucket_info);
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

        for (vector<rgw_bucket_dir_entry>::iterator iter = result.begin(); iter != result.end(); ++iter) {
          rgw_bucket_dir_entry& entry = *iter;
          encode_json("entry", entry, formatter);
        }
        formatter->flush(cout);
      } while (truncated && count < max_entries);

      formatter->close_section();
      formatter->flush(cout);
    } /* have bucket_name */
  } /* OPT_BUCKETS_LIST */

  if (opt_cmd == OPT_BUCKET_STATS) {
    bucket_op.set_fetch_stats(true);

    int r = RGWBucketAdminOp::info(store, bucket_op, f);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
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
      return EINVAL;
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
        return -r;
      }
      while (true) {
        string name;
        int r = store->log_list_next(h, &name);
        if (r == -ENOENT)
          break;
        if (r < 0) {
          cerr << "log list: error " << r << std::endl;
          return -r;
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
      exit(1);
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
        using namespace std::chrono;
        uint64_t total_time = duration_cast<milliseconds>(entry.total_time).count();

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
      exit(1);
    }

    int ret = store->svc.zone->add_bucket_placement(pool);
    if (ret < 0)
      cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOL_RM) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to remove!" << std::endl;
      exit(1);
    }

    int ret = store->svc.zone->remove_bucket_placement(pool);
    if (ret < 0)
      cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT_POOLS_LIST) {
    set<rgw_pool> pools;
    int ret = store->svc.zone->list_placement_set(pools);
    if (ret < 0) {
      cerr << "could not list placement set: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->reset();
    formatter->open_array_section("pools");
    for (auto siter = pools.begin(); siter != pools.end(); ++siter) {
      formatter->open_object_section("pool");
      formatter->dump_string("name",  siter->to_str());
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


    ret = RGWUsage::show(store, user_id, bucket_name, start_epoch, end_epoch,
			 show_log_entries, show_log_sum, &categories,
			 f);
    if (ret < 0) {
      cerr << "ERROR: failed to show usage" << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_USAGE_TRIM) {
    if (user_id.empty() && bucket_name.empty() &&
	start_date.empty() && end_date.empty() && !yes_i_really_mean_it) {
      cerr << "usage trim without user/date/bucket specified will remove *all* users data" << std::endl;
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

    ret = RGWUsage::trim(store, user_id, bucket_name, start_epoch, end_epoch);
    if (ret < 0) {
      cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_USAGE_CLEAR) {
    if (!yes_i_really_mean_it) {
      cerr << "usage clear would remove *all* users usage data for all time" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }

    ret = RGWUsage::clear(store);
    if (ret < 0) {
      return ret;
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
  }

  if (opt_cmd == OPT_OLH_GET) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWOLHInfo olh;
    rgw_obj obj(bucket, object);
    ret = store->get_olh(bucket_info, obj, &olh);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("olh", olh, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_OLH_READLOG) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
    bool is_truncated;

    RGWObjectCtx rctx(store);
    rgw_obj obj(bucket, object);

    RGWObjState *state;

    ret = store->get_obj_state(&rctx, bucket_info, obj, &state, false); /* don't follow olh */
    if (ret < 0) {
      return -ret;
    }

    ret = store->bucket_index_read_olh_log(bucket_info, *state, obj, 0, &log, &is_truncated);
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
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
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
    if (!object_version.empty()) {
      obj.key.set_instance(object_version);
    }

    rgw_cls_bi_entry entry;

    ret = store->bi_get(bucket_info, obj, bi_index_type, &entry);
    if (ret < 0) {
      cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("entry", entry, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_PUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
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

    rgw_obj obj(bucket, key);

    ret = store->bi_put(bucket, obj, entry);
    if (ret < 0) {
      cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BI_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
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

    int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

    formatter->open_array_section("entries");

    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(store);
      int shard_id = (bucket_info.num_shards > 0  ? i : -1);
      int ret = bs.init(bucket, shard_id, nullptr /* no RGWBucketInfo */);
      marker.clear();

      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      do {
        entries.clear();
        ret = store->bi_list(bs, object, marker, max_entries, &entries, &is_truncated);
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
      formatter->flush(cout);
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BI_PURGE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketInfo cur_bucket_info;
    rgw_bucket cur_bucket;
    ret = init_bucket(tenant, bucket_name, string(), cur_bucket_info, cur_bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init current bucket info for bucket_name=" << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    if (cur_bucket_info.bucket.bucket_id == bucket_info.bucket.bucket_id && !yes_i_really_mean_it) {
      cerr << "specified bucket instance points to a current bucket instance" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EINVAL;
    }

    int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(store);
      int shard_id = (bucket_info.num_shards > 0  ? i : -1);
      int ret = bs.init(bucket, shard_id, nullptr /* no RGWBucketInfo */);
      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      ret = store->bi_remove(bs);
      if (ret < 0) {
        cerr << "ERROR: failed to remove bucket index object: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  if (opt_cmd == OPT_OBJECT_PUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }

    RGWDataAccess data_access(store);
    rgw_obj_key key(object, object_version);

    RGWDataAccess::BucketRef b;
    RGWDataAccess::ObjectRef obj;

    int ret = data_access.get_bucket(tenant, bucket_name, bucket_id, &b);
    if (ret < 0) {
      cerr << "ERROR: failed to init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    ret = b->get_object(key, &obj);
    if (ret < 0) {
      cerr << "ERROR: failed to get object: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bufferlist bl;
    ret = read_input(infile, bl);
    if (ret < 0) {
      cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
    }

    map<string, bufferlist> attrs;
    ret = obj->put(bl, attrs);
    if (ret < 0) {
      cerr << "ERROR: put object returned error: " << cpp_strerror(-ret) << std::endl;
    }
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
    obj.key.set_instance(object_version);
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
    if (!store->process_expire_objects()) {
      cerr << "ERROR: process_expire_objects() processing returned error." << std::endl;
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

    rgw_obj_index_key marker;
    string prefix;

    formatter->open_object_section("result");
    formatter->dump_string("bucket", bucket_name);
    formatter->open_array_section("objects");
    while (is_truncated) {
      map<string, rgw_bucket_dir_entry> result;
      int r =
	store->cls_bucket_list_ordered(bucket_info, RGW_NO_SHARD, marker,
				       prefix, 1000, true,
				       result, &is_truncated, &marker,
				       bucket_object_check_filter);

      if (r < 0 && r != -ENOENT) {
        cerr << "ERROR: failed operation r=" << r << std::endl;
      }

      if (r == -ENOENT)
        break;

      map<string, rgw_bucket_dir_entry>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
        rgw_obj_key key = iter->second.key;
        rgw_bucket_dir_entry& entry = iter->second;

        formatter->open_object_section("object");
        formatter->dump_string("name", key.name);
        formatter->dump_string("instance", key.instance);
        formatter->dump_int("size", entry.meta.size);
        utime_t ut(entry.meta.mtime);
        ut.gmtime(formatter->dump_stream("mtime"));

        if ((entry.meta.size < min_rewrite_size) ||
            (entry.meta.size > max_rewrite_size) ||
            (start_epoch > 0 && start_epoch > (uint64_t)ut.sec()) ||
            (end_epoch > 0 && end_epoch < (uint64_t)ut.sec())) {
          formatter->dump_string("status", "Skipped");
        } else {
          rgw_obj obj(bucket, key);

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

  if (opt_cmd == OPT_BUCKET_RESHARD) {
    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;

    int ret = check_reshard_bucket_params(store,
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  bucket,
					  bucket_info,
					  attrs);
    if (ret < 0) {
      return ret;
    }

    RGWBucketReshard br(store, bucket_info, attrs, nullptr /* no callback */);

#define DEFAULT_RESHARD_MAX_ENTRIES 1000
    if (max_entries < 1) {
      max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
    }

    return br.execute(num_shards, max_entries,
                      verbose, &cout, formatter);
  }

  if (opt_cmd == OPT_RESHARD_ADD) {
    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;

    int ret = check_reshard_bucket_params(store,
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  bucket,
					  bucket_info,
					  attrs);
    if (ret < 0) {
      return ret;
    }

    int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

    RGWReshard reshard(store);
    cls_rgw_reshard_entry entry;
    entry.time = real_clock::now();
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    entry.bucket_id = bucket_info.bucket.bucket_id;
    entry.old_num_shards = num_source_shards;
    entry.new_num_shards = num_shards;

    return reshard.add(entry);
  }

  if (opt_cmd == OPT_RESHARD_LIST) {
    list<cls_rgw_reshard_entry> entries;
    int ret;
    int count = 0;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int num_logshards =
      store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");

    RGWReshard reshard(store);

    formatter->open_array_section("reshard");
    for (int i = 0; i < num_logshards; i++) {
      bool is_truncated = true;
      string marker;
      do {
        entries.clear();
        ret = reshard.list(i, marker, max_entries, entries, &is_truncated);
        if (ret < 0) {
          cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
          return ret;
        }
        for (auto iter=entries.begin(); iter != entries.end(); ++iter) {
          cls_rgw_reshard_entry& entry = *iter;
          encode_json("entry", entry, formatter);
          entry.get_key(&marker);
        }
        count += entries.size();
        formatter->flush(cout);
      } while (is_truncated && count < max_entries);

      if (count >= max_entries) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);
    return 0;
  }

  if (opt_cmd == OPT_RESHARD_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;
    ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(store, bucket_info, attrs, nullptr /* no callback */);
    list<cls_rgw_bucket_instance_entry> status;
    int r = br.get_status(&status);
    if (r < 0) {
      cerr << "ERROR: could not get resharding status for bucket " <<
	bucket_name << std::endl;
      return -r;
    }

    show_reshard_status(status, formatter);
  }

  if (opt_cmd == OPT_RESHARD_PROCESS) {
    RGWReshard reshard(store, true, &cout);

    int ret = reshard.process_all_logshards();
    if (ret < 0) {
      cerr << "ERROR: failed to process reshard logs, error=" << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_RESHARD_CANCEL) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;
    ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(store, bucket_info, attrs, nullptr /* no callback */);
    int ret = br.cancel();
    if (ret < 0) {
      if (ret == -EBUSY) {
	cerr << "There is ongoing resharding, please retry after " <<
	  store->ctx()->_conf.get_val<uint64_t>(
	    "rgw_reshard_bucket_lock_duration") <<
	  " seconds " << std::endl;
      } else {
	cerr << "Error canceling bucket " << bucket_name <<
	  " resharding: " << cpp_strerror(-ret) << std::endl;
      }
      return ret;
    }
    RGWReshard reshard(store);

    cls_rgw_reshard_entry entry;
    //entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    //entry.bucket_id = bucket_id;

    ret = reshard.remove(entry);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "Error in getting bucket " << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_UNLINK) {
    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<rgw_obj_index_key> oid_list;
    rgw_obj_key key(object, object_version);
    rgw_obj_index_key index_key;
    key.get_index_key(&index_key);
    oid_list.push_back(index_key);
    ret = store->remove_objs_from_index(bucket_info, oid_list);
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
    obj.key.set_instance(object_version);

    uint64_t obj_size;
    map<string, bufferlist> attrs;
    RGWObjectCtx obj_ctx(store);
    RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
    RGWRados::Object::Read read_op(&op_target);

    read_op.params.attrs = &attrs;
    read_op.params.obj_size = &obj_size;

    ret = read_op.prepare();
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
      } else if (iter->first == RGW_ATTR_COMPRESSION) {
        handled = decode_dump<RGWCompressionInfo>("compression", bl, formatter);
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
    if (!inconsistent_index) {
      RGWBucketAdminOp::remove_bucket(store, bucket_op, bypass_gc, true);
    } else {
      if (!yes_i_really_mean_it) {
	cerr << "using --inconsistent_index can corrupt the bucket index " << std::endl
	<< "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
	return 1;
      }
      RGWBucketAdminOp::remove_bucket(store, bucket_op, bypass_gc, false);
    }
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
    int ret = store->process_gc(!include_all);
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_LC_LIST) {
    formatter->open_array_section("lifecycle_list");
    map<string, int> bucket_lc_map;
    string marker;
#define MAX_LC_LIST_ENTRIES 100
    if (max_entries < 0) {
      max_entries = MAX_LC_LIST_ENTRIES;
    }
    do {
      int ret = store->list_lc_progress(marker, max_entries, &bucket_lc_map);
      if (ret < 0) {
        cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
        return 1;
      }
      map<string, int>::iterator iter;
      for (iter = bucket_lc_map.begin(); iter != bucket_lc_map.end(); ++iter) {
        formatter->open_object_section("bucket_lc_info");
        formatter->dump_string("bucket", iter->first);
        string lc_status = LC_STATUS[iter->second];
        formatter->dump_string("status", lc_status);
        formatter->close_section(); // objs
        formatter->flush(cout);
        marker = iter->first;
      }
    } while (!bucket_lc_map.empty());

    formatter->close_section(); //lifecycle list
    formatter->flush(cout);
  }


  if (opt_cmd == OPT_LC_GET) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    rgw_bucket bucket;
    RGWBucketInfo bucket_info;
    map<string, bufferlist> attrs;
    RGWLifecycleConfiguration config;
    ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto aiter = attrs.find(RGW_ATTR_LC);
    if (aiter == attrs.end()) {
      return -ENOENT;
    }

    bufferlist::const_iterator iter{&aiter->second};
    try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      cerr << "ERROR: decode life cycle config failed" << std::endl;
      return -EIO;
    }

    encode_json("result", config, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_LC_PROCESS) {
    int ret = store->process_lc();
    if (ret < 0) {
      cerr << "ERROR: lc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_ORPHANS_FIND) {
    if (!yes_i_really_mean_it) {
      cerr << "accidental removal of active objects can not be reversed; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    }

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

    info.pool = pool;
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

  if (opt_cmd == OPT_ORPHANS_LIST_JOBS){
    RGWOrphanStore orphan_store(store);
    int ret = orphan_store.init();
    if (ret < 0){
      cerr << "connection to cluster failed!" << std::endl;
      return -ret;
    }

    map <string,RGWOrphanSearchState> m;
    ret = orphan_store.list_jobs(m);
    if (ret < 0) {
      cerr << "job list failed" << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    for (const auto &it: m){
      if (!extra_info){
	formatter->dump_string("job-id",it.first);
      } else {
	encode_json("orphan_search_state", it.second, formatter);
      }
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_USER_CHECK) {
    check_bad_user_bucket_mapping(store, user_id, fix);
  }

  if (opt_cmd == OPT_USER_STATS) {
    if (user_id.empty()) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }

    string user_str = user_id.to_str();
    if (reset_stats) {
      if (!bucket_name.empty()){
	cerr << "ERROR: recalculate doesn't work on buckets" << std::endl;
	return EINVAL;
      }
      ret = store->cls_user_reset_stats(user_str);
      if (ret < 0) {
	cerr << "ERROR: could not clear user stats: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }

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

    cls_user_header header;
    int ret = store->cls_user_get_header(user_str, &header);
    if (ret < 0) {
      if (ret == -ENOENT) { /* in case of ENOENT */
        cerr << "User has not been initialized or user does not exist" << std::endl;
      } else {
        cerr << "ERROR: can't read user: " << cpp_strerror(ret) << std::endl;
      }
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
      return -ret;
    }
    ret = store->meta_mgr->put(metadata_key, bl, RGWMetadataHandler::RGWMetadataHandler::APPLY_ALWAYS);
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

  if (opt_cmd == OPT_METADATA_LIST || opt_cmd == OPT_USER_LIST) {
    if (opt_cmd == OPT_USER_LIST) {
      metadata_key = "user";
    }
    void *handle;
    int max = 1000;
    int ret = store->meta_mgr->list_keys_init(metadata_key, marker, &handle);
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    bool truncated;
    uint64_t count = 0;

    if (max_entries_specified) {
      formatter->open_object_section("result");
    }
    formatter->open_array_section("keys");

    uint64_t left;
    do {
      list<string> keys;
      left = (max_entries_specified ? max_entries - count : max);
      ret = store->meta_mgr->list_keys_next(handle, left, keys, &truncated);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      } if (ret != -ENOENT) {
	for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
	  formatter->dump_string("key", *iter);
          ++count;
	}
	formatter->flush(cout);
      }
    } while (truncated && left > 0);

    formatter->close_section();

    if (max_entries_specified) {
      encode_json("truncated", truncated, formatter);
      encode_json("count", count, formatter);
      if (truncated) {
        encode_json("marker", store->meta_mgr->get_marker(handle), formatter);
      }
      formatter->close_section();
    }
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

    if (period_id.empty()) {
      int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
      if (ret < 0) {
        return -ret;
      }
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      void *handle;
      list<cls_log_entry> entries;


      meta_log->init_list_entries(i, start_time.to_real_time(), end_time.to_real_time(), marker, &handle);
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

  if (opt_cmd == OPT_MDLOG_STATUS) {
    int i = (specified_shard_id ? shard_id : 0);

    if (period_id.empty()) {
      int ret = read_current_period_id(store, realm_id, realm_name, &period_id);
      if (ret < 0) {
        return -ret;
      }
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

    formatter->open_array_section("entries");

    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      RGWMetadataLogInfo info;
      meta_log->get_info(i, &info);

      ::encode_json("info", info, formatter);

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_MDLOG_AUTOTRIM) {
    // need a full history for purging old mdlog periods
    store->meta_mgr->init_oldest_log_period();

    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf()->rgw_md_log_max_shards;
    ret = crs.run(create_admin_meta_log_trim_cr(dpp(), store, &http, num_shards));
    if (ret < 0) {
      cerr << "automated mdlog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
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

    if (period_id.empty()) {
      std::cerr << "missing --period argument" << std::endl;
      return EINVAL;
    }
    RGWMetadataLog *meta_log = store->meta_mgr->get_log(period_id);

    ret = meta_log->trim(shard_id, start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: meta_log->trim(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_SYNC_STATUS) {
    sync_status(formatter);
  }

  if (opt_cmd == OPT_METADATA_SYNC_STATUS) {
    RGWMetaSyncStatusManager sync(store, store->get_async_rados());

    int ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_meta_sync_status sync_status;
    ret = sync.read_sync_status(&sync_status);
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    formatter->open_object_section("summary");
    encode_json("sync_status", sync_status, formatter);

    uint64_t full_total = 0;
    uint64_t full_complete = 0;

    for (auto marker_iter : sync_status.sync_markers) {
      full_total += marker_iter.second.total_entries;
      if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
        full_complete += marker_iter.second.pos;
      } else {
        full_complete += marker_iter.second.total_entries;
      }
    }

    formatter->open_object_section("full_sync");
    encode_json("total", full_total, formatter);
    encode_json("complete", full_complete, formatter);
    formatter->close_section();
    formatter->close_section();

    formatter->flush(cout);

  }

  if (opt_cmd == OPT_METADATA_SYNC_INIT) {
    RGWMetaSyncStatusManager sync(store, store->get_async_rados());

    int ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.init_sync_status();
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }


  if (opt_cmd == OPT_METADATA_SYNC_RUN) {
    RGWMetaSyncStatusManager sync(store, store->get_async_rados());

    int ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run();
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_DATA_SYNC_STATUS) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

    int ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_data_sync_status sync_status;
    if (specified_shard_id) {
      set<string> pending_buckets;
      set<string> recovering_buckets;
      rgw_data_sync_marker sync_marker;
      ret = sync.read_shard_status(shard_id, pending_buckets, recovering_buckets, &sync_marker, 
                                   max_entries_specified ? max_entries : 20);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: sync.read_shard_status() returned ret=" << ret << std::endl;
        return -ret;
      }
      formatter->open_object_section("summary");
      encode_json("shard_id", shard_id, formatter);
      encode_json("marker", sync_marker, formatter);
      encode_json("pending_buckets", pending_buckets, formatter);
      encode_json("recovering_buckets", recovering_buckets, formatter);
      formatter->close_section();
      formatter->flush(cout);
    } else {
      ret = sync.read_sync_status(&sync_status);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
        return -ret;
      }

      formatter->open_object_section("summary");
      encode_json("sync_status", sync_status, formatter);

      uint64_t full_total = 0;
      uint64_t full_complete = 0;

      for (auto marker_iter : sync_status.sync_markers) {
        full_total += marker_iter.second.total_entries;
        if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
          full_complete += marker_iter.second.pos;
        } else {
          full_complete += marker_iter.second.total_entries;
        }
      }

      formatter->open_object_section("full_sync");
      encode_json("total", full_total, formatter);
      encode_json("complete", full_complete, formatter);
      formatter->close_section();
      formatter->close_section();

      formatter->flush(cout);
    }
  }

  if (opt_cmd == OPT_DATA_SYNC_INIT) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone);

    int ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.init_sync_status();
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_DATA_SYNC_RUN) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWSyncModuleInstanceRef sync_module;
    int ret = store->svc.sync_modules->get_manager()->create_instance(g_ceph_context, store->svc.zone->get_zone().tier_type, 
        store->svc.zone->get_zone_params().tier_config, &sync_module);
    if (ret < 0) {
      lderr(cct) << "ERROR: failed to init sync module instance, ret=" << ret << dendl;
      return ret;
    }

    RGWDataSyncStatusManager sync(store, store->get_async_rados(), source_zone, sync_module);

    ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run();
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BUCKET_SYNC_INIT) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    rgw_bucket bucket;
    int ret = init_bucket_for_sync(tenant, bucket_name, bucket_id, bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketSyncStatusManager sync(store, source_zone, bucket);

    ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.init_sync_status();
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if ((opt_cmd == OPT_BUCKET_SYNC_DISABLE) || (opt_cmd == OPT_BUCKET_SYNC_ENABLE)) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    if (ret < 0) {
      cerr << "could not init realm " << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    RGWPeriod period;
    ret = period.init(g_ceph_context, store->svc.sysobj, realm_id, realm_name, true);
    if (ret < 0) {
      cerr << "failed to init period " << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (!store->svc.zone->is_meta_master()) {
      cerr << "failed to update bucket sync: only allowed on meta master zone "  << std::endl;
      cerr << period.get_master_zone() << " | " << period.get_realm() << std::endl;
      return EINVAL;
    }

    rgw_obj obj(bucket, object);
    ret = set_bucket_sync_enabled(store, opt_cmd, tenant, bucket_name);
    if (ret < 0)
      return -ret;
  }

  if (opt_cmd == OPT_BUCKET_SYNC_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    RGWBucketInfo bucket_info;
    rgw_bucket bucket;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      return -ret;
    }
    bucket_sync_status(store, bucket_info, source_zone, std::cout);
  }

  if (opt_cmd == OPT_BUCKET_SYNC_MARKERS) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    rgw_bucket bucket;
    int ret = init_bucket_for_sync(tenant, bucket_name, bucket_id, bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketSyncStatusManager sync(store, source_zone, bucket);

    ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.read_sync_status();
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    map<int, rgw_bucket_shard_sync_info>& sync_status = sync.get_sync_status();

    encode_json("sync_status", sync_status, formatter);
    formatter->flush(cout);
  }

 if (opt_cmd == OPT_BUCKET_SYNC_RUN) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    rgw_bucket bucket;
    int ret = init_bucket_for_sync(tenant, bucket_name, bucket_id, bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketSyncStatusManager sync(store, source_zone, bucket);

    ret = sync.init();
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run();
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BILOG_LIST) {
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
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    do {
      list<rgw_bi_log_entry> entries;
      ret = store->list_bi_log_entries(bucket_info, shard_id, marker, max_entries - count, entries, &truncated);
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

  if (opt_cmd == OPT_SYNC_ERROR_LIST) {
    if (max_entries < 0) {
      max_entries = 1000;
    }

    bool truncated;
    utime_t start_time, end_time;

    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    if (shard_id < 0) {
      shard_id = 0;
    }

    formatter->open_array_section("entries");

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      formatter->open_object_section("shard");
      encode_json("shard_id", shard_id, formatter);
      formatter->open_array_section("entries");

      int count = 0;
      string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);

      do {
        list<cls_log_entry> entries;
        ret = store->time_log_list(oid, start_time.to_real_time(), end_time.to_real_time(),
                                   max_entries - count, entries, marker, &marker, &truncated);
        if (ret == -ENOENT) {
          break;
        }
        if (ret < 0) {
          cerr << "ERROR: store->time_log_list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        count += entries.size();

        for (auto& cls_entry : entries) {
          rgw_sync_error_info log_entry;

          auto iter = cls_entry.data.cbegin();
          try {
            decode(log_entry, iter);
          } catch (buffer::error& err) {
            cerr << "ERROR: failed to decode log entry" << std::endl;
            continue;
          }
          formatter->open_object_section("entry");
          encode_json("id", cls_entry.id, formatter);
          encode_json("section", cls_entry.section, formatter);
          encode_json("name", cls_entry.name, formatter);
          encode_json("timestamp", cls_entry.timestamp, formatter);
          encode_json("info", log_entry, formatter);
          formatter->close_section();
          formatter->flush(cout);
        }
      } while (truncated && count < max_entries);

      formatter->close_section();
      formatter->close_section();

      if (specified_shard_id) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_SYNC_ERROR_TRIM) {
    utime_t start_time, end_time;
    int ret = parse_date_str(start_date, start_time);
    if (ret < 0)
      return -ret;

    ret = parse_date_str(end_date, end_time);
    if (ret < 0)
      return -ret;

    if (shard_id < 0) {
      shard_id = 0;
    }

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      ret = trim_sync_error_log(shard_id, start_time.to_real_time(),
                                end_time.to_real_time(), start_marker,
                                end_marker, trim_delay_ms);
      if (ret < 0) {
        cerr << "ERROR: sync error trim: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (specified_shard_id) {
        break;
      }
    }
  }

  if (opt_cmd == OPT_BILOG_TRIM) {
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
    ret = store->trim_bi_log_entries(bucket_info, shard_id, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_BILOG_STATUS) {
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
    map<int, string> markers;
    ret = store->get_bi_log_status(bucket_info, shard_id, markers);
    if (ret < 0) {
      cerr << "ERROR: get_bi_log_status(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("entries");
    encode_json("markers", markers, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BILOG_AUTOTRIM) {
    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    rgw::BucketTrimConfig config;
    configure_bucket_trim(store->ctx(), config);

    rgw::BucketTrimManager trim(store, config);
    ret = trim.init();
    if (ret < 0) {
      cerr << "trim manager init failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
    ret = crs.run(trim.create_admin_bucket_trim_cr(&http));
    if (ret < 0) {
      cerr << "automated bilog trim failed with " << cpp_strerror(ret) << std::endl;
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
    RGWDataChangesLog::LogMarker log_marker;

    do {
      list<rgw_data_change_log_entry> entries;
      if (specified_shard_id) {
        ret = log->list_entries(shard_id, start_time.to_real_time(), end_time.to_real_time(), max_entries - count, entries, marker, NULL, &truncated);
      } else {
        ret = log->list_entries(start_time.to_real_time(), end_time.to_real_time(), max_entries - count, entries, log_marker, &truncated);
      }
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (list<rgw_data_change_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_data_change_log_entry& entry = *iter;
        if (!extra_info) {
          encode_json("entry", entry.entry, formatter);
        } else {
          encode_json("entry", entry, formatter);
        }
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_DATALOG_STATUS) {
    RGWDataChangesLog *log = store->data_log;
    int i = (specified_shard_id ? shard_id : 0);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_data_log_num_shards; i++) {
      list<cls_log_entry> entries;

      RGWDataChangesLogInfo info;
      log->get_info(i, &info);

      ::encode_json("info", info, formatter);

      if (specified_shard_id)
        break;
    }

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
    ret = log->trim_entries(start_time.to_real_time(), end_time.to_real_time(), start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
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
        return set_user_bucket_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else if (quota_scope == "user") {
        return set_user_quota(opt_cmd, user, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else {
        cerr << "ERROR: invalid quota scope specification. Please specify either --quota-scope=bucket, or --quota-scope=user" << std::endl;
        return EINVAL;
      }
    }
  }

  if (opt_cmd == OPT_MFA_CREATE) {
    rados::cls::otp::otp_info_t config;

    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_seed.empty()) {
      cerr << "ERROR: TOTP device seed was not provided (via --totp-seed)" << std::endl;
      return EINVAL;
    }


    rados::cls::otp::SeedType seed_type;
    if (totp_seed_type == "hex") {
      seed_type = rados::cls::otp::OTP_SEED_HEX;
    } else if (totp_seed_type == "base32") {
      seed_type = rados::cls::otp::OTP_SEED_BASE32;
    } else {
      cerr << "ERROR: invalid seed type: " << totp_seed_type << std::endl;
      return EINVAL;
    }

    config.id = totp_serial;
    config.seed = totp_seed;
    config.seed_type = seed_type;

    if (totp_seconds > 0) {
      config.step_size = totp_seconds;
    }

    if (totp_window > 0) {
      config.window = totp_window;
    }

    real_time mtime = real_clock::now();
    string oid = store->get_mfa_oid(user_id);

    int ret = store->meta_mgr->mutate(rgw_otp_get_handler(), oid, mtime, &objv_tracker,
                                      MDLOG_STATUS_WRITE, RGWMetadataHandler::APPLY_ALWAYS,
                                      [&] {
      return store->create_mfa(user_id, config, &objv_tracker, mtime);
    });
    if (ret < 0) {
      cerr << "MFA creation failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    
    RGWUserInfo& user_info = user_op.get_user_info();
    user_info.mfa_ids.insert(totp_serial);
    user_op.set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = user.modify(user_op, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT_MFA_REMOVE) {
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    real_time mtime = real_clock::now();
    string oid = store->get_mfa_oid(user_id);

    int ret = store->meta_mgr->mutate(rgw_otp_get_handler(), oid, mtime, &objv_tracker,
                                      MDLOG_STATUS_WRITE, RGWMetadataHandler::APPLY_ALWAYS,
                                      [&] {
      return store->remove_mfa(user_id, totp_serial, &objv_tracker, mtime);
    });
    if (ret < 0) {
      cerr << "MFA removal failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWUserInfo& user_info = user_op.get_user_info();
    user_info.mfa_ids.erase(totp_serial);
    user_op.set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = user.modify(user_op, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT_MFA_GET) {
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t result;
    int ret = store->get_mfa(user_id, totp_serial, &result);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("entry", result, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

 if (opt_cmd == OPT_MFA_LIST) {
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = store->list_mfa(user_id, &result);
    if (ret < 0) {
      cerr << "MFA listing failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("entries", result, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

 if (opt_cmd == OPT_MFA_CHECK) {
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_pin.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-pin)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = store->check_mfa(user_id, totp_serial, totp_pin.front());
    if (ret < 0) {
      cerr << "MFA check failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    cout << "ok" << std::endl;
  }

 if (opt_cmd == OPT_MFA_RESYNC) {
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_pin.size() != 2) {
      cerr << "ERROR: missing two --totp-pin params (--totp-pin=<first> --totp-pin=<second>)" << std::endl;
    }

    rados::cls::otp::otp_info_t config;
    int ret = store->get_mfa(user_id, totp_serial, &config);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    ceph::real_time now;

    ret = store->otp_get_current_time(user_id, &now);
    if (ret < 0) {
      cerr << "ERROR: failed to fetch current time from osd: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    time_t time_ofs;

    ret = scan_totp(store->ctx(), now, config, totp_pin, &time_ofs);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "failed to resync, TOTP values not found in range" << std::endl;
      } else {
        cerr << "ERROR: failed to scan for TOTP values: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    config.time_ofs = time_ofs;

    /* now update the backend */
    real_time mtime = real_clock::now();
    string oid = store->get_mfa_oid(user_id);

    ret = store->meta_mgr->mutate(rgw_otp_get_handler(), oid, mtime, &objv_tracker,
                                  MDLOG_STATUS_WRITE, RGWMetadataHandler::APPLY_ALWAYS,
                                  [&] {
      return store->create_mfa(user_id, config, &objv_tracker, mtime);
    });
    if (ret < 0) {
      cerr << "MFA update failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

 }

 if (opt_cmd == OPT_RESHARD_STALE_INSTANCES_LIST) {
   ret = RGWBucketAdminOp::list_stale_instances(store, bucket_op,f);
   if (ret < 0) {
     cerr << "ERROR: listing stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

 if (opt_cmd == OPT_RESHARD_STALE_INSTANCES_DELETE) {
   ret = RGWBucketAdminOp::clear_stale_instances(store, bucket_op,f);
   if (ret < 0) {
     cerr << "ERROR: deleting stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

  if (opt_cmd == OPT_PUBSUB_TOPICS_LIST) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();

    RGWUserPubSub ups(store, user_info.user_id);

    rgw_bucket bucket;

    if (!bucket_name.empty()) {
      rgw_pubsub_bucket_topics result;
      RGWBucketInfo bucket_info;
      int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      auto b = ups.get_bucket(bucket_info.bucket);
      ret = b->get_topics(&result);
      if (ret < 0) {
        cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("result", result, formatter);
    } else {
      rgw_pubsub_user_topics result;
      int ret = ups.get_user_topics(&result);
      if (ret < 0) {
        cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("result", result, formatter);
    }
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_PUBSUB_TOPIC_CREATE) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    ret = ups.create_topic(topic_name);
    if (ret < 0) {
      cerr << "ERROR: could not create topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_PUBSUB_TOPIC_GET) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    rgw_pubsub_topic_subs topic;
    ret = ups.get_topic(topic_name, &topic);
    if (ret < 0) {
      cerr << "ERROR: could not create topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("topic", topic, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_PUBSUB_NOTIFICATION_CREATE) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    rgw_bucket bucket;

    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto b = ups.get_bucket(bucket_info.bucket);
    ret = b->create_notification(topic_name, event_types);
    if (ret < 0) {
      cerr << "ERROR: could not publish bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_PUBSUB_NOTIFICATION_RM) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    rgw_bucket bucket;

    RGWBucketInfo bucket_info;
    int ret = init_bucket(tenant, bucket_name, bucket_id, bucket_info, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto b = ups.get_bucket(bucket_info.bucket);
    ret = b->remove_notification(topic_name);
    if (ret < 0) {
      cerr << "ERROR: could not publish bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_PUBSUB_TOPIC_RM) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    ret = ups.remove_topic(topic_name);
    if (ret < 0) {
      cerr << "ERROR: could not remove topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_PUBSUB_SUB_GET) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --sub-name)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    rgw_pubsub_sub_config sub_conf;

    auto sub = ups.get_sub(sub_name);
    ret = sub->get_conf(&sub_conf);
    if (ret < 0) {
      cerr << "ERROR: could not get subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("sub", sub_conf, formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_PUBSUB_SUB_CREATE) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --sub-name)" << std::endl;
      return EINVAL;
    }
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    rgw_pubsub_topic_subs topic;
    int ret = ups.get_topic(topic_name, &topic);
    if (ret < 0) {
      cerr << "ERROR: topic not found" << std::endl;
      return EINVAL;
    }

    rgw_pubsub_sub_dest dest_config;
    dest_config.bucket_name = sub_dest_bucket;
    dest_config.oid_prefix = sub_oid_prefix;
    dest_config.push_endpoint = sub_push_endpoint;

    auto psmodule = static_cast<RGWPSSyncModuleInstance *>(store->get_sync_module().get());
    auto conf = psmodule->get_effective_conf();

    if (dest_config.bucket_name.empty()) {
      dest_config.bucket_name = string(conf["data_bucket_prefix"]) + user_info.user_id.to_str() + "-" + topic.topic.name;
    }
    if (dest_config.oid_prefix.empty()) {
      dest_config.oid_prefix = conf["data_oid_prefix"];
    }
    auto sub = ups.get_sub(sub_name);
    ret = sub->subscribe(topic_name, dest_config);
    if (ret < 0) {
      cerr << "ERROR: could not store subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT_PUBSUB_SUB_RM) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --sub-name)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    auto sub = ups.get_sub(sub_name);
    ret = sub->unsubscribe(topic_name);
    if (ret < 0) {
      cerr << "ERROR: could not get subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT_PUBSUB_SUB_PULL) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --sub-name)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    RGWUserPubSub::Sub::list_events_result result;

    if (!max_entries_specified) {
      max_entries = 100;
    }
    auto sub = ups.get_sub(sub_name);
    ret = sub->list_events(marker, max_entries, &result);
    if (ret < 0) {
      cerr << "ERROR: could not list events: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("result", result, formatter);
    formatter->flush(cout);
 }

 if (opt_cmd == OPT_PUBSUB_EVENT_RM) {
    if (get_tier_type(store) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (user_id.empty()) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --sub-name)" << std::endl;
      return EINVAL;
    }
    if (event_id.empty()) {
      cerr << "ERROR: event id was not provided (via --event-id)" << std::endl;
      return EINVAL;
    }
    RGWUserInfo& user_info = user_op.get_user_info();
    RGWUserPubSub ups(store, user_info.user_id);

    auto sub = ups.get_sub(sub_name);
    ret = sub->remove_event(event_id);
    if (ret < 0) {
      cerr << "ERROR: could not remove event: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  return 0;
}
