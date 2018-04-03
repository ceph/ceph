// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_admin_argument_parsing.h"

#include <boost/program_options.hpp>
#include <common/safe_io.h>

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

#include "common/ceph_json.h"
#include "common/ceph_argparse.h"

#include "rgw_replica_log.h"
#include "rgw_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_reshard.h"
#include "rgw_admin_multisite.h"
#include "rgw_admin_other.h"
#include "rgw_admin_opt_bucket.h"


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
  cout << "  usage show                 show usage (by user, date range)\n";
  cout << "  usage trim                 trim usage (by user, date range)\n";
  cout << "  usage clear                reset all the usage stats for the cluster\n";
  cout << "  gc list                    dump expired garbage collection objects (specify\n";
  cout << "                             --include-all to list all entries, including unexpired)\n";
  cout << "  gc process                 manually process garbage (specify\n";
  cout << "                             --include-all to process all entries, including unexpired)\n";
  cout << "  lc list                    list all bucket lifecycle progress\n";
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
  cout << "  opstate list               list stateful operations entries (use client_id,\n";
  cout << "                             op_id, object)\n";
  cout << "  opstate set                set state on an entry (use client_id, op_id, object, state)\n";
  cout << "  opstate renew              renew state on an entry (use client_id, op_id, object)\n";
  cout << "  opstate rm                 remove entry (use client_id, op_id, object)\n";
  cout << "  replicalog get             get replica metadata log entry\n";
  cout << "  replicalog update          update replica metadata log entry\n";
  cout << "  replicalog rm              remove replica metadata log entry\n";
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
  cout << "  sync error list            list sync error\n";
  cout << "  sync error trim            trim sync error\n";
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
  cout << "   --bucket=<bucket>         Specify the bucket name. Also used by the quota command.\n";
  cout << "   --pool=<pool>             Specify the pool name. Also used to scan for leaked rados objects.\n";
  cout << "   --object=<object>         object name\n";
  cout << "   --date=<date>             date in the format yyyy-mm-dd\n";
  cout << "   --start-date=<date>       start date in the format yyyy-mm-dd\n";
  cout << "   --end-date=<date>         end date in the format yyyy-mm-dd\n";
  cout << "   --bucket-id=<bucket-id>   bucket id\n";
  cout << "   --shard-id=<shard-id>     optional for mdlog list\n";
  cout << "                             required for: \n";
  cout << "                               mdlog trim\n";
  cout << "                               replica mdlog get/delete\n";
  cout << "                               replica datalog get/delete\n";
  cout << "   --metadata-key=<key>      key to retrieve metadata from with metadata get\n";
  cout << "   --remote=<remote>         zone or zonegroup id of remote gateway\n";
  cout << "   --period=<id>             period id\n";
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
  cout << "   --show-log-entries=<flag> enable/disable dump of log entries on log show\n";
  cout << "   --show-log-sum=<flag>     enable/disable dump of log summation on log show\n";
  cout << "   --skip-zero-entries       log show only dumps entries that don't have zero value\n";
  cout << "                             in one of the numeric field\n";
  cout << "   --infile=<file>           specify a file to read in when setting data\n";
  cout << "   --state=<state>           specify a state for the opstate set command\n";
  cout << "   --replica-log-type=<logtypestr>\n";
  cout << "                             replica log type (metadata, data, bucket), required for\n";
  cout << "                             replica log operations\n";
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
  cout << "\n";
  generic_client_usage();
}

static int get_cmd(const char *cmd, const char *prev_cmd, const char *prev_prev_cmd, bool *need_more)
{
  *need_more = false;
  // NOTE: please keep the checks in alphabetical order !!!
  if (strcmp(cmd, "bi") == 0 ||
      strcmp(cmd, "bilog") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "caps") == 0 ||
      strcmp(cmd, "data") == 0 ||
      strcmp(cmd, "datalog") == 0 ||
      strcmp(cmd, "error") == 0 ||
      strcmp(cmd, "gc") == 0 ||
      strcmp(cmd, "global") == 0 ||
      strcmp(cmd, "key") == 0 ||
      strcmp(cmd, "log") == 0 ||
      strcmp(cmd, "lc") == 0 ||
      strcmp(cmd, "mdlog") == 0 ||
      strcmp(cmd, "metadata") == 0 ||
      strcmp(cmd, "object") == 0 ||
      strcmp(cmd, "objects") == 0 ||
      strcmp(cmd, "olh") == 0 ||
      strcmp(cmd, "opstate") == 0 ||
      strcmp(cmd, "orphans") == 0 ||
      strcmp(cmd, "period") == 0 ||
      strcmp(cmd, "placement") == 0 ||
      strcmp(cmd, "pool") == 0 ||
      strcmp(cmd, "pools") == 0 ||
      strcmp(cmd, "quota") == 0 ||
      strcmp(cmd, "realm") == 0 ||
      strcmp(cmd, "replicalog") == 0 ||
      strcmp(cmd, "role") == 0 ||
      strcmp(cmd, "role-policy") == 0 ||
      strcmp(cmd, "subuser") == 0 ||
      strcmp(cmd, "sync") == 0 ||
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
    if (strcmp(cmd, "rm") == 0)
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
    if (strcmp(cmd, "rm") == 0)
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
    if (strcmp(cmd, "rm") == 0)
      return OPT_ZONEGROUP_DELETE;
    if (strcmp(cmd, "get") == 0)
      return OPT_ZONEGROUP_GET;
    if (strcmp(cmd, "modify") == 0)
      return OPT_ZONEGROUP_MODIFY;
    if (strcmp(cmd, "list") == 0)
      return OPT_ZONEGROUP_LIST;
    if (strcmp(cmd, "set") == 0)
      return OPT_ZONEGROUP_SET;
    if (strcmp(cmd, "rm") == 0)
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
    if (strcmp(cmd, "rm") == 0)
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
    if (strcmp(cmd, "rm") == 0)
      return OPT_REPLICALOG_DELETE;
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
    if (strcmp(cmd, "rm") == 0)
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
  }

  return -EINVAL;
}

ReplicaLogType get_replicalog_type(const std::string& name) {
  if (name == "md" || name == "meta" || name == "metadata")
    return ReplicaLog_Metadata;
  if (name == "data")
    return ReplicaLog_Data;
  if (name == "bucket")
    return ReplicaLog_Bucket;

  return ReplicaLog_Invalid;
}

BIIndexType get_bi_index_type(const std::string& type_str) {
  if (type_str == "plain")
    return PlainIdx;
  if (type_str == "instance")
    return InstanceIdx;
  if (type_str == "olh")
    return OLHIdx;

  return InvalidIdx;
}

static void parse_tier_config_param(const std::string& s, map<std::string, std::string, ltstr_nocase>& out)
{
  list<std::string> confs;
  get_str_list(s, ",", confs);
  for (auto c : confs) {
    ssize_t pos = c.find("=");
    if (pos < 0) {
      out[c] = "";
    } else {
      out[c.substr(0, pos)] = c.substr(pos + 1);
    }
  }
}

int parse_command(const std::string& access_key, int gen_access_key, const std::string& secret_key, int gen_secret_key,
                  vector<const char*>& args, RgwAdminCommand& opt_cmd, std::string& metadata_key, std::string& tenant, rgw_user& user_id)
{
  if (args.empty()) {
    exit(1)
  }
  bool need_more;
  const char *prev_cmd = nullptr;
  const char *prev_prev_cmd = nullptr;
  std::vector<const char*>::iterator i;
  int command_or_error = 0;
  for (i = args.begin(); i != args.end(); ++i) {
    command_or_error = get_cmd(*i, prev_cmd, prev_prev_cmd, &need_more);
    if (command_or_error < 0) {
      cerr << "unrecognized arg " << *i << std::endl;
      usage();
      ceph_abort();
    }
    if (!need_more) {
      ++i;
      break;
    }
    prev_prev_cmd = prev_cmd;
    prev_cmd = *i;
  }

  opt_cmd = static_cast<RgwAdminCommand>(command_or_error);
  if (opt_cmd == OPT_NO_CMD) {
    cerr << "no command" << std::endl;
    exit(1)
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
  return 0;
}

int parse_common_commandline_params(std::vector<const char*>& args, rgw_user& user_id,
                                    std::string& access_key, int& gen_access_key,
                                    std::string& secret_key, int& gen_secret_key,
                                    std::string& metadata_key, std::string& tenant, std::string& date,
                                    std::string& start_date, std::string& end_date,
                                    std::string& infile, std::string& source_zone_name,
                                    std::string& bucket_id, std::string& bucket_name,
                                    std::string& start_marker, std::string& end_marker,
                                    std::string& marker, int& yes_i_really_mean_it,
                                    int& max_entries, bool& max_entries_specified,
                                    std::string& object, int& shard_id, bool& specified_shard_id,
                                    int& fix, std::string& period_id, std::string& realm_id,
                                    std::string& realm_name, std::string& format, int& pretty_format,
                                    int& purge_data, int& delete_child_objects, int& max_concurrent_ios)
{
  long long tmp = 0;
  std::string val, key_type_str, err;
  std::ostringstream errs;
  int unused_param;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--uid", (char*) nullptr)) {
      user_id.from_str(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--tenant", (char*) nullptr)) {
      tenant = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--access-key", (char*) nullptr)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--secret", "--secret-key", (char*) nullptr)) {
      secret_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", "--bucket", (char*) nullptr)) {
      bucket_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-o", "--object", (char*) nullptr)) {
      object = val;
    } else if (ceph_argparse_binary_flag(args, i, &gen_access_key, nullptr, "--gen-access-key", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &gen_secret_key, nullptr, "--gen-secret", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &tmp, errs, "-a", "--auth-uid", (char*) nullptr)) {
      if (!errs.str().empty()) {
        cerr << errs.str() << std::endl;
        exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-entries", (char*) nullptr)) {
      max_entries = (int)strict_strtol(val.c_str(), 10, &err);
      max_entries_specified = true;
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max entries: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--date", "--time", (char*) nullptr)) {
      date = val;
      if (end_date.empty())
        end_date = date;
    } else if (ceph_argparse_witharg(args, i, &val, "--start-date", "--start-time", (char*) nullptr)) {
      start_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--end-date", "--end-time", (char*) nullptr)) {
      end_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--shard-id", (char*) nullptr)) {
      shard_id = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse shard id: " << err << std::endl;
        return EINVAL;
      }
      specified_shard_id = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--bucket-id", (char*) nullptr)) {
      bucket_id = val;
      if (bucket_id.empty()) {
        cerr << "bad bucket-id" << std::endl;
        exit(1)
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*) nullptr)) {
      format = val;
    } else if (ceph_argparse_binary_flag(args, i, &pretty_format, nullptr, "--pretty-format", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &yes_i_really_mean_it, nullptr, "--yes-i-really-mean-it", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &fix, nullptr, "--fix", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--infile", (char*) nullptr)) {
      infile = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--metadata-key", (char*) nullptr)) {
      metadata_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--marker", (char*) nullptr)) {
      marker = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--start-marker", (char*) nullptr)) {
      start_marker = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--end-marker", (char*) nullptr)) {
      end_marker = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--period", (char*) nullptr)) {
      period_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--realm-id", (char*) nullptr)) {
      realm_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--source-zone", (char*) nullptr)) {
      source_zone_name = val;
    } else if (ceph_argparse_binary_flag(args, i, &delete_child_objects, nullptr, "--purge-objects", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &purge_data, nullptr, "--purge-data", (char*) nullptr)) {
      delete_child_objects = purge_data;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-concurrent-ios", (char*) nullptr)) {
      max_concurrent_ios = (int) strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max concurrent ios: " << err << std::endl;
        return EINVAL;
      }
    }
      // keep the parsing of other options to ensure the validity of the passed parameters
    else if (ceph_argparse_witharg(args, i, &val, "-i", "--uid", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--subuser", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "-e", "--email", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "-n", "--display-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--object-version", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--client-id", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--op-id", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--state", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--op-mask", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--key-type", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--job-id", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--show-log-entries", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--show-log-sum", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--skip-zero-entries", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--admin", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--system", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--verbose", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--staging", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--commit", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--min-rewrite-size", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--max-rewrite-size", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--min-rewrite-stripe-size", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--max-buckets", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--max-size", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--max-objects", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--num-shards", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--orphan-stale-secs", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--daemon-id", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--access", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--temp-url-key", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--temp-url-key2", "--temp-url-key-2", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--categories", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--purge-keys", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--remove-bad", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--check-head-obj-locator", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--check-objects", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--sync-stats", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--include-all", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--extra-info", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--bypass-gc", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--warnings-only", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--inconsistent-index", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--caps", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--quota-scope", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--replica-log-type", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--index-type", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--master", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--default", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--redirect-zone", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--read-only", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--master-zone", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--epoch", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--remote", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--url", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--realm-new-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--zonegroup-id", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--zonegroup-new-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--placement-id", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--tags", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--tags-add", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--tags-rm", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--api-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--zone-id", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--zone-new-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--endpoints", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--sync-from", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--sync-from-rm", (char*) nullptr) ||
             ceph_argparse_binary_flag(args, i, &unused_param, nullptr, "--sync-from-all", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--tier-type", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--tier-config", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--tier-config-rm", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--index-pool", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--data-pool", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--data-extra-pool", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--placement-index-type", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--compression", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--role-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--path", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--assume-role-policy-doc", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--policy-name", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--policy-doc", (char*) nullptr) ||
             ceph_argparse_witharg(args, i, &val, "--path-prefix", (char*) nullptr)) {
      // do nothing
    } else if (strncmp(*i, "-", 1) == 0) {
      cerr << "ERROR: invalid flag " << *i << std::endl;
      return EINVAL;
    } else {
      ++i;
    }
  }
  return 0;
}

int parse_bucket_commandline_params(std::vector<const char*>& args, int& num_shards,
                                    bool& num_shards_specified, std::string& object_version,
                                    BIIndexType& bi_index_type, int& verbose, int& warnings_only,
                                    int& bypass_gc, int& check_head_obj_locator, int& remove_bad,
                                    int& check_objects, int& inconsistent_index,
                                    uint64_t& min_rewrite_size, uint64_t& max_rewrite_size,
                                    uint64_t& min_rewrite_stripe_size)
{
  std::string val, key_type_str, err;
  std::ostringstream errs;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--object-version", (char*) nullptr)) {
      object_version = val;
    } else if (ceph_argparse_binary_flag(args, i, &verbose, nullptr, "--verbose", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--min-rewrite-size", (char*) nullptr)) {
      min_rewrite_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--max-rewrite-size", (char*) nullptr)) {
      max_rewrite_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--min-rewrite-stripe-size", (char*) nullptr)) {
      min_rewrite_stripe_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--num-shards", (char*) nullptr)) {
      num_shards = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse num shards: " << err << std::endl;
        return EINVAL;
      }
      num_shards_specified = true;
    } else if (ceph_argparse_binary_flag(args, i, &remove_bad, nullptr, "--remove-bad", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &check_head_obj_locator, nullptr, "--check-head-obj-locator", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &check_objects, nullptr, "--check-objects", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &bypass_gc, nullptr, "--bypass-gc", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &warnings_only, nullptr, "--warnings-only", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &inconsistent_index, nullptr, "--inconsistent-index", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--index-type", (char*) nullptr)) {
      std::string index_type_str = val;
      bi_index_type = get_bi_index_type(index_type_str);
      if (bi_index_type == InvalidIdx) {
        cerr << "ERROR: invalid bucket index entry type" << std::endl;
        return EINVAL;
      }
    } else {
      ++i;
    }
  }
  return 0;
}

int parse_multisite_commandline_params(std::vector<const char*>& args, int& set_default,
                                       std::string& url, int& commit, std::string& period_epoch,
                                       std::string& remote, int& staging, std::string& realm_new_name,
                                       std::string& api_name, boost::optional<std::string>& compression_type,
                                       boost::optional<std::string>& index_pool,
                                       boost::optional<std::string>& data_pool,
                                       boost::optional<std::string>& data_extra_pool,
                                       std::list<std::string>& endpoints,
                                       RGWBucketIndexType& placement_index_type, bool& index_type_specified,
                                       bool& is_master, bool& is_master_set, bool& read_only,
                                       int& is_read_only_set, std::string& master_zone,
                                       std::string& placement_id, std::string& redirect_zone,
                                       bool& redirect_zone_set, std::list<std::string>& sync_from,
                                       std::list<std::string>& sync_from_rm, bool& sync_from_all,
                                       int& sync_from_all_specified, std::list<std::string>& tags,
                                       std::list<std::string>& tags_add, std::list<std::string>& tags_rm,
                                       std::string& tier_type, bool& tier_type_specified,
                                       map<std::string, std::string, ltstr_nocase>& tier_config_add,
                                       map<std::string, std::string, ltstr_nocase>& tier_config_rm,
                                       std::string& zone_id, std::string& zone_name,
                                       std::string& zone_new_name, std::string& zonegroup_id,
                                       std::string& zonegroup_name, std::string& zonegroup_new_name)
{
  int is_master_int, read_only_int, tmp_int;
  std::string val, key_type_str, err;
  std::ostringstream errs;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_binary_flag(args, i, &staging, nullptr, "--staging", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &commit, nullptr, "--commit", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &is_master_int, nullptr, "--master", (char*) nullptr)) {
      is_master = (bool)is_master_int;
      is_master_set = true;
    } else if (ceph_argparse_binary_flag(args, i, &set_default, nullptr, "--default", (char*) nullptr)) {
      /* do nothing */
    } else if (ceph_argparse_witharg(args, i, &val, "--redirect-zone", (char*) nullptr)) {
      redirect_zone = val;
      redirect_zone_set = true;
    } else if (ceph_argparse_binary_flag(args, i, &read_only_int, nullptr, "--read-only", (char*) nullptr)) {
      read_only = (bool)read_only_int;
      is_read_only_set = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--master-zone", (char*) nullptr)) {
      master_zone = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--epoch", (char*) nullptr)) {
      period_epoch = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--remote", (char*) nullptr)) {
      remote = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--url", (char*) nullptr)) {
      url = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--realm-new-name", (char*) nullptr)) {
      realm_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zonegroup-id", (char*) nullptr)) {
      zonegroup_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zonegroup-new-name", (char*) nullptr)) {
      zonegroup_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--placement-id", (char*) nullptr)) {
      placement_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--tags", (char*) nullptr)) {
      get_str_list(val, tags);
    } else if (ceph_argparse_witharg(args, i, &val, "--tags-add", (char*) nullptr)) {
      get_str_list(val, tags_add);
    } else if (ceph_argparse_witharg(args, i, &val, "--tags-rm", (char*) nullptr)) {
      get_str_list(val, tags_rm);
    } else if (ceph_argparse_witharg(args, i, &val, "--api-name", (char*) nullptr)) {
      api_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zone-id", (char*) nullptr)) {
      zone_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zone-new-name", (char*) nullptr)) {
      zone_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--endpoints", (char*) nullptr)) {
      get_str_list(val, endpoints);
    } else if (ceph_argparse_witharg(args, i, &val, "--sync-from", (char*) nullptr)) {
      get_str_list(val, sync_from);
    } else if (ceph_argparse_witharg(args, i, &val, "--sync-from-rm", (char*) nullptr)) {
      get_str_list(val, sync_from_rm);
    } else if (ceph_argparse_binary_flag(args, i, &tmp_int, nullptr, "--sync-from-all", (char*) nullptr)) {
      sync_from_all = (bool)tmp_int;
      sync_from_all_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--tier-type", (char*) nullptr)) {
      tier_type = val;
      tier_type_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--tier-config", (char*) nullptr)) {
      parse_tier_config_param(val, tier_config_add);
    } else if (ceph_argparse_witharg(args, i, &val, "--tier-config-rm", (char*) nullptr)) {
      parse_tier_config_param(val, tier_config_rm);
    } else if (ceph_argparse_witharg(args, i, &val, "--index-pool", (char*) nullptr)) {
      index_pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--data-pool", (char*) nullptr)) {
      data_pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--data-extra-pool", (char*) nullptr)) {
      data_extra_pool = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--placement-index-type", (char*) nullptr)) {
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
    } else if (ceph_argparse_witharg(args, i, &val, "--compression", (char*) nullptr)) {
      compression_type = val;
    } else {
      ++i;
    }
  }
  return 0;
}

int parse_quota_commandline_params(std::vector<const char*>& args, std::string& quota_scope,
                                   int64_t& max_size, bool& have_max_size,
                                   int64_t& max_objects, bool& have_max_objects)
{
  std::string val, key_type_str, err;
  std::ostringstream errs;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-size", (char*) nullptr)) {
      max_size = strict_si_cast<int64_t>(val.c_str(), &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max size: " << err << std::endl;
        return EINVAL;
      }
      have_max_size = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-objects", (char*) nullptr)) {
      max_objects = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max objects: " << err << std::endl;
        return EINVAL;
      }
      have_max_objects = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--quota-scope", (char*) nullptr)) {
      quota_scope = val;
    } else if (strncmp(*i, "-", 1) == 0) {
      cerr << "ERROR: invalid flag " << *i << std::endl;
      return EINVAL;
    } else {
      ++i;
    }
  }
  return 0;
}

int parse_role_commandline_params(std::vector<const char*>& args, std::string& role_name,
                                  std::string& policy_name, std::string& assume_role_doc,
                                  std::string& perm_policy_doc, std::string& path,
                                  std::string& path_prefix)
{
  std::string val, key_type_str, err;
  std::ostringstream errs;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--role-name", (char*) nullptr)) {
      role_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--path", (char*) nullptr)) {
      path = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--assume-role-policy-doc", (char*) nullptr)) {
      assume_role_doc = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--policy-name", (char*) nullptr)) {
      policy_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--policy-doc", (char*) nullptr)) {
      perm_policy_doc = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--path-prefix", (char*) nullptr)) {
      path_prefix = val;
    } else {
      ++i;
    }
  }
  return 0;
}

int parse_user_commandline_params(std::vector<const char*>& args,
                                  std::string& subuser, std::string& display_name,
                                  std::string& user_email, RGWUserAdminOpState& user_op,
                                  std::string& caps, std::string& op_mask_str, int& key_type,
                                  int& purge_keys, int& max_buckets, bool& max_buckets_specified,
                                  int& admin, bool& admin_specified, int& system, bool& system_specified,
                                  bool& set_temp_url_key, map<int, std::string>& temp_url_keys,
                                  std::string& access, uint32_t& perm_mask, bool& set_perm,
                                  int& sync_stats)
{
  std::string val, key_type_str, err;
  std::ostringstream errs;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--subuser", (char*) nullptr)) {
      subuser = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-e", "--email", (char*) nullptr)) {
      user_email = val;
      user_op.user_email_specified=true;
    } else if (ceph_argparse_witharg(args, i, &val, "-n", "--display-name", (char*) nullptr)) {
      display_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--op-mask", (char*) nullptr)) {
      op_mask_str = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--key-type", (char*) nullptr)) {
      key_type_str = val;
      if (key_type_str.compare("swift") == 0) {
        key_type = KEY_TYPE_SWIFT;
      } else if (key_type_str.compare("s3") == 0) {
        key_type = KEY_TYPE_S3;
      } else {
        cerr << "bad key type: " << key_type_str << std::endl;
        exit(1)
      }
    } else if (ceph_argparse_binary_flag(args, i, &admin, nullptr, "--admin", (char*) nullptr)) {
      admin_specified = true;
    } else if (ceph_argparse_binary_flag(args, i, &system, nullptr, "--system", (char*) nullptr)) {
      system_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-buckets", (char*) nullptr)) {
      max_buckets = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max buckets: " << err << std::endl;
        return EINVAL;
      }
      max_buckets_specified = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--access", (char*) nullptr)) {
      access = val;
      perm_mask = rgw_str_to_perm(access.c_str());
      set_perm = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--temp-url-key", (char*) nullptr)) {
      temp_url_keys[0] = val;
      set_temp_url_key = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--temp-url-key2", "--temp-url-key-2", (char*) nullptr)) {
      temp_url_keys[1] = val;
      set_temp_url_key = true;
    } else if (ceph_argparse_binary_flag(args, i, &purge_keys, nullptr, "--purge-keys", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &sync_stats, nullptr, "--sync-stats", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--caps", (char*) nullptr)) {
      caps = val;
    } else {
      ++i;
    }
  }
  return 0;
}

int parse_other_commandline_params(std::vector<const char*>& args, std::string& pool_name,
                                   rgw_pool& pool, std::string& daemon_id, bool& specified_daemon_id,
                                   std::string& replica_log_type_str,
                                   ReplicaLogType& replica_log_type, int& extra_info,
                                   int& include_all, std::string& client_id, std::string& op_id,
                                   std::string& state_str, int& show_log_entries, int& show_log_sum,
                                   int& skip_zero_entries, std::map<std::string, bool>& categories,
                                   uint64_t& orphan_stale_secs, std::string& job_id)
{
  std::string val, key_type_str, err;
  std::ostringstream errs;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*) nullptr)) {
      pool_name = val;
      pool = rgw_pool(pool_name);
    } else if (ceph_argparse_witharg(args, i, &val, "--client-id", (char*) nullptr)) {
      client_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--op-id", (char*) nullptr)) {
      op_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--state", (char*) nullptr)) {
      state_str = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--job-id", (char*) nullptr)) {
      job_id = val;
    } else if (ceph_argparse_binary_flag(args, i, &show_log_entries, nullptr, "--show-log-entries", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &show_log_sum, nullptr, "--show-log-sum", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &skip_zero_entries, nullptr, "--skip-zero-entries", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--orphan-stale-secs", (char*) nullptr)) {
      orphan_stale_secs = (uint64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse orphan stale secs: " << err << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--daemon-id", (char*) nullptr)) {
      daemon_id = val;
      specified_daemon_id = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--categories", (char*) nullptr)) {
      std::string cat_str = val;
      list<std::string> cat_list;
      list<std::string>::iterator iter;
      get_str_list(cat_str, cat_list);
      for (iter = cat_list.begin(); iter != cat_list.end(); ++iter) {
        categories[*iter] = true;
      }
    } else if (ceph_argparse_binary_flag(args, i, &include_all, nullptr, "--include-all", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &extra_info, nullptr, "--extra-info", (char*) nullptr)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--replica-log-type", (char*) nullptr)) {
      replica_log_type_str = val;
      replica_log_type = get_replicalog_type(replica_log_type_str);
      if (replica_log_type == ReplicaLog_Invalid) {
        cerr << "ERROR: invalid replica log type" << std::endl;
        return EINVAL;
      }
    } else {
      ++i;
    }
  }
  return 0;
}

const std::unordered_map<std::string, RgwAdminCommandGroup> RgwAdminCommandGroupHandlerFactory::str_to_rgw_command_group = {
    {"bi", BI},
    {"bilog", BILOG},
    {"bucket", BUCKET},
    {"bucket sync", BUCKET_SYNC},
    {"object", OBJECT},
    {"objects", OBJECT},
    {"policy", POLICY},
    {"reshard", RESHARD},
    {"data sync", DATA_SYNC},
    {"metadata sync", METADATA_SYNC},
    {"period", PERIOD},
    {"realm", REALM},
    {"zone", ZONE},
    {"zone placement", ZONE_PLACEMENT},
    {"zonegroup", ZONEGROUP},
    {"zonegroup placement", ZONEGROUP_PLACEMENT},
    {"global quota", GLOBAL_QUOTA},
    {"quota", QUOTA},
    {"role", ROLE},
    {"role-policy", ROLE_POLICY},
    {"user", USER},
    {"subuser", SUBUSER},
    {"caps", CAPS},
    {"key", KEY},
    {"datalog", DATALOG},
    {"log", LOG},
    {"mdlog", MDLOG},
    {"replicalog", REPLICALOG},
    {"gc", GC},
    {"lc", LC},
    {"metadata", METADATA},
    {"olh", OLH},
    {"opstate", OPSTATE},
    {"orphans", ORPHANS},
    {"pool", POOL},
    {"sync", SYNC},
    {"usage", USAGE},
    {"user list", USER_LIST},
};

RgwAdminCommandGroup RgwAdminCommandGroupHandlerFactory::parse_command_group(
    std::vector<const char*>& args, std::vector<std::string>& command_prefix) {
  const char COMMAND_GROUP[] = "command";
  std::vector<std::string> command;
  boost::program_options::options_description desc{"General options"};
  desc.add_options()
      ("help,h", "Help screen")
      (COMMAND_GROUP, boost::program_options::value(&command), "Command");

  boost::program_options::positional_options_description pos_desc;
  pos_desc.add(COMMAND_GROUP, -1);
  boost::program_options::variables_map var_map;
  try {
    boost::program_options::parsed_options options = boost::program_options::command_line_parser{args.size(), args.data()}
        .options(desc)
        .positional(pos_desc)
        .allow_unregistered()
        .run();

    boost::program_options::store(options, var_map);
    boost::program_options::notify(var_map);

    if (var_map.count("help")) {
      usage();
    }
    else if (var_map.count(COMMAND_GROUP)) {
      std::string first_word = command[0];
      command_prefix.push_back(first_word);
      if (command.size() == 1) {
        // Will throw an exception if such a command group is not found. Since an exception could
        // also be thrown by boost::program_options, fail instead of introducing additional checks in the code.
        return str_to_rgw_command_group.at(first_word);
      } else {
        std::string maybe_second_word = command[1];
        if (str_to_rgw_command_group.count(first_word + " " + maybe_second_word) > 0) {
          command_prefix.push_back(maybe_second_word);
          return str_to_rgw_command_group.at(first_word + " " + maybe_second_word);
        } else {
          // Will throw an exception if such a command group is not found. Since an exception could
          // also be thrown by boost::program_options, fail instead of introducing additional checks in the code.
          return str_to_rgw_command_group.at(first_word);
        }
      }
    } else {
      std::cout << "No command and no help request provided." << std::endl;
      usage();
    }
  } catch (const std::exception& ex) {
    std::cout << "Incorrect command:" << std::endl << desc << std::endl;
  }
  return INVALID;
}

RgwAdminCommandGroupHandler* RgwAdminCommandGroupHandlerFactory::get_command_group_handler(std::vector<const char*>& args) {
  // TODO: pass pointers to RGWRados and Formatter
  std::vector<std::string> command_prefix;
  RgwAdminCommandGroup command_group = RgwAdminCommandGroupHandlerFactory::parse_command_group(args, command_prefix);
  std::cout << "Command group: " << command_group << std::endl;
  switch (command_group) {
    case(INVALID) : return nullptr;
    case (BI) :
      return new RgwAdminBiCommandsHandler(args, command_prefix, nullptr, nullptr);
    case (BILOG) :
      return new RgwAdminBilogCommandsHandler(args, command_prefix, nullptr, nullptr);
    case (BUCKET) :
      return new RgwAdminBucketCommandsHandler(args, command_prefix, nullptr, nullptr);
    case (BUCKET_SYNC) :
      return new RgwAdminBucketSyncCommandsHandler(args, command_prefix, nullptr, nullptr);
    case(CAPS) : return nullptr;
    case(DATA_SYNC) : return nullptr;
    case(DATALOG) : return nullptr;
    case(GC) : return nullptr;
    case(GLOBAL_QUOTA) : return nullptr;
    case(KEY) : return nullptr;
    case(LC) : return nullptr;
    case(LOG) : return nullptr;
    case(MDLOG) : return nullptr;
    case (METADATA) :
      return new RgwAdminMetadataCommandsHandler(args, command_prefix, nullptr, nullptr);
    case (METADATA_SYNC) :
      return new RgwAdminMetadataSyncCommandsHandler(args, command_prefix, nullptr, nullptr);
    case (OBJECT) :
      return new RgwAdminObjectCommandsHandler(args, command_prefix, nullptr, nullptr);
    case(OLH) : return nullptr;
    case(OPSTATE) : return nullptr;
    case(ORPHANS) : return nullptr;
    case (PERIOD) :
      return new RgwAdminPeriodCommandsHandler(args, command_prefix, nullptr, nullptr);
    case(POLICY) : return nullptr;
    case(POOL) : return nullptr;
    case(QUOTA) : return nullptr;
    case (REALM) :
      return new RgwAdminRealmCommandsHandler(args, command_prefix, nullptr, nullptr);
    case(REPLICALOG) : return nullptr;
    case (RESHARD) :
      return new RgwAdminReshardCommandsHandler(args, command_prefix, nullptr, nullptr);
    case(ROLE) : return nullptr;
    case(ROLE_POLICY) : return nullptr;
    case(SUBUSER) : return nullptr;
    case(SYNC) : return nullptr;
    case(USAGE) : return nullptr;
    case(USER) : return nullptr;
    case(USER_LIST) : return nullptr;
    case(ZONE) : return nullptr;
    case(ZONE_PLACEMENT) : return nullptr;
    case(ZONEGROUP) : return nullptr;
    case(ZONEGROUP_PLACEMENT) : return nullptr;
  }
  return nullptr;
}









