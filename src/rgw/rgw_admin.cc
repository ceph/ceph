// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "cls/rgw/cls_rgw_types.h"
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
#include "rgw_datalog.h"
#include "rgw_lc.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_orphan.h"
#include "rgw_sync.h"
#include "rgw_trim_bilog.h"
#include "rgw_trim_datalog.h"
#include "rgw_trim_mdlog.h"
#include "rgw_data_sync.h"
#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"
#include "rgw_role.h"
#include "rgw_reshard.h"
#include "rgw_http_client_curl.h"
#include "rgw_zone.h"
#include "rgw_pubsub.h"
#include "rgw_bucket_sync.h"
#include "rgw_sync_checkpoint.h"
#include "rgw_lua.h"

#include "services/svc_sync_modules.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta_be_otp.h"
#include "services/svc_zone.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

using namespace std;

static rgw::sal::Store* store = NULL;

static const DoutPrefixProvider* dpp() {
  struct GlobalPrefix : public DoutPrefixProvider {
    CephContext *get_cct() const override { return dout_context; }
    unsigned get_subsys() const override { return dout_subsys; }
    std::ostream& gen_prefix(std::ostream& out) const override { return out; }
  };
  static GlobalPrefix global_dpp;
  return &global_dpp;
}

#define CHECK_TRUE(x, msg, err) \
  do { \
    if (!x) { \
      cerr << msg << std::endl; \
      return err; \
    } \
  } while (0)

#define CHECK_SUCCESS(x, msg) \
  do { \
    int _x_val = (x); \
    if (_x_val < 0) { \
      cerr << msg << ": " << cpp_strerror(-_x_val) << std::endl; \
      return _x_val; \
    } \
  } while (0)

void usage()
{
  cout << "usage: radosgw-admin <cmd> [options...]" << std::endl;
  cout << "commands:\n";
  cout << "  user create                create a new user\n" ;
  cout << "  user modify                modify user\n";
  cout << "  user info                  get user info\n";
  cout << "  user rename                rename user\n";
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
  cout << "  bucket list                list buckets (specify --allow-unordered for\n";
  cout << "                             faster, unsorted listing)\n";
  cout << "  bucket limit check         show bucket sharding stats\n";
  cout << "  bucket link                link bucket to specified user\n";
  cout << "  bucket unlink              unlink bucket from specified user\n";
  cout << "  bucket stats               returns bucket statistics\n";
  cout << "  bucket rm                  remove bucket\n";
  cout << "  bucket check               check bucket index\n";
  cout << "  bucket chown               link bucket to specified user and update its object ACLs\n";
  cout << "  bucket reshard             reshard bucket\n";
  cout << "  bucket rewrite             rewrite all objects in the specified bucket\n";
  cout << "  bucket sync checkpoint     poll a bucket's sync status until it catches up to its remote\n";
  cout << "  bucket sync disable        disable bucket sync\n";
  cout << "  bucket sync enable         enable bucket sync\n";
  cout << "  bucket radoslist           list rados objects backing bucket's objects\n";
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
  cout << "  objects expire-stale list  list stale expired objects (caused by reshard)\n";
  cout << "  objects expire-stale rm    remove stale expired objects\n";
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
  cout << "  ratelimit get              get ratelimit params\n";
  cout << "  ratelimit set              set ratelimit params\n";
  cout << "  ratelimit enable           enable ratelimit\n";
  cout << "  ratelimit disable          disable ratelimit\n";
  cout << "  global quota get           view global quota params\n";
  cout << "  global quota set           set global quota params\n";
  cout << "  global quota enable        enable a global quota\n";
  cout << "  global quota disable       disable a global quota\n";
  cout << "  global ratelimit get       view global ratelimit params\n";
  cout << "  global ratelimit set       set global ratelimit params\n";
  cout << "  global ratelimit enable    enable a ratelimit quota\n";
  cout << "  global ratelimit disable   disable a ratelimit quota\n";
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
  cout << "  zonegroup delete           delete a zone group info\n";
  cout << "  zonegroup get              show zone group info\n";
  cout << "  zonegroup modify           modify an existing zonegroup\n";
  cout << "  zonegroup set              set zone group info (requires infile)\n";
  cout << "  zonegroup rm               remove a zone from a zonegroup\n";
  cout << "  zonegroup rename           rename a zone group\n";
  cout << "  zonegroup list             list all zone groups set on this cluster\n";
  cout << "  zonegroup placement list   list zonegroup's placement targets\n";
  cout << "  zonegroup placement get    get a placement target of a specific zonegroup\n";
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
  cout << "  zone placement get         get a zone placement target\n";
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
  cout << "  lc reshard fix             fix LC for a resharded bucket\n";
  cout << "  metadata get               get metadata info\n";
  cout << "  metadata put               put metadata info\n";
  cout << "  metadata rm                remove metadata info\n";
  cout << "  metadata list              list metadata info\n";
  cout << "  mdlog list                 list metadata log\n";
  cout << "  mdlog autotrim             auto trim metadata log\n";
  cout << "  mdlog trim                 trim metadata log (use marker)\n";
  cout << "  mdlog status               read metadata log status\n";
  cout << "  bilog list                 list bucket index log\n";
  cout << "  bilog trim                 trim bucket index log (use start-marker, end-marker)\n";
  cout << "  bilog status               read bucket index log status\n";
  cout << "  bilog autotrim             auto trim bucket index log\n";
  cout << "  datalog list               list data log\n";
  cout << "  datalog trim               trim data log\n";
  cout << "  datalog status             read data log status\n";
  cout << "  datalog type               change datalog type to --log_type={fifo,omap}\n";
  cout << "  orphans find               deprecated -- init and run search for leaked rados objects (use job-id, pool)\n";
  cout << "  orphans finish             deprecated -- clean up search for leaked rados objects\n";
  cout << "  orphans list-jobs          deprecated -- list the current job-ids for orphans search\n";
  cout << "                           * the three 'orphans' sub-commands are now deprecated; consider using the `rgw-orphan-list` tool\n";
  cout << "  role create                create a AWS role for use with STS\n";
  cout << "  role delete                remove a role\n";
  cout << "  role get                   get a role\n";
  cout << "  role list                  list roles with specified path prefix\n";
  cout << "  role modify                modify the assume role policy of an existing role\n";
  cout << "  role-policy put            add/update permission policy to role\n";
  cout << "  role-policy list           list policies attached to a role\n";
  cout << "  role-policy get            get the specified inline policy document embedded with the given role\n";
  cout << "  role-policy delete         remove policy attached to a role\n";
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
  cout << "  topic list                 list bucket notifications/pubsub topics\n";
  cout << "  topic get                  get a bucket notifications/pubsub topic\n";
  cout << "  topic rm                   remove a bucket notifications/pubsub topic\n";
  cout << "  subscription get           get a pubsub subscription definition\n";
  cout << "  subscription rm            remove a pubsub subscription\n";
  cout << "  subscription pull          show events in a pubsub subscription\n";
  cout << "  subscription ack           ack (remove) an events in a pubsub subscription\n";
  cout << "  script put                 upload a lua script to a context\n";
  cout << "  script get                 get the lua script of a context\n";
  cout << "  script rm                  remove the lua scripts of a context\n";
  cout << "  script-package add         add a lua package to the scripts allowlist\n";
  cout << "  script-package rm          remove a lua package from the scripts allowlist\n";
  cout << "  script-package list        get the lua packages allowlist\n";
  cout << "options:\n";
  cout << "   --tenant=<tenant>         tenant name\n";
  cout << "   --user_ns=<namespace>     namespace of user (oidc in case of users authenticated with oidc provider)\n";
  cout << "   --uid=<id>                user id\n";
  cout << "   --new-uid=<id>            new user id\n";
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
  cout << "   --object-version=<version>         object version\n";
  cout << "   --date=<date>             date in the format yyyy-mm-dd\n";
  cout << "   --start-date=<date>       start date in the format yyyy-mm-dd\n";
  cout << "   --end-date=<date>         end date in the format yyyy-mm-dd\n";
  cout << "   --bucket-id=<bucket-id>   bucket id\n";
  cout << "   --bucket-new-name=<bucket>\n";
  cout << "                             for bucket link: optional new name\n";
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
  cout << "   --bucket-index-max-shards override a zone/zonegroup's default bucket index shard count\n";
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
  cout << "   --show-config             show configuration\n";
  cout << "   --show-log-entries=<flag> enable/disable dump of log entries on log show\n";
  cout << "   --show-log-sum=<flag>     enable/disable dump of log summation on log show\n";
  cout << "   --skip-zero-entries       log show only dumps entries that don't have zero value\n";
  cout << "                             in one of the numeric field\n";
  cout << "   --infile=<file>           specify a file to read in when setting data\n";
  cout << "   --categories=<list>       comma separated list of categories, used in usage show\n";
  cout << "   --caps=<caps>             list of caps (e.g., \"usage=read, write; user=read\")\n";
  cout << "   --op-mask=<op-mask>       permission of user's operations (e.g., \"read, write, delete, *\")\n";
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
  cout << "   --max-concurrent-ios      maximum concurrent ios for bucket operations (default: 32)\n";
  cout << "\n";
  cout << "<date> := \"YYYY-MM-DD[ hh:mm:ss]\"\n";
  cout << "\nQuota options:\n";
  cout << "   --max-objects             specify max objects (negative value to disable)\n";
  cout << "   --max-size                specify max size (in B/K/M/G/T, negative value to disable)\n";
  cout << "   --quota-scope             scope of quota (bucket, user)\n";
  cout << "\nRate limiting options:\n";
  cout << "   --max-read-ops            specify max requests per minute for READ ops per RGW (GET and HEAD request methods), 0 means unlimited\n";
  cout << "   --max-read-bytes          specify max bytes per minute for READ ops per RGW (GET and HEAD request methods), 0 means unlimited\n";
  cout << "   --max-write-ops           specify max requests per minute for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited\n";
  cout << "   --max-write-bytes         specify max bytes per minute for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited\n";
  cout << "   --ratelimit-scope         scope of rate limiting: bucket, user, anonymous\n";
  cout << "                             anonymous can be configured only with global rate limit\n";
  cout << "\nOrphans search options:\n";
  cout << "   --num-shards              num of shards to use for keeping the temporary scan info\n";
  cout << "   --orphan-stale-secs       num of seconds to wait before declaring an object to be an orphan (default: 86400)\n";
  cout << "   --job-id                  set the job id (for orphans find)\n";
  cout << "   --detail                  detailed mode, log and stat head objects as well\n";
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
  cout << "\nBucket notifications/pubsub options:\n";
  cout << "   --topic                   bucket notifications/pubsub topic name\n";
  cout << "   --subscription            pubsub subscription name\n";
  cout << "   --event-id                event id in a pubsub subscription\n";
  cout << "\nScript options:\n";
  cout << "   --context                 context in which the script runs. one of: preRequest, postRequest\n";
  cout << "   --package                 name of the lua package that should be added/removed to/from the allowlist\n";
  cout << "   --allow-compilation       package is allowed to compile C code as part of its installation\n";
  cout << "\nradoslist options:\n";
  cout << "   --rgw-obj-fs              the field separator that will separate the rados\n";
  cout << "                             object name from the rgw object name;\n";
  cout << "                             additionally rados objects for incomplete\n";
  cout << "                             multipart uploads will not be output\n";
  cout << "\n";
  generic_client_usage();
}


class SimpleCmd {
public:
  struct Def {
    string cmd;
    std::any opt;
  };

  using Aliases = std::vector<std::set<string> >;
  using Commands = std::vector<Def>;

private:
  struct Node {
    map<string, Node> next;
    set<string> expected; /* separate un-normalized list */
    std::any opt;
  };

  Node cmd_root;
  map<string, string> alias_map;

  string normalize_alias(const string& s) const {
    auto iter = alias_map.find(s);
    if (iter == alias_map.end()) {
      return s;
    }

    return iter->second;
  }
  void init_alias_map(Aliases& aliases) {
    for (auto& alias_set : aliases) {
      std::optional<string> first;

      for (auto& alias : alias_set) {
        if (!first) {
          first = alias;
        } else {
          alias_map[alias] = *first;
        }
      }
    }
  }

  bool gen_next_expected(Node *node, vector<string> *expected, bool ret) {
    for (auto& next_cmd : node->expected) {
      expected->push_back(next_cmd);
    }
    return ret;
  }

  Node root;

public:
  SimpleCmd() {}

  SimpleCmd(std::optional<Commands> cmds,
            std::optional<Aliases> aliases) {
    if (aliases) {
      add_aliases(*aliases);
    }

    if (cmds) {
      add_commands(*cmds);
    }
  }

  void add_aliases(Aliases& aliases) {
    init_alias_map(aliases);
  }

  void add_commands(std::vector<Def>& cmds) {
    for (auto& cmd : cmds) {
      vector<string> words;
      get_str_vec(cmd.cmd, " ", words);

      auto node = &cmd_root;
      for (auto& word : words) {
        auto norm = normalize_alias(word);
        auto parent = node;

        node->expected.insert(word);

        node = &node->next[norm];

        if (norm == "[*]") { /* optional param at the end */
          parent->next["*"] = *node; /* can be also looked up by '*' */
          parent->opt = cmd.opt;
        }
      }

      node->opt = cmd.opt;
    }
  }

  template <class Container>
  bool find_command(Container& args,
                    std::any *opt_cmd,
                    vector<string> *extra_args,
                    string *error,
                    vector<string> *expected) {
    auto node = &cmd_root;

    std::optional<std::any> found_opt;

    for (auto& arg : args) {
      string norm = normalize_alias(arg);
      auto iter = node->next.find(norm);
      if (iter == node->next.end()) {
        iter = node->next.find("*");
        if (iter == node->next.end()) {
          *error = string("ERROR: Unrecognized argument: '") + arg + "'";
          return gen_next_expected(node, expected, false);
        }
        extra_args->push_back(arg);
        if (!found_opt) {
          found_opt = node->opt;
        }
      }
      node = &(iter->second);
    }

    *opt_cmd = found_opt.value_or(node->opt);

    if (!opt_cmd->has_value()) {
      *error ="ERROR: Unknown command";
      return gen_next_expected(node, expected, false);
    }

    return true;
  }
};


namespace rgw_admin {

enum class OPT {
  NO_CMD,
  USER_CREATE,
  USER_INFO,
  USER_MODIFY,
  USER_RENAME,
  USER_RM,
  USER_SUSPEND,
  USER_ENABLE,
  USER_CHECK,
  USER_STATS,
  USER_LIST,
  SUBUSER_CREATE,
  SUBUSER_MODIFY,
  SUBUSER_RM,
  KEY_CREATE,
  KEY_RM,
  BUCKETS_LIST,
  BUCKET_LIMIT_CHECK,
  BUCKET_LINK,
  BUCKET_UNLINK,
  BUCKET_STATS,
  BUCKET_CHECK,
  BUCKET_SYNC_CHECKPOINT,
  BUCKET_SYNC_INFO,
  BUCKET_SYNC_STATUS,
  BUCKET_SYNC_MARKERS,
  BUCKET_SYNC_INIT,
  BUCKET_SYNC_RUN,
  BUCKET_SYNC_DISABLE,
  BUCKET_SYNC_ENABLE,
  BUCKET_RM,
  BUCKET_REWRITE,
  BUCKET_RESHARD,
  BUCKET_CHOWN,
  BUCKET_RADOS_LIST,
  POLICY,
  POOL_ADD,
  POOL_RM,
  POOLS_LIST,
  LOG_LIST,
  LOG_SHOW,
  LOG_RM,
  USAGE_SHOW,
  USAGE_TRIM,
  USAGE_CLEAR,
  OBJECT_PUT,
  OBJECT_RM,
  OBJECT_UNLINK,
  OBJECT_STAT,
  OBJECT_REWRITE,
  OBJECTS_EXPIRE,
  OBJECTS_EXPIRE_STALE_LIST,
  OBJECTS_EXPIRE_STALE_RM,
  BI_GET,
  BI_PUT,
  BI_LIST,
  BI_PURGE,
  OLH_GET,
  OLH_READLOG,
  QUOTA_SET,
  QUOTA_ENABLE,
  QUOTA_DISABLE,
  GC_LIST,
  GC_PROCESS,
  LC_LIST,
  LC_GET,
  LC_PROCESS,
  LC_RESHARD_FIX,
  ORPHANS_FIND,
  ORPHANS_FINISH,
  ORPHANS_LIST_JOBS,
  RATELIMIT_GET,
  RATELIMIT_SET,
  RATELIMIT_ENABLE,
  RATELIMIT_DISABLE,
  ZONEGROUP_ADD,
  ZONEGROUP_CREATE,
  ZONEGROUP_DEFAULT,
  ZONEGROUP_DELETE,
  ZONEGROUP_GET,
  ZONEGROUP_MODIFY,
  ZONEGROUP_SET,
  ZONEGROUP_LIST,
  ZONEGROUP_REMOVE,
  ZONEGROUP_RENAME,
  ZONEGROUP_PLACEMENT_ADD,
  ZONEGROUP_PLACEMENT_MODIFY,
  ZONEGROUP_PLACEMENT_RM,
  ZONEGROUP_PLACEMENT_LIST,
  ZONEGROUP_PLACEMENT_GET,
  ZONEGROUP_PLACEMENT_DEFAULT,
  ZONE_CREATE,
  ZONE_DELETE,
  ZONE_GET,
  ZONE_MODIFY,
  ZONE_SET,
  ZONE_LIST,
  ZONE_RENAME,
  ZONE_DEFAULT,
  ZONE_PLACEMENT_ADD,
  ZONE_PLACEMENT_MODIFY,
  ZONE_PLACEMENT_RM,
  ZONE_PLACEMENT_LIST,
  ZONE_PLACEMENT_GET,
  CAPS_ADD,
  CAPS_RM,
  METADATA_GET,
  METADATA_PUT,
  METADATA_RM,
  METADATA_LIST,
  METADATA_SYNC_STATUS,
  METADATA_SYNC_INIT,
  METADATA_SYNC_RUN,
  MDLOG_LIST,
  MDLOG_AUTOTRIM,
  MDLOG_TRIM,
  MDLOG_FETCH,
  MDLOG_STATUS,
  SYNC_ERROR_LIST,
  SYNC_ERROR_TRIM,
  SYNC_GROUP_CREATE,
  SYNC_GROUP_MODIFY,
  SYNC_GROUP_GET,
  SYNC_GROUP_REMOVE,
  SYNC_GROUP_FLOW_CREATE,
  SYNC_GROUP_FLOW_REMOVE,
  SYNC_GROUP_PIPE_CREATE,
  SYNC_GROUP_PIPE_MODIFY,
  SYNC_GROUP_PIPE_REMOVE,
  SYNC_POLICY_GET,
  BILOG_LIST,
  BILOG_TRIM,
  BILOG_STATUS,
  BILOG_AUTOTRIM,
  DATA_SYNC_STATUS,
  DATA_SYNC_INIT,
  DATA_SYNC_RUN,
  DATALOG_LIST,
  DATALOG_STATUS,
  DATALOG_AUTOTRIM,
  DATALOG_TRIM,
  DATALOG_TYPE,
  DATALOG_PRUNE,
  REALM_CREATE,
  REALM_DELETE,
  REALM_GET,
  REALM_GET_DEFAULT,
  REALM_LIST,
  REALM_LIST_PERIODS,
  REALM_RENAME,
  REALM_SET,
  REALM_DEFAULT,
  REALM_PULL,
  PERIOD_DELETE,
  PERIOD_GET,
  PERIOD_GET_CURRENT,
  PERIOD_PULL,
  PERIOD_PUSH,
  PERIOD_LIST,
  PERIOD_UPDATE,
  PERIOD_COMMIT,
  GLOBAL_QUOTA_GET,
  GLOBAL_QUOTA_SET,
  GLOBAL_QUOTA_ENABLE,
  GLOBAL_QUOTA_DISABLE,
  GLOBAL_RATELIMIT_GET,
  GLOBAL_RATELIMIT_SET,
  GLOBAL_RATELIMIT_ENABLE,
  GLOBAL_RATELIMIT_DISABLE,
  SYNC_INFO,
  SYNC_STATUS,
  ROLE_CREATE,
  ROLE_DELETE,
  ROLE_GET,
  ROLE_MODIFY,
  ROLE_LIST,
  ROLE_POLICY_PUT,
  ROLE_POLICY_LIST,
  ROLE_POLICY_GET,
  ROLE_POLICY_DELETE,
  RESHARD_ADD,
  RESHARD_LIST,
  RESHARD_STATUS,
  RESHARD_PROCESS,
  RESHARD_CANCEL,
  MFA_CREATE,
  MFA_REMOVE,
  MFA_GET,
  MFA_LIST,
  MFA_CHECK,
  MFA_RESYNC,
  RESHARD_STALE_INSTANCES_LIST,
  RESHARD_STALE_INSTANCES_DELETE,
  PUBSUB_TOPICS_LIST,
  // TODO add "subscription list" command
  PUBSUB_TOPIC_GET,
  PUBSUB_TOPIC_RM,
  PUBSUB_SUB_GET,
  PUBSUB_SUB_RM,
  PUBSUB_SUB_PULL,
  PUBSUB_EVENT_RM,
  SCRIPT_PUT,
  SCRIPT_GET,
  SCRIPT_RM,
  SCRIPT_PACKAGE_ADD,
  SCRIPT_PACKAGE_RM,
  SCRIPT_PACKAGE_LIST
};

}

using namespace rgw_admin;

static SimpleCmd::Commands all_cmds = {
  { "user create", OPT::USER_CREATE },
  { "user info", OPT::USER_INFO },
  { "user modify", OPT::USER_MODIFY },
  { "user rename", OPT::USER_RENAME },
  { "user rm", OPT::USER_RM },
  { "user suspend", OPT::USER_SUSPEND },
  { "user enable", OPT::USER_ENABLE },
  { "user check", OPT::USER_CHECK },
  { "user stats", OPT::USER_STATS },
  { "user list", OPT::USER_LIST },
  { "subuser create", OPT::SUBUSER_CREATE },
  { "subuser modify", OPT::SUBUSER_MODIFY },
  { "subuser rm", OPT::SUBUSER_RM },
  { "key create", OPT::KEY_CREATE },
  { "key rm", OPT::KEY_RM },
  { "buckets list", OPT::BUCKETS_LIST },
  { "bucket list", OPT::BUCKETS_LIST },
  { "bucket limit check", OPT::BUCKET_LIMIT_CHECK },
  { "bucket link", OPT::BUCKET_LINK },
  { "bucket unlink", OPT::BUCKET_UNLINK },
  { "bucket stats", OPT::BUCKET_STATS },
  { "bucket check", OPT::BUCKET_CHECK },
  { "bucket sync checkpoint", OPT::BUCKET_SYNC_CHECKPOINT },
  { "bucket sync info", OPT::BUCKET_SYNC_INFO },
  { "bucket sync status", OPT::BUCKET_SYNC_STATUS },
  { "bucket sync markers", OPT::BUCKET_SYNC_MARKERS },
  { "bucket sync init", OPT::BUCKET_SYNC_INIT },
  { "bucket sync run", OPT::BUCKET_SYNC_RUN },
  { "bucket sync disable", OPT::BUCKET_SYNC_DISABLE },
  { "bucket sync enable", OPT::BUCKET_SYNC_ENABLE },
  { "bucket rm", OPT::BUCKET_RM },
  { "bucket rewrite", OPT::BUCKET_REWRITE },
  { "bucket reshard", OPT::BUCKET_RESHARD },
  { "bucket chown", OPT::BUCKET_CHOWN },
  { "bucket radoslist", OPT::BUCKET_RADOS_LIST },
  { "bucket rados list", OPT::BUCKET_RADOS_LIST },
  { "policy", OPT::POLICY },
  { "pool add", OPT::POOL_ADD },
  { "pool rm", OPT::POOL_RM },
  { "pool list", OPT::POOLS_LIST },
  { "pools list", OPT::POOLS_LIST },
  { "log list", OPT::LOG_LIST },
  { "log show", OPT::LOG_SHOW },
  { "log rm", OPT::LOG_RM },
  { "usage show", OPT::USAGE_SHOW },
  { "usage trim", OPT::USAGE_TRIM },
  { "usage clear", OPT::USAGE_CLEAR },
  { "object put", OPT::OBJECT_PUT },
  { "object rm", OPT::OBJECT_RM },
  { "object unlink", OPT::OBJECT_UNLINK },
  { "object stat", OPT::OBJECT_STAT },
  { "object rewrite", OPT::OBJECT_REWRITE },
  { "objects expire", OPT::OBJECTS_EXPIRE },
  { "objects expire-stale list", OPT::OBJECTS_EXPIRE_STALE_LIST },
  { "objects expire-stale rm", OPT::OBJECTS_EXPIRE_STALE_RM },
  { "bi get", OPT::BI_GET },
  { "bi put", OPT::BI_PUT },
  { "bi list", OPT::BI_LIST },
  { "bi purge", OPT::BI_PURGE },
  { "olh get", OPT::OLH_GET },
  { "olh readlog", OPT::OLH_READLOG },
  { "quota set", OPT::QUOTA_SET },
  { "quota enable", OPT::QUOTA_ENABLE },
  { "quota disable", OPT::QUOTA_DISABLE },
  { "ratelimit get", OPT::RATELIMIT_GET },
  { "ratelimit set", OPT::RATELIMIT_SET },
  { "ratelimit enable", OPT::RATELIMIT_ENABLE },
  { "ratelimit disable", OPT::RATELIMIT_DISABLE },
  { "gc list", OPT::GC_LIST },
  { "gc process", OPT::GC_PROCESS },
  { "lc list", OPT::LC_LIST },
  { "lc get", OPT::LC_GET },
  { "lc process", OPT::LC_PROCESS },
  { "lc reshard fix", OPT::LC_RESHARD_FIX },
  { "orphans find", OPT::ORPHANS_FIND },
  { "orphans finish", OPT::ORPHANS_FINISH },
  { "orphans list jobs", OPT::ORPHANS_LIST_JOBS },
  { "orphans list-jobs", OPT::ORPHANS_LIST_JOBS },
  { "zonegroup add", OPT::ZONEGROUP_ADD },
  { "zonegroup create", OPT::ZONEGROUP_CREATE },
  { "zonegroup default", OPT::ZONEGROUP_DEFAULT },
  { "zonegroup delete", OPT::ZONEGROUP_DELETE },
  { "zonegroup get", OPT::ZONEGROUP_GET },
  { "zonegroup modify", OPT::ZONEGROUP_MODIFY },
  { "zonegroup set", OPT::ZONEGROUP_SET },
  { "zonegroup list", OPT::ZONEGROUP_LIST },
  { "zonegroups list", OPT::ZONEGROUP_LIST },
  { "zonegroup remove", OPT::ZONEGROUP_REMOVE },
  { "zonegroup remove zone", OPT::ZONEGROUP_REMOVE },
  { "zonegroup rename", OPT::ZONEGROUP_RENAME },
  { "zonegroup placement add", OPT::ZONEGROUP_PLACEMENT_ADD },
  { "zonegroup placement modify", OPT::ZONEGROUP_PLACEMENT_MODIFY },
  { "zonegroup placement rm", OPT::ZONEGROUP_PLACEMENT_RM },
  { "zonegroup placement list", OPT::ZONEGROUP_PLACEMENT_LIST },
  { "zonegroup placement get", OPT::ZONEGROUP_PLACEMENT_GET },
  { "zonegroup placement default", OPT::ZONEGROUP_PLACEMENT_DEFAULT },
  { "zone create", OPT::ZONE_CREATE },
  { "zone delete", OPT::ZONE_DELETE },
  { "zone get", OPT::ZONE_GET },
  { "zone modify", OPT::ZONE_MODIFY },
  { "zone set", OPT::ZONE_SET },
  { "zone list", OPT::ZONE_LIST },
  { "zones list", OPT::ZONE_LIST },
  { "zone rename", OPT::ZONE_RENAME },
  { "zone default", OPT::ZONE_DEFAULT },
  { "zone placement add", OPT::ZONE_PLACEMENT_ADD },
  { "zone placement modify", OPT::ZONE_PLACEMENT_MODIFY },
  { "zone placement rm", OPT::ZONE_PLACEMENT_RM },
  { "zone placement list", OPT::ZONE_PLACEMENT_LIST },
  { "zone placement get", OPT::ZONE_PLACEMENT_GET },
  { "caps add", OPT::CAPS_ADD },
  { "caps rm", OPT::CAPS_RM },
  { "metadata get [*]", OPT::METADATA_GET },
  { "metadata put [*]", OPT::METADATA_PUT },
  { "metadata rm [*]", OPT::METADATA_RM },
  { "metadata list [*]", OPT::METADATA_LIST },
  { "metadata sync status", OPT::METADATA_SYNC_STATUS },
  { "metadata sync init", OPT::METADATA_SYNC_INIT },
  { "metadata sync run", OPT::METADATA_SYNC_RUN },
  { "mdlog list", OPT::MDLOG_LIST },
  { "mdlog autotrim", OPT::MDLOG_AUTOTRIM },
  { "mdlog trim", OPT::MDLOG_TRIM },
  { "mdlog fetch", OPT::MDLOG_FETCH },
  { "mdlog status", OPT::MDLOG_STATUS },
  { "sync error list", OPT::SYNC_ERROR_LIST },
  { "sync error trim", OPT::SYNC_ERROR_TRIM },
  { "sync policy get", OPT::SYNC_POLICY_GET },
  { "sync group create", OPT::SYNC_GROUP_CREATE },
  { "sync group modify", OPT::SYNC_GROUP_MODIFY },
  { "sync group get", OPT::SYNC_GROUP_GET },
  { "sync group remove", OPT::SYNC_GROUP_REMOVE },
  { "sync group flow create", OPT::SYNC_GROUP_FLOW_CREATE },
  { "sync group flow remove", OPT::SYNC_GROUP_FLOW_REMOVE },
  { "sync group pipe create", OPT::SYNC_GROUP_PIPE_CREATE },
  { "sync group pipe modify", OPT::SYNC_GROUP_PIPE_MODIFY },
  { "sync group pipe remove", OPT::SYNC_GROUP_PIPE_REMOVE },
  { "bilog list", OPT::BILOG_LIST },
  { "bilog trim", OPT::BILOG_TRIM },
  { "bilog status", OPT::BILOG_STATUS },
  { "bilog autotrim", OPT::BILOG_AUTOTRIM },
  { "data sync status", OPT::DATA_SYNC_STATUS },
  { "data sync init", OPT::DATA_SYNC_INIT },
  { "data sync run", OPT::DATA_SYNC_RUN },
  { "datalog list", OPT::DATALOG_LIST },
  { "datalog status", OPT::DATALOG_STATUS },
  { "datalog autotrim", OPT::DATALOG_AUTOTRIM },
  { "datalog trim", OPT::DATALOG_TRIM },
  { "datalog type", OPT::DATALOG_TYPE },
  { "datalog prune", OPT::DATALOG_PRUNE },
  { "realm create", OPT::REALM_CREATE },
  { "realm rm", OPT::REALM_DELETE },
  { "realm get", OPT::REALM_GET },
  { "realm get default", OPT::REALM_GET_DEFAULT },
  { "realm get-default", OPT::REALM_GET_DEFAULT },
  { "realm list", OPT::REALM_LIST },
  { "realm list periods", OPT::REALM_LIST_PERIODS },
  { "realm list-periods", OPT::REALM_LIST_PERIODS },
  { "realm rename", OPT::REALM_RENAME },
  { "realm set", OPT::REALM_SET },
  { "realm default", OPT::REALM_DEFAULT },
  { "realm pull", OPT::REALM_PULL },
  { "period delete", OPT::PERIOD_DELETE },
  { "period get", OPT::PERIOD_GET },
  { "period get-current", OPT::PERIOD_GET_CURRENT },
  { "period get current", OPT::PERIOD_GET_CURRENT },
  { "period pull", OPT::PERIOD_PULL },
  { "period push", OPT::PERIOD_PUSH },
  { "period list", OPT::PERIOD_LIST },
  { "period update", OPT::PERIOD_UPDATE },
  { "period commit", OPT::PERIOD_COMMIT },
  { "global quota get", OPT::GLOBAL_QUOTA_GET },
  { "global quota set", OPT::GLOBAL_QUOTA_SET },
  { "global quota enable", OPT::GLOBAL_QUOTA_ENABLE },
  { "global quota disable", OPT::GLOBAL_QUOTA_DISABLE },
  { "global ratelimit get", OPT::GLOBAL_RATELIMIT_GET },
  { "global ratelimit set", OPT::GLOBAL_RATELIMIT_SET },
  { "global ratelimit enable", OPT::GLOBAL_RATELIMIT_ENABLE },
  { "global ratelimit disable", OPT::GLOBAL_RATELIMIT_DISABLE },
  { "sync info", OPT::SYNC_INFO },
  { "sync status", OPT::SYNC_STATUS },
  { "role create", OPT::ROLE_CREATE },
  { "role delete", OPT::ROLE_DELETE },
  { "role get", OPT::ROLE_GET },
  { "role modify", OPT::ROLE_MODIFY },
  { "role list", OPT::ROLE_LIST },
  { "role policy put", OPT::ROLE_POLICY_PUT },
  { "role-policy put", OPT::ROLE_POLICY_PUT },
  { "role policy list", OPT::ROLE_POLICY_LIST },
  { "role-policy list", OPT::ROLE_POLICY_LIST },
  { "role policy get", OPT::ROLE_POLICY_GET },
  { "role-policy get", OPT::ROLE_POLICY_GET },
  { "role policy delete", OPT::ROLE_POLICY_DELETE },
  { "role-policy delete", OPT::ROLE_POLICY_DELETE },
  { "reshard bucket", OPT::BUCKET_RESHARD },
  { "reshard add", OPT::RESHARD_ADD },
  { "reshard list", OPT::RESHARD_LIST },
  { "reshard status", OPT::RESHARD_STATUS },
  { "reshard process", OPT::RESHARD_PROCESS },
  { "reshard cancel", OPT::RESHARD_CANCEL },
  { "mfa create", OPT::MFA_CREATE },
  { "mfa remove", OPT::MFA_REMOVE },
  { "mfa get", OPT::MFA_GET },
  { "mfa list", OPT::MFA_LIST },
  { "mfa check", OPT::MFA_CHECK },
  { "mfa resync", OPT::MFA_RESYNC },
  { "reshard stale-instances list", OPT::RESHARD_STALE_INSTANCES_LIST },
  { "reshard stale list", OPT::RESHARD_STALE_INSTANCES_LIST },
  { "reshard stale-instances delete", OPT::RESHARD_STALE_INSTANCES_DELETE },
  { "reshard stale delete", OPT::RESHARD_STALE_INSTANCES_DELETE },
  { "topic list", OPT::PUBSUB_TOPICS_LIST },
  { "topic get", OPT::PUBSUB_TOPIC_GET },
  { "topic rm", OPT::PUBSUB_TOPIC_RM },
  { "subscription get", OPT::PUBSUB_SUB_GET },
  { "subscription rm", OPT::PUBSUB_SUB_RM },
  { "subscription pull", OPT::PUBSUB_SUB_PULL },
  { "subscription ack", OPT::PUBSUB_EVENT_RM },
  { "script put", OPT::SCRIPT_PUT },
  { "script get", OPT::SCRIPT_GET },
  { "script rm", OPT::SCRIPT_RM },
  { "script-package add", OPT::SCRIPT_PACKAGE_ADD },
  { "script-package rm", OPT::SCRIPT_PACKAGE_RM },
  { "script-package list", OPT::SCRIPT_PACKAGE_LIST },
};

static SimpleCmd::Aliases cmd_aliases = {
  { "delete", "del" },
  { "remove", "rm" },
  { "rename", "mv" },
};



BIIndexType get_bi_index_type(const string& type_str) {
  if (type_str == "plain")
    return BIIndexType::Plain;
  if (type_str == "instance")
    return BIIndexType::Instance;
  if (type_str == "olh")
    return BIIndexType::OLH;

  return BIIndexType::Invalid;
}

log_type get_log_type(const string& type_str) {
  if (strcasecmp(type_str.c_str(), "fifo") == 0)
    return log_type::fifo;
  if (strcasecmp(type_str.c_str(), "omap") == 0)
    return log_type::omap;

  return static_cast<log_type>(0xff);
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

static void show_role_info(rgw::sal::RGWRole* role, Formatter* formatter)
{
  formatter->open_object_section("role");
  role->dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
}

static void show_roles_info(vector<std::unique_ptr<rgw::sal::RGWRole>>& roles, Formatter* formatter)
{
  formatter->open_array_section("Roles");
  for (const auto& it : roles) {
    formatter->open_object_section("role");
    it->dump(formatter);
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
    formatter->dump_int("num_shards", entry.num_shards);
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
}

class StoreDestructor {
  rgw::sal::Store* store;
public:
  explicit StoreDestructor(rgw::sal::RadosStore* _s) : store(_s) {}
  ~StoreDestructor() {
    StoreManager::close_storage(store);
    rgw_http_client_cleanup();
  }
};

static int init_bucket(rgw::sal::User* user, const rgw_bucket& b,
                       std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  return store->get_bucket(dpp(), nullptr, b, bucket, null_yield);
}

static int init_bucket(rgw::sal::User* user,
		       const string& tenant_name,
		       const string& bucket_name,
		       const string& bucket_id,
                       std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  rgw_bucket b{tenant_name, bucket_name, bucket_id};
  return init_bucket(user, b, bucket);
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
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
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
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }
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

bool set_ratelimit_info(RGWRateLimitInfo& ratelimit, OPT opt_cmd, int64_t max_read_ops, int64_t max_write_ops,
                    int64_t max_read_bytes, int64_t max_write_bytes,
                    bool have_max_read_ops, bool have_max_write_ops,
                    bool have_max_read_bytes, bool have_max_write_bytes)
{
  bool ratelimit_configured = true;
  switch (opt_cmd) {
    case OPT::RATELIMIT_ENABLE:
    case OPT::GLOBAL_RATELIMIT_ENABLE:
      ratelimit.enabled = true;
      break;

    case OPT::RATELIMIT_SET:
    case OPT::GLOBAL_RATELIMIT_SET:
      ratelimit_configured = false;
      if (have_max_read_ops) {
        if (max_read_ops >= 0) {
          ratelimit.max_read_ops = max_read_ops;
          ratelimit_configured = true;
        }
      }
      if (have_max_write_ops) {
        if (max_write_ops >= 0) {
          ratelimit.max_write_ops = max_write_ops;
          ratelimit_configured = true;
        }
      }
      if (have_max_read_bytes) {
        if (max_read_bytes >= 0) {
          ratelimit.max_read_bytes = max_read_bytes;
          ratelimit_configured = true;
        }
      }
      if (have_max_write_bytes) {
        if (max_write_bytes >= 0) {
          ratelimit.max_write_bytes = max_write_bytes;
          ratelimit_configured = true;
        }
      }
      break;
    case OPT::RATELIMIT_DISABLE:
    case OPT::GLOBAL_RATELIMIT_DISABLE:
      ratelimit.enabled = false;
      break;
    default:
      break;
  }
  return ratelimit_configured;
}

void set_quota_info(RGWQuotaInfo& quota, OPT opt_cmd, int64_t max_size, int64_t max_objects,
                    bool have_max_size, bool have_max_objects)
{
  switch (opt_cmd) {
    case OPT::QUOTA_ENABLE:
    case OPT::GLOBAL_QUOTA_ENABLE:
      quota.enabled = true;

      // falling through on purpose

    case OPT::QUOTA_SET:
    case OPT::GLOBAL_QUOTA_SET:
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
    case OPT::QUOTA_DISABLE:
    case OPT::GLOBAL_QUOTA_DISABLE:
      quota.enabled = false;
      break;
    default:
      break;
  }
}

int set_bucket_quota(rgw::sal::Store* store, OPT opt_cmd,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_size, int64_t max_objects,
                     bool have_max_size, bool have_max_objects)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = store->get_bucket(dpp(), nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket->get_info().quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  r = bucket->put_info(dpp(), false, real_time());
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_bucket_ratelimit(rgw::sal::Store* store, OPT opt_cmd,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_read_ops, int64_t max_write_ops,
                     int64_t max_read_bytes, int64_t max_write_bytes,
                     bool have_max_read_ops, bool have_max_write_ops,
                     bool have_max_read_bytes, bool have_max_write_bytes)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = store->get_bucket(dpp(), nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  RGWRateLimitInfo ratelimit_info;
  auto iter = bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != bucket->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp(), 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  bool ratelimit_configured = set_ratelimit_info(ratelimit_info, opt_cmd, max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
  if (!ratelimit_configured) {
    ldpp_dout(dpp(), 0) << "ERROR: no rate limit values have been specified" << dendl;
    return -EINVAL;
  }
  bufferlist bl;
  ratelimit_info.encode(bl);
  rgw::sal::Attrs attr;
  attr[RGW_ATTR_RATELIMIT] = bl;
  r = bucket->merge_and_store_attrs(dpp(), attr, null_yield);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_user_ratelimit(OPT opt_cmd, std::unique_ptr<rgw::sal::User>& user,
                     int64_t max_read_ops, int64_t max_write_ops,
                     int64_t max_read_bytes, int64_t max_write_bytes,
                     bool have_max_read_ops, bool have_max_write_ops,
                     bool have_max_read_bytes, bool have_max_write_bytes)
{
  RGWRateLimitInfo ratelimit_info;
  user->load_user(dpp(), null_yield);
  auto iter = user->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != user->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp(), 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  bool ratelimit_configured = set_ratelimit_info(ratelimit_info, opt_cmd, max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
  if (!ratelimit_configured) {
    ldpp_dout(dpp(), 0) << "ERROR: no rate limit values have been specified" << dendl;
    return -EINVAL;
  }
  bufferlist bl;
  ratelimit_info.encode(bl);
  rgw::sal::Attrs attr;
  attr[RGW_ATTR_RATELIMIT] = bl;
  int r = user->merge_and_store_attrs(dpp(), attr, null_yield);
  if (r < 0) {
    cerr << "ERROR: failed writing user instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int show_user_ratelimit(std::unique_ptr<rgw::sal::User>& user, Formatter *formatter)
{
  RGWRateLimitInfo ratelimit_info;
  user->load_user(dpp(), null_yield);
  auto iter = user->get_attrs().find(RGW_ATTR_RATELIMIT);
  if(iter != user->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp(), 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  formatter->open_object_section("user_ratelimit");
  encode_json("user_ratelimit", ratelimit_info, formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}

int show_bucket_ratelimit(rgw::sal::Store* store, const string& tenant_name,
                          const string& bucket_name, Formatter *formatter)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = store->get_bucket(dpp(), nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  RGWRateLimitInfo ratelimit_info;
  auto iter = bucket->get_attrs().find(RGW_ATTR_RATELIMIT);
  if (iter != bucket->get_attrs().end()) {
    try {
      bufferlist& bl = iter->second;
      auto biter = bl.cbegin();
      decode(ratelimit_info, biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp(), 0) << "ERROR: failed to decode rate limit" << dendl;
      return -EIO;
    }
  }
  formatter->open_object_section("bucket_ratelimit");
  encode_json("bucket_ratelimit", ratelimit_info, formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  return 0;
}
int set_user_bucket_quota(OPT opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                          bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.bucket_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_bucket_quota(user_info.bucket_quota);

  string err;
  int r = user.modify(dpp(), op_state, null_yield, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int set_user_quota(OPT opt_cmd, RGWUser& user, RGWUserAdminOpState& op_state, int64_t max_size, int64_t max_objects,
                   bool have_max_size, bool have_max_objects)
{
  RGWUserInfo& user_info = op_state.get_user_info();

  set_quota_info(user_info.user_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_user_quota(user_info.user_quota);

  string err;
  int r = user.modify(dpp(), op_state, null_yield, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int check_min_obj_stripe_size(rgw::sal::Store* store, rgw::sal::Object* obj, uint64_t min_stripe_size, bool *need_rewrite)
{
  RGWObjectCtx obj_ctx(store);
  int ret = obj->get_obj_attrs(&obj_ctx, null_yield, dpp());
  if (ret < 0) {
    ldpp_dout(dpp(), -1) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<string, bufferlist>::iterator iter;
  iter = obj->get_attrs().find(RGW_ATTR_MANIFEST);
  if (iter == obj->get_attrs().end()) {
    *need_rewrite = (obj->get_obj_size() >= min_stripe_size);
    return 0;
  }

  RGWObjManifest manifest;

  try {
    bufferlist& bl = iter->second;
    auto biter = bl.cbegin();
    decode(manifest, biter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp(), 0) << "ERROR: failed to decode manifest" << dendl;
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


int check_obj_locator_underscore(rgw::sal::Object* obj, bool fix, bool remove_bad, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "head");
  f->dump_string("name", obj->get_name());
  f->dump_string("instance", obj->get_instance());
  f->close_section();

  string oid;
  string locator;

  get_obj_bucket_and_oid_loc(obj->get_obj(), oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);

  RGWObjectCtx obj_ctx(store);
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj->get_read_op(&obj_ctx);

  int ret = read_op->prepare(null_yield, dpp());
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->fix_head_obj_locator(dpp(), obj->get_bucket()->get_info(), needs_fixing, remove_bad, obj->get_key());
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

int check_obj_tail_locator_underscore(RGWBucketInfo& bucket_info, rgw_obj_key& key, bool fix, Formatter *f) {
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "tail");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  bool needs_fixing;
  string status;

  int ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->fix_tail_obj_locator(dpp(), bucket_info, key, fix, &needs_fixing, null_yield);
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

  std::unique_ptr<rgw::sal::Bucket> bucket;
  string bucket_id;

  f->open_object_section("bucket");
  f->dump_string("bucket", bucket_name);
  int ret = init_bucket(nullptr, tenant_name, bucket_name, bucket_id, &bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  int count = 0;

  int max_entries = 1000;

  string prefix;
  string delim;
  string marker;
  vector<rgw_bucket_dir_entry> result;
  string ns;

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = prefix;
  params.delim = delim;
  params.marker = rgw_obj_key(marker);
  params.ns = ns;
  params.enforce_ns = true;
  params.list_versions = true;

  f->open_array_section("check_objects");
  do {
    ret = bucket->list(dpp(), params, max_entries - count, results, null_yield);
    if (ret < 0) {
      cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += results.objs.size();

    for (vector<rgw_bucket_dir_entry>::iterator iter = results.objs.begin(); iter != results.objs.end(); ++iter) {
      std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(iter->key);

      if (obj->get_name()[0] == '_') {
        ret = check_obj_locator_underscore(obj.get(), fix, remove_bad, f);

	if (ret >= 0) {
          ret = check_obj_tail_locator_underscore(bucket->get_info(), obj->get_key(), fix, f);
          if (ret < 0) {
              cerr << "ERROR: check_obj_tail_locator_underscore(): " << cpp_strerror(-ret) << std::endl;
              return -ret;
          }
	}
      }
    }
    f->flush(cout);
  } while (results.is_truncated && count < max_entries);
  f->close_section();
  f->close_section();

  f->flush(cout);

  return 0;
}

/// search for a matching zone/zonegroup id and return a connection if found
static boost::optional<RGWRESTConn> get_remote_conn(rgw::sal::RadosStore* store,
                                                    const RGWZoneGroup& zonegroup,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  if (remote == zonegroup.get_id()) {
    conn.emplace(store->ctx(), store->svc()->zone, remote, zonegroup.endpoints, zonegroup.api_name);
  } else {
    for (const auto& z : zonegroup.zones) {
      const auto& zone = z.second;
      if (remote == zone.id) {
        conn.emplace(store->ctx(), store->svc()->zone, remote, zone.endpoints, zonegroup.api_name);
        break;
      }
    }
  }
  return conn;
}

/// search each zonegroup for a connection
static boost::optional<RGWRESTConn> get_remote_conn(rgw::sal::RadosStore* store,
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
  int ret = conn->forward(dpp(), user, info, nullptr, MAX_REST_RESPONSE, &in_data, &response, null_yield);

  int parse_ret = parser.parse(response.c_str(), response.length());
  if (parse_ret < 0) {
    cerr << "failed to parse response" << std::endl;
    return parse_ret;
  }
  return ret;
}

static int send_to_url(const string& url,
                       std::optional<string> opt_region,
                       const string& access,
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
  RGWRESTSimpleRequest req(g_ceph_context, info.method, url, NULL, &params, opt_region);

  bufferlist response;
  int ret = req.forward_request(dpp(), key, info, MAX_REST_RESPONSE, &in_data, &response, null_yield);

  int parse_ret = parser.parse(response.c_str(), response.length());
  if (parse_ret < 0) {
    cout << "failed to parse response" << std::endl;
    return parse_ret;
  }
  return ret;
}

static int send_to_remote_or_url(RGWRESTConn *conn, const string& url,
                                 std::optional<string> opt_region,
                                 const string& access, const string& secret,
                                 req_info& info, bufferlist& in_data,
                                 JSONParser& parser)
{
  if (url.empty()) {
    return send_to_remote_gateway(conn, info, in_data, parser);
  }
  return send_to_url(url, opt_region, access, secret, info, in_data, parser);
}

static int commit_period(RGWRealm& realm, RGWPeriod& period,
                         string remote, const string& url,
                         std::optional<string> opt_region,
                         const string& access, const string& secret,
                         bool force)
{
  auto& master_zone = period.get_master_zone();
  if (master_zone.empty()) {
    cerr << "cannot commit period: period does not have a master zone of a master zonegroup" << std::endl;
    return -EINVAL;
  }
  // are we the period's master zone?
  if (store->get_zone()->get_id() == master_zone) {
    // read the current period
    RGWPeriod current_period;
    int ret = current_period.init(dpp(), g_ceph_context,
				  static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm.get_id(),
				  null_yield);
    if (ret < 0) {
      cerr << "Error initializing current period: "
          << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    // the master zone can commit locally
    ret = period.commit(dpp(), store, realm, current_period, cerr, null_yield, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
    }
    return ret;
  }

  if (remote.empty() && url.empty()) {
    // use the new master zone's connection
    remote = master_zone.id;
    cerr << "Sending period to new master zone " << remote << std::endl;
  }
  boost::optional<RGWRESTConn> conn;
  RGWRESTConn *remote_conn = nullptr;
  if (!remote.empty()) {
    conn = get_remote_conn(static_cast<rgw::sal::RadosStore*>(store), period.get_map(), remote);
    if (!conn) {
      cerr << "failed to find a zone or zonegroup for remote "
          << remote << std::endl;
      return -ENOENT;
    }
    remote_conn = &*conn;
  }

  // push period to the master with an empty period id
  period.set_id(string());

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
  int ret = send_to_remote_or_url(remote_conn, url, opt_region, access, secret, info, bl, p);
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
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }
  if (period.get_id().empty()) {
    cerr << "Period commit got back an empty period id" << std::endl;
    return -EINVAL;
  }
  // the master zone gave us back the period that it committed, so it's
  // safe to save it as our latest epoch
  ret = period.store_info(dpp(), false, null_yield);
  if (ret < 0) {
    cerr << "Error storing committed period " << period.get_id() << ": "
        << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = period.set_latest_epoch(dpp(), null_yield, period.get_epoch());
  if (ret < 0) {
    cerr << "Error updating period epoch: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = period.reflect(dpp(), null_yield);
  if (ret < 0) {
    cerr << "Error updating local objects: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  realm.notify_new_period(dpp(), period, null_yield);
  return ret;
}

static int update_period(const string& realm_id, const string& realm_name,
                         const string& period_id, const string& period_epoch,
                         bool commit, const string& remote, const string& url,
                         std::optional<string> opt_region,
                         const string& access, const string& secret,
                         Formatter *formatter, bool force)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
  if (ret < 0 ) {
    cerr << "Error initializing realm " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  epoch_t epoch = 0;
  if (!period_epoch.empty()) {
    epoch = atoi(period_epoch.c_str());
  }
  RGWPeriod period(period_id, epoch);
  ret = period.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm.get_id(), null_yield);
  if (ret < 0) {
    cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  period.fork();
  ret = period.update(dpp(), null_yield);
  if(ret < 0) {
    // Dropping the error message here, as both the ret codes were handled in
    // period.update()
    return ret;
  }
  ret = period.store_info(dpp(), false, null_yield);
  if (ret < 0) {
    cerr << "failed to store period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  if (commit) {
    ret = commit_period(realm, period, remote, url, opt_region, access, secret, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  encode_json("period", period, formatter);
  formatter->flush(cout);
  return 0;
}

static int init_bucket_for_sync(rgw::sal::User* user,
				const string& tenant, const string& bucket_name,
                                const string& bucket_id,
				std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  int ret = init_bucket(user, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  return 0;
}

static int do_period_pull(RGWRESTConn *remote_conn, const string& url,
                          std::optional<string> opt_region,
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
  int ret = send_to_remote_or_url(remote_conn, url, opt_region, access_key, secret_key,
                                  info, bl, p);
  if (ret < 0) {
    cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  ret = period->init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield, false);
  if (ret < 0) {
    cerr << "faile to init period " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  try {
    decode_json_obj(*period, &p);
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }
  ret = period->store_info(dpp(), false, null_yield);
  if (ret < 0) {
    cerr << "Error storing period " << period->get_id() << ": " << cpp_strerror(ret) << std::endl;
  }
  // store latest epoch (ignore errors)
  period->update_latest_epoch(dpp(), period->get_epoch(), null_yield);
  return 0;
}

static int read_current_period_id(rgw::sal::RadosStore* store, const std::string& realm_id,
                                  const std::string& realm_name,
                                  std::string* period_id)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(dpp(), g_ceph_context, store->svc()->sysobj, null_yield);
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
  RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

  int ret = sync.init(dpp());
  if (ret < 0) {
    status.push_back(string("failed to retrieve sync info: sync.init() failed: ") + cpp_strerror(-ret));
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(dpp(), &sync_status);
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
  string master_period = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_current_period_id();

  ret = sync.read_master_log_shards_info(dpp(), master_period, &master_shards_info);
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

  // fetch remote log entries to determine the oldest change
  std::optional<std::pair<int, ceph::real_time>> oldest;
  if (!shards_behind.empty()) {
    map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(dpp(), sync_status.sync_info.period, shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch master next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;

        if (shard_data.entries.empty()) {
          // there aren't any entries in this shard, so we're not really behind
          shards_behind.erase(iter.first);
          shards_behind_set.erase(iter.first);
        } else {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (!oldest) {
            oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest->second) {
            oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status) << "metadata is caught up with master";
  } else {
    push_ss(ss, status) << "metadata is behind on " << total_behind << " shards";
    push_ss(ss, status) << "behind shards: " << "[" << shards_behind_set << "]";
    if (oldest) {
      push_ss(ss, status) << "oldest incremental change not applied: "
          << oldest->second << " [" << oldest->first << ']';
    }
  }

  flush_ss(ss, status);
}

static void get_data_sync_status(const rgw_zone_id& source_zone, list<string>& status, int tab)
{
  stringstream ss;

  RGWZone *sz;

  if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone(source_zone, &sz)) {
    push_ss(ss, status, tab) << string("zone not found");
    flush_ss(ss, status);
    return;
  }

  if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->zone_syncs_from(static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone(), *sz)) {
    push_ss(ss, status, tab) << string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor(), source_zone, nullptr);

  int ret = sync.init(dpp());
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(dpp(), &sync_status);
  if (ret < 0 && ret != -ENOENT) {
    push_ss(ss, status, tab) << string("failed read sync status: ") + cpp_strerror(-ret);
    return;
  }

  set<int> recovering_shards;
  ret = sync.read_recovering_shards(dpp(), sync_status.sync_info.num_shards, recovering_shards);
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

  ret = sync.read_source_log_shards_info(dpp(), &source_shards_info);
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
    ret = sync.read_source_log_shards_next(dpp(), shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      std::optional<std::pair<int, ceph::real_time>> oldest;

      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (!oldest) {
            oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest->second) {
            oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }

      if (oldest) {
        push_ss(ss, status, tab) << "oldest incremental change not applied: "
            << oldest->second << " [" << oldest->first << ']';
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
  const RGWRealm& realm = store->get_zone()->get_realm();
  const RGWZoneGroup& zonegroup = store->get_zone()->get_zonegroup();
  const RGWZone& zone = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << realm.get_id() << " (" << realm.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone.id << " (" << zone.name << ")" << std::endl;

  list<string> md_status;

  if (store->is_meta_master()) {
    md_status.push_back("no sync (zone is master)");
  } else {
    get_md_sync_status(md_status);
  }

  tab_dump("metadata sync", width, md_status);

  list<string> data_status;

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone_conn_map();

  for (auto iter : zone_conn_map) {
    const rgw_zone_id& source_id = iter.first;
    string source_str = "source: ";
    string s = source_str + source_id.id;
    RGWZone *sz;
    if (static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone(source_id, &sz)) {
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

static int bucket_source_sync_status(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* store, const RGWZone& zone,
                                     const RGWZone& source, RGWRESTConn *conn,
                                     const RGWBucketInfo& bucket_info,
                                     rgw_sync_bucket_pipe pipe,
                                     int width, std::ostream& out)
{
  out << indented{width, "source zone"} << source.id << " (" << source.name << ")" << std::endl;

  // syncing from this zone?
  if (!zone.syncs_from(source.name)) {
    out << indented{width} << "does not sync from zone\n";
    return 0;
  }

  if (!pipe.source.bucket) {
    ldpp_dout(dpp, -1) << __func__ << "(): missing source bucket" << dendl;
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> source_bucket;
  int r = init_bucket(nullptr, *pipe.source.bucket, &source_bucket);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to read source bucket info: " << cpp_strerror(r) << dendl;
    return r;
  }

  pipe.source.bucket = source_bucket->get_key();
  pipe.dest.bucket = bucket_info.bucket;

  std::vector<rgw_bucket_shard_sync_info> status;
  r = rgw_bucket_sync_status(dpp, store, pipe, bucket_info, &source_bucket->get_info(), &status);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to read bucket sync status: " << cpp_strerror(r) << dendl;
    return r;
  }

  out << indented{width, "source bucket"} << source_bucket << std::endl;

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
  r = rgw_read_remote_bilog_info(dpp, conn, source_bucket->get_key(), remote_markers, null_yield);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to read remote log: " << cpp_strerror(r) << dendl;
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
  if (!shards_behind.empty()) {
    out << indented{width} << "bucket is behind on " << shards_behind.size() << " shards\n";
    out << indented{width} << "behind shards: [" << shards_behind << "]\n" ;
  } else if (!num_full) {
    out << indented{width} << "bucket is caught up with source\n";
  }
  return 0;
}

void encode_json(const char *name, const RGWBucketSyncFlowManager::pipe_set& pset, Formatter *f)
{
  Formatter::ObjectSection top_section(*f, name);
  Formatter::ArraySection as(*f, "entries");

  for (auto& pipe_handler : pset) {
    Formatter::ObjectSection hs(*f, "handler");
    encode_json("source", pipe_handler.source, f);
    encode_json("dest", pipe_handler.dest, f);
  }
}

static std::vector<string> convert_bucket_set_to_str_vec(const std::set<rgw_bucket>& bs)
{
  std::vector<string> result;
  result.reserve(bs.size());
  for (auto& b : bs) {
    result.push_back(b.get_key());
  }
  return result;
}

static void get_hint_entities(const std::set<rgw_zone_id>& zones, const std::set<rgw_bucket>& buckets,
			      std::set<rgw_sync_bucket_entity> *hint_entities)
{
  for (auto& zone_id : zones) {
    for (auto& b : buckets) {
      std::unique_ptr<rgw::sal::Bucket> hint_bucket;
      int ret = init_bucket(nullptr, b, &hint_bucket);
      if (ret < 0) {
	ldpp_dout(dpp(), 20) << "could not init bucket info for hint bucket=" << b << " ... skipping" << dendl;
	continue;
      }

      hint_entities->insert(rgw_sync_bucket_entity(zone_id, hint_bucket->get_key()));
    }
  }
}

static rgw_zone_id resolve_zone_id(const string& s)
{
  rgw_zone_id result;

  RGWZone *zone;
  if (static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone(s, &zone)) {
    return rgw_zone_id(s);
  }
  if (static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone_id_by_name(s, &result)) {
    return result;
  }
  return rgw_zone_id(s);
}

rgw_zone_id validate_zone_id(const rgw_zone_id& zone_id)
{
  return resolve_zone_id(zone_id.id);
}

static int sync_info(std::optional<rgw_zone_id> opt_target_zone, std::optional<rgw_bucket> opt_bucket, Formatter *formatter)
{
  rgw_zone_id zone_id = opt_target_zone.value_or(store->get_zone()->get_id());

  auto zone_policy_handler = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_sync_policy_handler(zone_id);

  RGWBucketSyncPolicyHandlerRef bucket_handler;

  std::optional<rgw_bucket> eff_bucket = opt_bucket;

  auto handler = zone_policy_handler;

  if (eff_bucket) {
    std::unique_ptr<rgw::sal::Bucket> bucket;

    int ret = init_bucket(nullptr, *eff_bucket, &bucket);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: init_bucket failed: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (ret >= 0) {
      rgw::sal::Attrs attrs = bucket->get_attrs();
      bucket_handler.reset(handler->alloc_child(bucket->get_info(), std::move(attrs)));
    } else {
      cerr << "WARNING: bucket not found, simulating result" << std::endl;
      bucket_handler.reset(handler->alloc_child(*eff_bucket, nullopt));
    }

    ret = bucket_handler->init(dpp(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to init bucket sync policy handler: " << cpp_strerror(-ret) << " (ret=" << ret << ")" << std::endl;
      return ret;
    }

    handler = bucket_handler;
  }

  std::set<rgw_sync_bucket_pipe> sources;
  std::set<rgw_sync_bucket_pipe> dests;

  handler->get_pipes(&sources, &dests, std::nullopt);

  auto source_hints_vec = convert_bucket_set_to_str_vec(handler->get_source_hints());
  auto target_hints_vec = convert_bucket_set_to_str_vec(handler->get_target_hints());

  std::set<rgw_sync_bucket_pipe> resolved_sources;
  std::set<rgw_sync_bucket_pipe> resolved_dests;

  rgw_sync_bucket_entity self_entity(zone_id, opt_bucket);

  set<rgw_zone_id> source_zones;
  set<rgw_zone_id> target_zones;

  zone_policy_handler->reflect(dpp(), nullptr, nullptr,
                               nullptr, nullptr,
                               &source_zones,
                               &target_zones,
                               false); /* relaxed: also get all zones that we allow to sync to/from */

  std::set<rgw_sync_bucket_entity> hint_entities;

  get_hint_entities(source_zones, handler->get_source_hints(), &hint_entities);
  get_hint_entities(target_zones, handler->get_target_hints(), &hint_entities);

  for (auto& hint_entity : hint_entities) {
    if (!hint_entity.zone ||
	!hint_entity.bucket) {
      continue; /* shouldn't really happen */
    }

    auto zid = validate_zone_id(*hint_entity.zone);
    auto& hint_bucket = *hint_entity.bucket;

    RGWBucketSyncPolicyHandlerRef hint_bucket_handler;
    int r = store->get_sync_policy_handler(dpp(), zid, hint_bucket, &hint_bucket_handler, null_yield);
    if (r < 0) {
      ldpp_dout(dpp(), 20) << "could not get bucket sync policy handler for hint bucket=" << hint_bucket << " ... skipping" << dendl;
      continue;
    }

    hint_bucket_handler->get_pipes(&resolved_dests,
                                   &resolved_sources,
                                   self_entity); /* flipping resolved dests and sources as these are
                                                    relative to the remote entity */
  }

  {
    Formatter::ObjectSection os(*formatter, "result");
    encode_json("sources", sources, formatter);
    encode_json("dests", dests, formatter);
    {
      Formatter::ObjectSection hints_section(*formatter, "hints");
      encode_json("sources", source_hints_vec, formatter);
      encode_json("dests", target_hints_vec, formatter);
    }
    {
      Formatter::ObjectSection resolved_hints_section(*formatter, "resolved-hints-1");
      encode_json("sources", resolved_sources, formatter);
      encode_json("dests", resolved_dests, formatter);
    }
    {
      Formatter::ObjectSection resolved_hints_section(*formatter, "resolved-hints");
      encode_json("sources", handler->get_resolved_source_hints(), formatter);
      encode_json("dests", handler->get_resolved_dest_hints(), formatter);
    }
  }

  formatter->flush(cout);

  return 0;
}

static int bucket_sync_info(rgw::sal::RadosStore* store, const RGWBucketInfo& info,
                              std::ostream& out)
{
  const RGWRealm& realm = store->get_zone()->get_realm();
  const RGWZoneGroup& zonegroup = store->get_zone()->get_zonegroup();
  const RGWZone& zone = store->svc()->zone->get_zone();
  constexpr int width = 15;

  out << indented{width, "realm"} << realm.get_id() << " (" << realm.get_name() << ")\n";
  out << indented{width, "zonegroup"} << zonegroup.get_id() << " (" << zonegroup.get_name() << ")\n";
  out << indented{width, "zone"} << zone.id << " (" << zone.name << ")\n";
  out << indented{width, "bucket"} << info.bucket << "\n\n";

  if (!static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp())) {
    out << "Sync is disabled for bucket " << info.bucket.name << '\n';
    return 0;
  }

  RGWBucketSyncPolicyHandlerRef handler;

  int r = store->get_sync_policy_handler(dpp(), std::nullopt, info.bucket, &handler, null_yield);
  if (r < 0) {
    ldpp_dout(dpp(), -1) << "ERROR: failed to get policy handler for bucket (" << info.bucket << "): r=" << r << ": " << cpp_strerror(-r) << dendl;
    return r;
  }

  auto& sources = handler->get_sources();

  for (auto& m : sources) {
    auto& zone = m.first;
    out << indented{width, "source zone"} << zone << std::endl;
    for (auto& pipe_handler : m.second) {
      out << indented{width, "bucket"} << *pipe_handler.source.bucket << std::endl;
    }
  }

  return 0;
}

static int bucket_sync_status(rgw::sal::RadosStore* store, const RGWBucketInfo& info,
                              const rgw_zone_id& source_zone_id,
			      std::optional<rgw_bucket>& opt_source_bucket,
                              std::ostream& out)
{
  const RGWRealm& realm = store->get_zone()->get_realm();
  const RGWZoneGroup& zonegroup = store->get_zone()->get_zonegroup();
  const RGWZone& zone = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone();
  constexpr int width = 15;

  out << indented{width, "realm"} << realm.get_id() << " (" << realm.get_name() << ")\n";
  out << indented{width, "zonegroup"} << zonegroup.get_id() << " (" << zonegroup.get_name() << ")\n";
  out << indented{width, "zone"} << zone.id << " (" << zone.name << ")\n";
  out << indented{width, "bucket"} << info.bucket << "\n\n";

  if (!static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp())) {
    out << "Sync is disabled for bucket " << info.bucket.name << " or bucket has no sync sources" << std::endl;
    return 0;
  }

  RGWBucketSyncPolicyHandlerRef handler;

  int r = store->get_sync_policy_handler(dpp(), std::nullopt, info.bucket, &handler, null_yield);
  if (r < 0) {
    ldpp_dout(dpp(), -1) << "ERROR: failed to get policy handler for bucket (" << info.bucket << "): r=" << r << ": " << cpp_strerror(-r) << dendl;
    return r;
  }

  auto sources = handler->get_all_sources();

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone_conn_map();
  set<rgw_zone_id> zone_ids;

  if (!source_zone_id.empty()) {
    auto z = zonegroup.zones.find(source_zone_id);
    if (z == zonegroup.zones.end()) {
      ldpp_dout(dpp(), -1) << "Source zone not found in zonegroup "
          << zonegroup.get_name() << dendl;
      return -EINVAL;
    }
    auto c = zone_conn_map.find(source_zone_id);
    if (c == zone_conn_map.end()) {
      ldpp_dout(dpp(), -1) << "No connection to zone " << z->second.name << dendl;
      return -EINVAL;
    }
    zone_ids.insert(source_zone_id);
  } else {
    for (const auto& entry : zonegroup.zones) {
      zone_ids.insert(entry.second.id);
    }
  }

  for (auto& zone_id : zone_ids) {
    auto z = zonegroup.zones.find(zone_id.id);
    if (z == zonegroup.zones.end()) { /* should't happen */
      continue;
    }
    auto c = zone_conn_map.find(zone_id.id);
    if (c == zone_conn_map.end()) { /* should't happen */
      continue;
    }

    for (auto& entry : sources) {
      auto& pipe = entry.second;
      if (opt_source_bucket &&
	  pipe.source.bucket != opt_source_bucket) {
	continue;
      }
      if (pipe.source.zone.value_or(rgw_zone_id()) == z->second.id) {
	bucket_source_sync_status(dpp(), store, zone, z->second,
				  c->second,
				  info, pipe,
				  width, out);
      }
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
  int ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_rados_handle()->ioctx_create(pool.to_str().c_str(), io_ctx);
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

int check_reshard_bucket_params(rgw::sal::RadosStore* store,
				const string& bucket_name,
				const string& tenant,
				const string& bucket_id,
				bool num_shards_specified,
				int num_shards,
				int yes_i_really_mean_it,
				std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return -EINVAL;
  }

  if (!num_shards_specified) {
    cerr << "ERROR: --num-shards not specified" << std::endl;
    return -EINVAL;
  }

  if (num_shards > (int)static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_max_bucket_shards()) {
    cerr << "ERROR: num_shards too high, max value: " << static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_max_bucket_shards() << std::endl;
    return -EINVAL;
  }

  if (num_shards < 0) {
    cerr << "ERROR: num_shards must be non-negative integer" << std::endl;
    return -EINVAL;
  }

  int ret = init_bucket(nullptr, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  if ((*bucket)->get_info().reshard_status != cls_rgw_reshard_status::NOT_RESHARDING) {
    // if in_progress or done then we have an old BucketInfo
    cerr << "ERROR: the bucket is currently undergoing resharding and "
      "cannot be added to the reshard list at this time" << std::endl;
    return -EBUSY;
  }

  int num_source_shards = ((*bucket)->get_info().layout.current_index.layout.normal.num_shards > 0 ? (*bucket)->get_info().layout.current_index.layout.normal.num_shards : 1);

  if (num_shards <= num_source_shards && !yes_i_really_mean_it) {
    cerr << "num shards is less or equal to current shards count" << std::endl
	 << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return -EINVAL;
  }
  return 0;
}

static int scan_totp(CephContext *cct, ceph::real_time& now, rados::cls::otp::otp_info_t& totp, vector<string>& pins,
                     time_t *pofs)
{
#define MAX_TOTP_SKEW_HOURS (24 * 7)
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
        ldpp_dout(dpp(), 20) << "found at time=" << start_time - time_ofs << " time_ofs=" << time_ofs << dendl;
        return 0;
      }
    }
    sign = -sign;
    time_ofs_abs = (++count) * step_size;
    time_ofs = sign * time_ofs_abs;
  }

  return -ENOENT;
}

static int trim_sync_error_log(int shard_id, const string& marker, int delay_ms)
{
  auto oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX,
                                               shard_id);
  // call cls_log_trim() until it returns -ENODATA
  for (;;) {
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->timelog.trim(dpp(), oid, {}, {}, {}, marker, nullptr,
					      null_yield);
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

const string& get_tier_type(rgw::sal::RadosStore* store) {
  return store->svc()->zone->get_zone().tier_type;
}

static bool symmetrical_flow_opt(const string& opt)
{
  return (opt == "symmetrical" || opt == "symmetric");
}

static bool directional_flow_opt(const string& opt)
{
  return (opt == "directional" || opt == "direction");
}

template <class T>
static bool require_opt(std::optional<T> opt, bool extra_check = true)
{
  if (!opt || !extra_check) {
    return false;
  }
  return true;
}

template <class T>
static bool require_non_empty_opt(std::optional<T> opt, bool extra_check = true)
{
  if (!opt || opt->empty() || !extra_check) {
    return false;
  }
  return true;
}

template <class T>
static void show_result(T& obj,
                        Formatter *formatter,
                        ostream& os)
{
  encode_json("obj", obj, formatter);

  formatter->flush(cout);
}

void init_optional_bucket(std::optional<rgw_bucket>& opt_bucket,
                          std::optional<string>& opt_tenant,
                          std::optional<string>& opt_bucket_name,
                          std::optional<string>& opt_bucket_id)
{
  if (opt_tenant || opt_bucket_name || opt_bucket_id) {
    opt_bucket.emplace();
    if (opt_tenant) {
      opt_bucket->tenant = *opt_tenant;
    }
    if (opt_bucket_name) {
      opt_bucket->name = *opt_bucket_name;
    }
    if (opt_bucket_id) {
      opt_bucket->bucket_id = *opt_bucket_id;
    }
  }
}

class SyncPolicyContext
{
  RGWZoneGroup zonegroup;

  std::optional<rgw_bucket> b;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  rgw_sync_policy_info *policy{nullptr};

  std::optional<rgw_user> owner;

public:
  SyncPolicyContext(const string& zonegroup_id,
                    const string& zonegroup_name,
                    std::optional<rgw_bucket> _bucket) : zonegroup(zonegroup_id, zonegroup_name),
                                                         b(_bucket) {}

  int init() {
    int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
    if (ret < 0) {
      cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (!b) {
      policy = &zonegroup.sync_policy;
      return 0;
    }

    ret = init_bucket(nullptr, *b, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    owner = bucket->get_info().owner;

    if (!bucket->get_info().sync_policy) {
      rgw_sync_policy_info new_policy;
      bucket->get_info().set_sync_policy(std::move(new_policy));
    }

    policy = &(*bucket->get_info().sync_policy);

    return 0;
  }

  int write_policy() {
    if (!b) {
      int ret = zonegroup.update(dpp(), null_yield);
      if (ret < 0) {
        cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      return 0;
    }

    int ret = bucket->put_info(dpp(), false, real_time());
    if (ret < 0) {
      cerr << "failed to store bucket info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    return 0;
  }

  rgw_sync_policy_info& get_policy() {
    return *policy;
  }

  std::optional<rgw_user>& get_owner() {
    return owner;
  }
};

void resolve_zone_id_opt(std::optional<string>& zone_name, std::optional<rgw_zone_id>& zone_id)
{
  if (!zone_name || zone_id) {
    return;
  }
  zone_id.emplace();
  if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone_id_by_name(*zone_name, &(*zone_id))) {
    cerr << "WARNING: cannot find source zone id for name=" << *zone_name << std::endl;
    zone_id = rgw_zone_id(*zone_name);
  }
}
void resolve_zone_ids_opt(std::optional<vector<string> >& names, std::optional<vector<rgw_zone_id> >& ids)
{
  if (!names || ids) {
    return;
  }
  ids.emplace();
  for (auto& name : *names) {
    rgw_zone_id zid;
    if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone_id_by_name(name, &zid)) {
      cerr << "WARNING: cannot find source zone id for name=" << name << std::endl;
      zid = rgw_zone_id(name);
    }
    ids->push_back(zid);
  }
}

static vector<rgw_zone_id> zone_ids_from_str(const string& val)
{
  vector<rgw_zone_id> result;
  vector<string> v;
  get_str_vec(val, v);
  for (auto& z : v) {
    result.push_back(rgw_zone_id(z));
  }
  return result;
}

class JSONFormatter_PrettyZone : public JSONFormatter {
  class Handler : public JSONEncodeFilter::Handler<rgw_zone_id> {
    void encode_json(const char *name, const void *pval, ceph::Formatter *f) const override {
      auto zone_id = *(static_cast<const rgw_zone_id *>(pval));
      string zone_name;
      RGWZone *zone;
      if (static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone(zone_id, &zone)) {
        zone_name = zone->name;
      } else {
        cerr << "WARNING: cannot find zone name for id=" << zone_id << std::endl;
        zone_name = zone_id.id;
      }

      ::encode_json(name, zone_name, f);
    }
  } zone_id_type_handler;

  JSONEncodeFilter encode_filter;
public:
  JSONFormatter_PrettyZone(bool pretty_format) : JSONFormatter(pretty_format) {
    encode_filter.register_type(&zone_id_type_handler);
  }

  void *get_external_feature_handler(const std::string& feature) override {
    if (feature != "JSONEncodeFilter") {
      return nullptr;
    }
    return &encode_filter;
  }
};

static int search_entities_by_zone(rgw_zone_id zone_id,
                                   RGWRealm *prealm,
                                   RGWPeriod *pperiod,
                                   RGWZoneGroup *pzonegroup,
                                   bool *pfound)
{
  *pfound = false;

  auto& found = *pfound;

  list<string> realms;
  int r = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_realms(dpp(), realms);
  if (r < 0) {
    cerr << "failed to list realms: " << cpp_strerror(-r) << std::endl;
    return r;
  }

  for (auto& realm_name : realms) {
    string realm_id;
    string period_id;
    RGWRealm realm(realm_id, realm_name);
    r = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
    if (r < 0) {
      cerr << "WARNING: can't open realm " << realm_name << ": " << cpp_strerror(-r) << " ... skipping" << std::endl;
      continue;
    }
    RGWPeriod period;
    r = realm.find_zone(dpp(), zone_id, pperiod,
                        pzonegroup, &found, null_yield);

    if (found) {
      *prealm = realm;
      break;
    }
  }

  return 0;
}

static int try_to_resolve_local_zone(string& zone_id, string& zone_name)
{
  /* try to read zone info */
  RGWZoneParams zone(zone_id, zone_name);
  int r = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
  if (r == -ENOENT) {
    ldpp_dout(dpp(), 20) << __func__ << "(): local zone not found (id=" << zone_id << ", name= " << zone_name << ")" << dendl;
    return r;
  }

  if (r < 0) {
    ldpp_dout(dpp(), 0) << __func__ << "(): unable to read zone (id=" << zone_id << ", name= " << zone_name << "): " << cpp_strerror(-r) << dendl;

    return r;
  }

  zone_id = zone.get_id();
  zone_name = zone.get_name();

  return 0;
}

static void check_set_consistent(const string& resolved_param,
                                 string& param,
                                 const string& param_name)
{
  if (!param.empty() && param != resolved_param) {
    ldpp_dout(dpp(), 5) << "WARNING: " << param_name << " resolve mismatch. (param=" << param << ", resolved=" << resolved_param << ")" << dendl;
    return;
  }

  param = resolved_param;
  ldpp_dout(dpp(), 20) << __func__ << "(): resolved param: " << param_name << ": " << param << dendl;
}


static int try_to_resolve_local_entities(string& realm_id, string& realm_name,
                                         string& zonegroup_id, string& zonegroup_name,
                                         string& zone_id, string& zone_name)
{
  /*
   * Try to figure out realm, zonegroup, and zone entities, based on provided params and local zone.
   *
   * First read the local zone info (for zone id/name). Then search existing realm and period
   *  configuration and if found, update (but don't override) passed params.
   *
   */

  ldpp_dout(dpp(), 20) << __func__ << "(): before: realm_id=" << realm_id << " realm_name=" << realm_name << " zonegroup_id=" << zonegroup_id << " zonegroup_name=" << zonegroup_name << " zone_id=" << zone_id << " zone_name=" << zone_name << dendl;
  int r = try_to_resolve_local_zone(zone_id, zone_name);
  if (r == -ENOENT) {
    /* this local zone doesn't exist, abort */
    return 0;
  }
  if (r < 0) {
    return r;
  }

  if (zone_id.empty()) {
    /* not sure it's possible, but let's abort */
    return 0;
  }

  bool found;
  RGWRealm realm;
  RGWPeriod period;
  RGWZoneGroup zonegroup;
  r = search_entities_by_zone(zone_id, &realm, &period, &zonegroup, &found);
  if (r < 0) {
    ldpp_dout(dpp(), 0) << "ERROR: error when searching for realm id (r=" << r << "), ignoring" << dendl;
    return r;
  }

  if (!found) {
    return 0;
  }

  check_set_consistent(realm.get_id(), realm_id, "realm id (--realm-id)");
  check_set_consistent(realm.get_name(), realm_name, "realm name (--rgw-realm)");
  check_set_consistent(zonegroup.get_id(), zonegroup_id, "zonegroup id (--zonegroup-id)");
  check_set_consistent(zonegroup.get_name(), zonegroup_name, "zonegroup name (--rgw-zonegroup)");

  ldpp_dout(dpp(), 20) << __func__ << "(): after: realm_id=" << realm_id << " realm_name=" << realm_name << " zonegroup_id=" << zonegroup_id << " zonegroup_name=" << zonegroup_name << " zone_id=" << zone_id << " zone_name=" << zone_name << dendl;

  return 0;
}

static bool empty_opt(std::optional<string>& os)
{
  return (!os || os->empty());
}

static string safe_opt(std::optional<string>& os)
{
  return os.value_or(string());
}

void init_realm_param(CephContext *cct, string& var, std::optional<string>& opt_var, const string& conf_name)
{
  var = cct->_conf.get_val<string>(conf_name);
  if (!var.empty()) {
    opt_var = var;
  }
}

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
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

  rgw_user user_id_arg;
  std::unique_ptr<rgw::sal::User> user;
  string tenant;
  string user_ns;
  rgw_user new_user_id;
  std::string access_key, secret_key, user_email, display_name;
  std::string bucket_name, pool_name, object;
  rgw_pool pool;
  std::string date, subuser, access, format;
  std::string start_date, end_date;
  std::string key_type_str;
  std::string period_id, period_epoch, remote, url;
  std::optional<string> opt_region;
  std::string master_zone;
  std::string realm_name, realm_id, realm_new_name;
  std::optional<string> opt_realm_name, opt_realm_id;
  std::string zone_name, zone_id, zone_new_name;
  std::optional<string> opt_zone_name, opt_zone_id;
  std::string zonegroup_name, zonegroup_id, zonegroup_new_name;
  std::optional<string> opt_zonegroup_name, opt_zonegroup_id;
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
  std::unique_ptr<rgw::sal::Bucket> bucket;
  uint32_t perm_mask = 0;
  RGWUserInfo info;
  OPT opt_cmd = OPT::NO_CMD;
  int gen_access_key = 0;
  int gen_secret_key = 0;
  bool set_perm = false;
  bool set_temp_url_key = false;
  map<int, string> temp_url_keys;
  string bucket_id;
  string new_bucket_name;
  std::unique_ptr<Formatter> formatter;
  std::unique_ptr<Formatter> zone_formatter;
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
  string ratelimit_scope;
  string object_version;
  string placement_id;
  std::optional<string> opt_storage_class;
  list<string> tags;
  list<string> tags_add;
  list<string> tags_rm;

  int64_t max_objects = -1;
  int64_t max_size = -1;
  int64_t max_read_ops = 0;
  int64_t max_write_ops = 0;
  int64_t max_read_bytes = 0;
  int64_t max_write_bytes = 0;
  bool have_max_objects = false;
  bool have_max_size = false;
  bool have_max_write_ops = false;
  bool have_max_read_ops = false;
  bool have_max_write_bytes = false;
  bool have_max_read_bytes = false;
  int include_all = false;
  int allow_unordered = false;

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
  std::optional<log_type> opt_log_type;

  string job_id;
  int num_shards = 0;
  bool num_shards_specified = false;
  std::optional<int> bucket_index_max_shards;
  int max_concurrent_ios = 32;
  uint64_t orphan_stale_secs = (24 * 3600);
  int detail = false;

  std::string val;
  std::ostringstream errs;
  string err;

  string source_zone_name;
  rgw_zone_id source_zone; /* zone id */

  string tier_type;
  bool tier_type_specified = false;

  map<string, string, ltstr_nocase> tier_config_add;
  map<string, string, ltstr_nocase> tier_config_rm;

  boost::optional<string> index_pool;
  boost::optional<string> data_pool;
  boost::optional<string> data_extra_pool;
  rgw::BucketIndexType placement_index_type = rgw::BucketIndexType::Normal;
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
  string event_id;

  std::optional<std::string> str_script_ctx;
  std::optional<std::string> script_package;
  int allow_compilation = false;

  std::optional<string> opt_group_id;
  std::optional<string> opt_status;
  std::optional<string> opt_flow_type;
  std::optional<vector<string> > opt_zone_names;
  std::optional<vector<rgw_zone_id> > opt_zone_ids;
  std::optional<string> opt_flow_id;
  std::optional<string> opt_source_zone_name;
  std::optional<rgw_zone_id> opt_source_zone_id;
  std::optional<string> opt_dest_zone_name;
  std::optional<rgw_zone_id> opt_dest_zone_id;
  std::optional<vector<string> > opt_source_zone_names;
  std::optional<vector<rgw_zone_id> > opt_source_zone_ids;
  std::optional<vector<string> > opt_dest_zone_names;
  std::optional<vector<rgw_zone_id> > opt_dest_zone_ids;
  std::optional<string> opt_pipe_id;
  std::optional<rgw_bucket> opt_bucket;
  std::optional<string> opt_tenant;
  std::optional<string> opt_bucket_name;
  std::optional<string> opt_bucket_id;
  std::optional<rgw_bucket> opt_source_bucket;
  std::optional<string> opt_source_tenant;
  std::optional<string> opt_source_bucket_name;
  std::optional<string> opt_source_bucket_id;
  std::optional<rgw_bucket> opt_dest_bucket;
  std::optional<string> opt_dest_tenant;
  std::optional<string> opt_dest_bucket_name;
  std::optional<string> opt_dest_bucket_id;
  std::optional<string> opt_effective_zone_name;
  std::optional<rgw_zone_id> opt_effective_zone_id;

  std::optional<string> opt_prefix;
  std::optional<string> opt_prefix_rm;

  std::optional<int> opt_priority;
  std::optional<string> opt_mode;
  std::optional<rgw_user> opt_dest_owner;
  ceph::timespan opt_retry_delay_ms = std::chrono::milliseconds(2000);
  ceph::timespan opt_timeout_sec = std::chrono::seconds(60);

  SimpleCmd cmd(all_cmds, cmd_aliases);
  bool raw_storage_op = false;

  std::optional<std::string> rgw_obj_fs; // radoslist field separator

  init_realm_param(cct.get(), realm_id, opt_realm_id, "rgw_realm_id");
  init_realm_param(cct.get(), zonegroup_id, opt_zonegroup_id, "rgw_zonegroup_id");
  init_realm_param(cct.get(), zone_id, opt_zone_id, "rgw_zone_id");

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--uid", (char*)NULL)) {
      user_id_arg.from_str(val);
      if (user_id_arg.empty()) {
        cerr << "no value for uid" << std::endl;
        exit(1);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--new-uid", (char*)NULL)) {
      new_user_id.from_str(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--tenant", (char*)NULL)) {
      tenant = val;
      opt_tenant = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--user_ns", (char*)NULL)) {
      user_ns = val;
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
      opt_bucket_name = val;
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
      max_size = strict_iec_cast<long long>(val, &err);
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
    } else if (ceph_argparse_witharg(args, i, &val, "--max-read-ops", (char*)NULL)) {
      max_read_ops = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max read requests: " << err << std::endl;
        return EINVAL;
      }
      have_max_read_ops = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-write-ops", (char*)NULL)) {
      max_write_ops = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max write requests: " << err << std::endl;
        return EINVAL;
      }
      have_max_write_ops = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-read-bytes", (char*)NULL)) {
      max_read_bytes = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max read bytes: " << err << std::endl;
        return EINVAL;
      }
      have_max_read_bytes = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-write-bytes", (char*)NULL)) {
      max_write_bytes = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max write bytes: " << err << std::endl;
        return EINVAL;
      }
      have_max_write_bytes = true;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--bucket-index-max-shards", (char*)NULL)) {
      bucket_index_max_shards = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse bucket-index-max-shards: " << err << std::endl;
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
      opt_bucket_id = val;
      if (bucket_id.empty()) {
        cerr << "no value for bucket-id" << std::endl;
        exit(1);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--bucket-new-name", (char*)NULL)) {
      new_bucket_name = val;
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
    } else if (ceph_argparse_binary_flag(args, i, &allow_unordered, NULL, "--allow-unordered", (char*)NULL)) {
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
    } else if (ceph_argparse_witharg(args, i, &val, "--infile", (char*)NULL)) {
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
    } else if (ceph_argparse_witharg(args, i, &val, "--ratelimit-scope", (char*)NULL)) {
      ratelimit_scope = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--index-type", (char*)NULL)) {
      string index_type_str = val;
      bi_index_type = get_bi_index_type(index_type_str);
      if (bi_index_type == BIIndexType::Invalid) {
        cerr << "ERROR: invalid bucket index entry type" << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--log-type", (char*)NULL)) {
      string log_type_str = val;
      auto l = get_log_type(log_type_str);
      if (l == static_cast<log_type>(0xff)) {
        cerr << "ERROR: invalid log type" << std::endl;
        return EINVAL;
      }
      opt_log_type = l;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--region", (char*)NULL)) {
      opt_region = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--realm-id", (char*)NULL)) {
      realm_id = val;
      opt_realm_id = val;
      g_conf().set_val("rgw_realm_id", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--realm-new-name", (char*)NULL)) {
      realm_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zonegroup-id", (char*)NULL)) {
      zonegroup_id = val;
      opt_zonegroup_id = val;
      g_conf().set_val("rgw_zonegroup_id", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--zonegroup-new-name", (char*)NULL)) {
      zonegroup_new_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--placement-id", (char*)NULL)) {
      placement_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--storage-class", (char*)NULL)) {
      opt_storage_class = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--tags", (char*)NULL)) {
      get_str_list(val, ",", tags);
    } else if (ceph_argparse_witharg(args, i, &val, "--tags-add", (char*)NULL)) {
      get_str_list(val, ",", tags_add);
    } else if (ceph_argparse_witharg(args, i, &val, "--tags-rm", (char*)NULL)) {
      get_str_list(val, ",", tags_rm);
    } else if (ceph_argparse_witharg(args, i, &val, "--api-name", (char*)NULL)) {
      api_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zone-id", (char*)NULL)) {
      zone_id = val;
      opt_zone_id = val;
      g_conf().set_val("rgw_zone_id", val);
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
      opt_source_zone_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--source-zone-id", (char*)NULL)) {
      opt_source_zone_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-zone", (char*)NULL)) {
      opt_dest_zone_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-zone-id", (char*)NULL)) {
      opt_dest_zone_id = val;
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
        placement_index_type = rgw::BucketIndexType::Normal;
      } else if (val == "indexless") {
        placement_index_type = rgw::BucketIndexType::Indexless;
      } else {
        placement_index_type = (rgw::BucketIndexType)strict_strtol(val.c_str(), 10, &err);
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
    } else if (ceph_argparse_witharg(args, i, &val, "--subscription", (char*)NULL)) {
      sub_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--event-id", (char*)NULL)) {
      event_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--group-id", (char*)NULL)) {
      opt_group_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--status", (char*)NULL)) {
      opt_status = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--flow-type", (char*)NULL)) {
      opt_flow_type = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--zones", "--zone-names", (char*)NULL)) {
      vector<string> v;
      get_str_vec(val, v);
      opt_zone_names = std::move(v);
    } else if (ceph_argparse_witharg(args, i, &val, "--zone-ids", (char*)NULL)) {
      opt_zone_ids = zone_ids_from_str(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--source-zones", "--source-zone-names", (char*)NULL)) {
      vector<string> v;
      get_str_vec(val, v);
      opt_source_zone_names = std::move(v);
    } else if (ceph_argparse_witharg(args, i, &val, "--source-zone-ids", (char*)NULL)) {
      opt_source_zone_ids = zone_ids_from_str(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-zones", "--dest-zone-names", (char*)NULL)) {
      vector<string> v;
      get_str_vec(val, v);
      opt_dest_zone_names = std::move(v);
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-zone-ids", (char*)NULL)) {
      opt_dest_zone_ids = zone_ids_from_str(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--flow-id", (char*)NULL)) {
      opt_flow_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--pipe-id", (char*)NULL)) {
      opt_pipe_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--source-tenant", (char*)NULL)) {
      opt_source_tenant = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--source-bucket", (char*)NULL)) {
      opt_source_bucket_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--source-bucket-id", (char*)NULL)) {
      opt_source_bucket_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-tenant", (char*)NULL)) {
      opt_dest_tenant = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-bucket", (char*)NULL)) {
      opt_dest_bucket_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-bucket-id", (char*)NULL)) {
      opt_dest_bucket_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--effective-zone-name", "--effective-zone", (char*)NULL)) {
      opt_effective_zone_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--effective-zone-id", (char*)NULL)) {
      opt_effective_zone_id = rgw_zone_id(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--prefix", (char*)NULL)) {
      opt_prefix = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--prefix-rm", (char*)NULL)) {
      opt_prefix_rm = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--priority", (char*)NULL)) {
      opt_priority = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--mode", (char*)NULL)) {
      opt_mode = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--dest-owner", (char*)NULL)) {
      opt_dest_owner.emplace(val);
      opt_dest_owner = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--retry-delay-ms", (char*)NULL)) {
      opt_retry_delay_ms = std::chrono::milliseconds(atoi(val.c_str()));
    } else if (ceph_argparse_witharg(args, i, &val, "--timeout-sec", (char*)NULL)) {
      opt_timeout_sec = std::chrono::seconds(atoi(val.c_str()));
    } else if (ceph_argparse_binary_flag(args, i, &detail, NULL, "--detail", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--context", (char*)NULL)) {
      str_script_ctx = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--package", (char*)NULL)) {
      script_package = val;
    } else if (ceph_argparse_binary_flag(args, i, &allow_compilation, NULL, "--allow-compilation", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--rgw-obj-fs", (char*)NULL)) {
      rgw_obj_fs = val;
    } else if (strncmp(*i, "-", 1) == 0) {
      cerr << "ERROR: invalid flag " << *i << std::endl;
      return EINVAL;
    } else {
      ++i;
    }
  }

  /* common_init_finish needs to be called after g_conf().set_val() */
  common_init_finish(g_ceph_context);

  if (args.empty()) {
    usage();
    exit(1);
  }
  else {
    std::vector<string> extra_args;
    std::vector<string> expected;

    std::any _opt_cmd;

    if (!cmd.find_command(args, &_opt_cmd, &extra_args, &err, &expected)) {
      if (!expected.empty()) {
        cerr << err << std::endl;
        cerr << "Expected one of the following:" << std::endl;
        for (auto& exp : expected) {
          if (exp == "*" || exp == "[*]") {
            continue;
          }
          cerr << "  " << exp << std::endl;
        }
      } else {
        cerr << "Command not found:";
        for (auto& arg : args) {
          cerr << " " << arg;
        }
        cerr << std::endl;
      }
      exit(1);
    }

    opt_cmd = std::any_cast<OPT>(_opt_cmd);

    /* some commands may have an optional extra param */
    if (!extra_args.empty()) {
      switch (opt_cmd) {
        case OPT::METADATA_GET:
        case OPT::METADATA_PUT:
        case OPT::METADATA_RM:
        case OPT::METADATA_LIST:
          metadata_key = extra_args[0];
          break;
        default:
          break;
      }
    }

    // not a raw op if 'period update' needs to commit to master
    bool raw_period_update = opt_cmd == OPT::PERIOD_UPDATE && !commit;
    // not a raw op if 'period pull' needs to read zone/period configuration
    bool raw_period_pull = opt_cmd == OPT::PERIOD_PULL && !url.empty();

    std::set<OPT> raw_storage_ops_list = {OPT::ZONEGROUP_ADD, OPT::ZONEGROUP_CREATE,
			 OPT::ZONEGROUP_DELETE,
			 OPT::ZONEGROUP_GET, OPT::ZONEGROUP_LIST,
			 OPT::ZONEGROUP_SET, OPT::ZONEGROUP_DEFAULT,
			 OPT::ZONEGROUP_RENAME, OPT::ZONEGROUP_MODIFY,
			 OPT::ZONEGROUP_REMOVE,
			 OPT::ZONEGROUP_PLACEMENT_ADD, OPT::ZONEGROUP_PLACEMENT_RM,
			 OPT::ZONEGROUP_PLACEMENT_MODIFY, OPT::ZONEGROUP_PLACEMENT_LIST,
			 OPT::ZONEGROUP_PLACEMENT_GET,
			 OPT::ZONEGROUP_PLACEMENT_DEFAULT,
			 OPT::ZONE_CREATE, OPT::ZONE_DELETE,
			 OPT::ZONE_GET, OPT::ZONE_SET, OPT::ZONE_RENAME,
			 OPT::ZONE_LIST, OPT::ZONE_MODIFY, OPT::ZONE_DEFAULT,
			 OPT::ZONE_PLACEMENT_ADD, OPT::ZONE_PLACEMENT_RM,
			 OPT::ZONE_PLACEMENT_MODIFY, OPT::ZONE_PLACEMENT_LIST,
			 OPT::ZONE_PLACEMENT_GET,
			 OPT::REALM_CREATE,
			 OPT::PERIOD_DELETE, OPT::PERIOD_GET,
			 OPT::PERIOD_GET_CURRENT, OPT::PERIOD_LIST,
			 OPT::GLOBAL_QUOTA_GET, OPT::GLOBAL_QUOTA_SET,
			 OPT::GLOBAL_QUOTA_ENABLE, OPT::GLOBAL_QUOTA_DISABLE,
       OPT::GLOBAL_RATELIMIT_GET, OPT::GLOBAL_RATELIMIT_SET,
			 OPT::GLOBAL_RATELIMIT_ENABLE, OPT::GLOBAL_RATELIMIT_DISABLE,
			 OPT::REALM_DELETE, OPT::REALM_GET, OPT::REALM_LIST,
			 OPT::REALM_LIST_PERIODS,
			 OPT::REALM_GET_DEFAULT,
			 OPT::REALM_RENAME, OPT::REALM_SET,
			 OPT::REALM_DEFAULT, OPT::REALM_PULL};

    std::set<OPT> readonly_ops_list = {
                         OPT::USER_INFO,
			 OPT::USER_STATS,
			 OPT::BUCKETS_LIST,
			 OPT::BUCKET_LIMIT_CHECK,
			 OPT::BUCKET_STATS,
			 OPT::BUCKET_SYNC_CHECKPOINT,
			 OPT::BUCKET_SYNC_INFO,
			 OPT::BUCKET_SYNC_STATUS,
			 OPT::BUCKET_SYNC_MARKERS,
			 OPT::LOG_LIST,
			 OPT::LOG_SHOW,
			 OPT::USAGE_SHOW,
			 OPT::OBJECT_STAT,
			 OPT::BI_GET,
			 OPT::BI_LIST,
			 OPT::OLH_GET,
			 OPT::OLH_READLOG,
			 OPT::GC_LIST,
			 OPT::LC_LIST,
			 OPT::ORPHANS_LIST_JOBS,
			 OPT::ZONEGROUP_GET,
			 OPT::ZONEGROUP_LIST,
			 OPT::ZONEGROUP_PLACEMENT_LIST,
			 OPT::ZONEGROUP_PLACEMENT_GET,
			 OPT::ZONE_GET,
			 OPT::ZONE_LIST,
			 OPT::ZONE_PLACEMENT_LIST,
			 OPT::ZONE_PLACEMENT_GET,
			 OPT::METADATA_GET,
			 OPT::METADATA_LIST,
			 OPT::METADATA_SYNC_STATUS,
			 OPT::MDLOG_LIST,
			 OPT::MDLOG_STATUS,
			 OPT::SYNC_ERROR_LIST,
			 OPT::SYNC_GROUP_GET,
			 OPT::SYNC_POLICY_GET,
			 OPT::BILOG_LIST,
			 OPT::BILOG_STATUS,
			 OPT::DATA_SYNC_STATUS,
			 OPT::DATALOG_LIST,
			 OPT::DATALOG_STATUS,
			 OPT::REALM_GET,
			 OPT::REALM_GET_DEFAULT,
			 OPT::REALM_LIST,
			 OPT::REALM_LIST_PERIODS,
			 OPT::PERIOD_GET,
			 OPT::PERIOD_GET_CURRENT,
			 OPT::PERIOD_LIST,
			 OPT::GLOBAL_QUOTA_GET,
       OPT::GLOBAL_RATELIMIT_GET,
			 OPT::SYNC_INFO,
			 OPT::SYNC_STATUS,
			 OPT::ROLE_GET,
			 OPT::ROLE_LIST,
			 OPT::ROLE_POLICY_LIST,
			 OPT::ROLE_POLICY_GET,
			 OPT::RESHARD_LIST,
			 OPT::RESHARD_STATUS,
			 OPT::PUBSUB_TOPICS_LIST,
			 OPT::PUBSUB_TOPIC_GET,
			 OPT::PUBSUB_SUB_GET,
			 OPT::PUBSUB_SUB_PULL,
			 OPT::SCRIPT_GET,
    };

    std::set<OPT> gc_ops_list = {
			 OPT::GC_LIST,
			 OPT::GC_PROCESS,
			 OPT::OBJECT_RM,
			 OPT::BUCKET_RM,  // --purge-objects
			 OPT::USER_RM,    // --purge-data
			 OPT::OBJECTS_EXPIRE,
			 OPT::OBJECTS_EXPIRE_STALE_RM,
			 OPT::LC_PROCESS
    };

    raw_storage_op = (raw_storage_ops_list.find(opt_cmd) != raw_storage_ops_list.end() ||
			   raw_period_update || raw_period_pull);
    bool need_cache = readonly_ops_list.find(opt_cmd) == readonly_ops_list.end();
    bool need_gc = (gc_ops_list.find(opt_cmd) != gc_ops_list.end()) && !bypass_gc;

    std::string rgw_store = "rados";
    const auto& config_store = g_conf().get_val<std::string>("rgw_backend_store");
    #ifdef WITH_RADOSGW_DBSTORE
    if (config_store == "dbstore") {
      rgw_store = "dbstore";
    }
    #endif

    #ifdef WITH_RADOSGW_MOTR
    if (config_store == "motr") {
      rgw_store = "motr";
    }
    #endif
    lsubdout(cct, rgw, 1) << "RGW CONF BACKEND STORE = " << config_store << dendl;
    lsubdout(cct, rgw, 1) << "RGW BACKEND STORE = " << rgw_store << dendl;

    if (raw_storage_op) {
      store = StoreManager::get_raw_storage(dpp(), g_ceph_context, rgw_store);
    } else {
      store = StoreManager::get_storage(dpp(), g_ceph_context, rgw_store, false, false, false,
					   false, false,
					   need_cache && g_conf()->rgw_cache_enabled, need_gc);
    }
    if (!store) {
      cerr << "couldn't init storage provider" << std::endl;
      return 5; //EIO
    }

    /* Needs to be after the store is initialized.  Note, user could be empty here. */
    user = store->get_user(user_id_arg);

    init_optional_bucket(opt_bucket, opt_tenant,
                         opt_bucket_name, opt_bucket_id);
    init_optional_bucket(opt_source_bucket, opt_source_tenant,
                         opt_source_bucket_name, opt_source_bucket_id);
    init_optional_bucket(opt_dest_bucket, opt_dest_tenant,
                         opt_dest_bucket_name, opt_dest_bucket_id);

    if (tenant.empty()) {
      tenant = user->get_tenant();
    } else {
      if (rgw::sal::User::empty(user) && opt_cmd != OPT::ROLE_CREATE
                          && opt_cmd != OPT::ROLE_DELETE
                          && opt_cmd != OPT::ROLE_GET
                          && opt_cmd != OPT::ROLE_MODIFY
                          && opt_cmd != OPT::ROLE_LIST
                          && opt_cmd != OPT::ROLE_POLICY_PUT
                          && opt_cmd != OPT::ROLE_POLICY_LIST
                          && opt_cmd != OPT::ROLE_POLICY_GET
                          && opt_cmd != OPT::ROLE_POLICY_DELETE
                          && opt_cmd != OPT::RESHARD_ADD
                          && opt_cmd != OPT::RESHARD_CANCEL
                          && opt_cmd != OPT::RESHARD_STATUS) {
        cerr << "ERROR: --tenant is set, but there's no user ID" << std::endl;
        return EINVAL;
      }
      user->set_tenant(tenant);
    }
    if (user_ns.empty()) {
      user_ns = user->get_id().ns;
    } else {
      user->set_ns(user_ns);
    }

    if (!new_user_id.empty() && !tenant.empty()) {
      new_user_id.tenant = tenant;
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
    formatter = make_unique<XMLFormatter>(new XMLFormatter(pretty_format));
  else if (format == "json")
    formatter = make_unique<JSONFormatter>(new JSONFormatter(pretty_format));
  else {
    cerr << "unrecognized format: " << format << std::endl;
    exit(1);
  }

  zone_formatter = std::make_unique<JSONFormatter_PrettyZone>(pretty_format);

  realm_name = g_conf()->rgw_realm;
  zone_name = g_conf()->rgw_zone;
  zonegroup_name = g_conf()->rgw_zonegroup;

  if (!realm_name.empty()) {
    opt_realm_name = realm_name;
  }

  if (!zone_name.empty()) {
    opt_zone_name = zone_name;
  }

  if (!zonegroup_name.empty()) {
    opt_zonegroup_name = zonegroup_name;
  }

  RGWStreamFlusher stream_flusher(formatter.get(), cout);

  RGWUserAdminOpState user_op(store);
  if (!user_email.empty()) {
    user_op.user_email_specified=true;
  }

  if (!source_zone_name.empty()) {
    if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone_id_by_name(source_zone_name, &source_zone)) {
      cerr << "WARNING: cannot find source zone id for name=" << source_zone_name << std::endl;
      source_zone = source_zone_name;
    }
  }

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

  StoreDestructor store_destructor(static_cast<rgw::sal::RadosStore*>(store));

  if (raw_storage_op) {
    try_to_resolve_local_entities(realm_id, realm_name,
                                  zonegroup_id, zonegroup_name,
                                  zone_id, zone_name);


    switch (opt_cmd) {
    case OPT::PERIOD_DELETE:
      {
	if (period_id.empty()) {
	  cerr << "missing period id" << std::endl;
	  return EINVAL;
	}
	RGWPeriod period(period_id);
	int ret = period.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "period.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = period.delete_obj(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT::PERIOD_GET:
      {
	epoch_t epoch = 0;
	if (!period_epoch.empty()) {
	  epoch = atoi(period_epoch.c_str());
	}
        if (staging) {
          RGWRealm realm(realm_id, realm_name);
          int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
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
	int ret = period.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm_id,
			      null_yield, realm_name);
	if (ret < 0) {
	  cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("period", period, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_GET_CURRENT:
      {
        int ret = read_current_period_id(static_cast<rgw::sal::RadosStore*>(store), realm_id, realm_name, &period_id);
	if (ret < 0) {
	  return -ret;
	}
	formatter->open_object_section("period_get_current");
	encode_json("current_period", period_id, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_LIST:
      {
	list<string> periods;
	int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_periods(dpp(), periods);
	if (ret < 0) {
	  cerr << "failed to list periods: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("periods_list");
	encode_json("periods", periods, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_UPDATE:
      {
        int ret = update_period(realm_id, realm_name, period_id, period_epoch,
                                commit, remote, url, opt_region,
                                access_key, secret_key,
                                formatter.get(), yes_i_really_mean_it);
	if (ret < 0) {
	  return -ret;
	}
      }
      break;
    case OPT::PERIOD_PULL:
      {
        boost::optional<RGWRESTConn> conn;
        RGWRESTConn *remote_conn = nullptr;
        if (url.empty()) {
          // load current period for endpoints
          RGWRealm realm(realm_id, realm_name);
          int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
          if (ret < 0) {
            cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          RGWPeriod current_period(realm.get_current_period());
          ret = current_period.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
          if (ret < 0) {
            cerr << "failed to init current period: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (remote.empty()) {
            // use realm master zone as remote
            remote = current_period.get_master_zone().id;
          }
          conn = get_remote_conn(static_cast<rgw::sal::RadosStore*>(store), current_period.get_map(), remote);
          if (!conn) {
            cerr << "failed to find a zone or zonegroup for remote "
                << remote << std::endl;
            return -ENOENT;
          }
          remote_conn = &*conn;
        }

        RGWPeriod period;
        int ret = do_period_pull(remote_conn, url, opt_region,
                                 access_key, secret_key,
                                 realm_id, realm_name, period_id, period_epoch,
                                 &period);
        if (ret < 0) {
          cerr << "period pull failed: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("period", period, formatter.get());
        formatter->flush(cout);
      }
      break;
    case OPT::GLOBAL_RATELIMIT_GET:
    case OPT::GLOBAL_RATELIMIT_SET:
    case OPT::GLOBAL_RATELIMIT_ENABLE:
    case OPT::GLOBAL_RATELIMIT_DISABLE:
      {
        if (realm_id.empty()) {
          RGWRealm realm(g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj);
          if (!realm_name.empty()) {
            // look up realm_id for the given realm_name
            int ret = realm.read_id(dpp(), realm_name, realm_id, null_yield);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default realm_id when none is given
            int ret = realm.read_default_id(dpp(), realm_id, null_yield);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = period_config.read(dpp(), static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm_id, null_yield);
        if (ret < 0 && ret != -ENOENT) {
          cerr << "ERROR: failed to read period config: "
              << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        bool ratelimit_configured = true;
        formatter->open_object_section("period_config");
        if (ratelimit_scope == "bucket") {
          ratelimit_configured = set_ratelimit_info(period_config.bucket_ratelimit, opt_cmd,
                         max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
          encode_json("bucket_ratelimit", period_config.bucket_ratelimit, formatter.get());
        } else if (ratelimit_scope == "user") {
          ratelimit_configured = set_ratelimit_info(period_config.user_ratelimit, opt_cmd,
                         max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
          encode_json("user_ratelimit", period_config.user_ratelimit, formatter.get());
        } else if (ratelimit_scope == "anonymous") {
          ratelimit_configured = set_ratelimit_info(period_config.anon_ratelimit, opt_cmd,
                         max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
          encode_json("anonymous_ratelimit", period_config.anon_ratelimit, formatter.get());
        } else if (ratelimit_scope.empty() && opt_cmd == OPT::GLOBAL_RATELIMIT_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket_ratelimit", period_config.bucket_ratelimit, formatter.get());
          encode_json("user_ratelimit", period_config.user_ratelimit, formatter.get());
          encode_json("anonymous_ratelimit", period_config.anon_ratelimit, formatter.get());
        } else {
          cerr << "ERROR: invalid rate limit scope specification. Please specify "
              "either --ratelimit-scope=bucket, or --ratelimit-scope=user or --ratelimit-scope=anonymous" << std::endl;
          return EINVAL;
        }
        if (!ratelimit_configured) {
          cerr << "ERROR: no rate limit values have been specified" << std::endl;
          return EINVAL;
        }

        formatter->close_section();

        if (opt_cmd != OPT::GLOBAL_RATELIMIT_GET) {
          // write the modified period config
          ret = period_config.write(dpp(), static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm_id, null_yield);
          if (ret < 0) {
            cerr << "ERROR: failed to write period config: "
                << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (!realm_id.empty()) {
            cout << "Global ratelimit changes saved. Use 'period update' to apply "
                "them to the staging period, and 'period commit' to commit the "
                "new period." << std::endl;
          } else {
            cout << "Global ratelimit changes saved. They will take effect as "
                "the gateways are restarted." << std::endl;
          }
        }

        formatter->flush(cout);
      }
      break;
    case OPT::GLOBAL_QUOTA_GET:
    case OPT::GLOBAL_QUOTA_SET:
    case OPT::GLOBAL_QUOTA_ENABLE:
    case OPT::GLOBAL_QUOTA_DISABLE:
      {
        if (realm_id.empty()) {
          RGWRealm realm(g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj);
          if (!realm_name.empty()) {
            // look up realm_id for the given realm_name
            int ret = realm.read_id(dpp(), realm_name, realm_id, null_yield);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default realm_id when none is given
            int ret = realm.read_default_id(dpp(), realm_id, null_yield);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = period_config.read(dpp(), static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm_id, null_yield);
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
          encode_json("bucket quota", period_config.bucket_quota, formatter.get());
        } else if (quota_scope == "user") {
          set_quota_info(period_config.user_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("user quota", period_config.user_quota, formatter.get());
        } else if (quota_scope.empty() && opt_cmd == OPT::GLOBAL_QUOTA_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket quota", period_config.bucket_quota, formatter.get());
          encode_json("user quota", period_config.user_quota, formatter.get());
        } else {
          cerr << "ERROR: invalid quota scope specification. Please specify "
              "either --quota-scope=bucket, or --quota-scope=user" << std::endl;
          return EINVAL;
        }
        formatter->close_section();

        if (opt_cmd != OPT::GLOBAL_QUOTA_GET) {
          // write the modified period config
          ret = period_config.write(dpp(), static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm_id, null_yield);
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
    case OPT::REALM_CREATE:
      {
	if (realm_name.empty()) {
	  cerr << "missing realm name" << std::endl;
	  return EINVAL;
	}

	RGWRealm realm(realm_name, g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj);
	int ret = realm.create(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create realm " << realm_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = realm.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("realm", realm, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::REALM_DELETE:
      {
	if (empty_opt(opt_realm_name) && empty_opt(opt_realm_id)) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(safe_opt(opt_realm_id), safe_opt(opt_realm_name));
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.delete_obj(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't : " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT::REALM_GET:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  if (ret == -ENOENT && realm_name.empty() && realm_id.empty()) {
	    cerr << "missing realm name or id, or default realm not found" << std::endl;
	  } else {
	    cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
          }
	  return -ret;
	}
	encode_json("realm", realm, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::REALM_GET_DEFAULT:
      {
	RGWRealm realm(g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj);
	string default_id;
	int ret = realm.read_default_id(dpp(), default_id, null_yield);
	if (ret == -ENOENT) {
	  cout << "No default realm is set" << std::endl;
	  return -ret;
	} else if (ret < 0) {
	  cerr << "Error reading default realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	cout << "default realm: " << default_id << std::endl;
      }
      break;
    case OPT::REALM_LIST:
      {
	RGWRealm realm(g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj);
	string default_id;
	int ret = realm.read_default_id(dpp(), default_id, null_yield);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default realm: " << cpp_strerror(-ret) << std::endl;
	}
	list<string> realms;
	ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_realms(dpp(), realms);
	if (ret < 0) {
	  cerr << "failed to list realms: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("realms_list");
	encode_json("default_info", default_id, formatter.get());
	encode_json("realms", realms, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::REALM_LIST_PERIODS:
      {
        int ret = read_current_period_id(static_cast<rgw::sal::RadosStore*>(store), realm_id, realm_name, &period_id);
	if (ret < 0) {
	  return -ret;
	}
	list<string> periods;
	ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_periods(dpp(), period_id, periods, null_yield);
	if (ret < 0) {
	  cerr << "list periods failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	formatter->open_object_section("realm_periods_list");
	encode_json("current_period", period_id, formatter.get());
	encode_json("periods", periods, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;

    case OPT::REALM_RENAME:
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
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "realm.init failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.rename(dpp(), realm_new_name, null_yield);
	if (ret < 0) {
	  cerr << "realm.rename failed: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        cout << "Realm name updated. Note that this change only applies to "
            "the current cluster, so this command must be run separately "
            "on each of the realm's other clusters." << std::endl;
      }
      break;
    case OPT::REALM_SET:
      {
	if (realm_id.empty() && realm_name.empty()) {
	  cerr << "no realm name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(realm_id, realm_name);
	bool new_realm = false;
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
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
	  ret = realm.create(dpp(), null_yield);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't create new realm: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	} else {
	  ret = realm.update(dpp(), null_yield);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't store realm info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = realm.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }
	encode_json("realm", realm, formatter.get());
	formatter->flush(cout);
      }
      break;

    case OPT::REALM_DEFAULT:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = realm.set_as_default(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to set realm as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::REALM_PULL:
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
        int ret = send_to_url(url, opt_region, access_key, secret_key, info, bl, p);
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
        realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield, false);
        try {
          decode_json_obj(realm, &p);
        } catch (const JSONDecoder::err& e) {
          cerr << "failed to decode JSON response: " << e.what() << std::endl;
          return EINVAL;
        }
        RGWPeriod period;
        auto& current_period = realm.get_current_period();
        if (!current_period.empty()) {
          // pull the latest epoch of the realm's current period
          ret = do_period_pull(nullptr, url, opt_region,
                               access_key, secret_key,
                               realm_id, realm_name, current_period, "",
                               &period);
          if (ret < 0) {
            cerr << "could not fetch period " << current_period << std::endl;
            return -ret;
          }
        }
        ret = realm.create(dpp(), null_yield, false);
        if (ret < 0 && ret != -EEXIST) {
          cerr << "Error storing realm " << realm.get_id() << ": "
            << cpp_strerror(ret) << std::endl;
          return -ret;
        } else if (ret ==-EEXIST) {
	  ret = realm.update(dpp(), null_yield);
	  if (ret < 0) {
	    cerr << "Error storing realm " << realm.get_id() << ": "
		 << cpp_strerror(ret) << std::endl;
	  }
	}

        if (set_default) {
          ret = realm.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set realm " << realm_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("realm", realm, formatter.get());
        formatter->flush(cout);
      }
      break;

    case OPT::ZONEGROUP_ADD:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id,zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to initialize zonegroup " << zonegroup_name << " id " << zonegroup_id << ": "
	       << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        if (zone.realm_id != zonegroup.realm_id) {
          zone.realm_id = zonegroup.realm_id;
          ret = zone.update(dpp(), null_yield);
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

        ret = zonegroup.add_zone(dpp(), zone,
                                 (is_master_set ? &is_master : NULL),
                                 (is_read_only_set ? &read_only : NULL),
                                 endpoints, ptier_type,
                                 psync_from_all, sync_from, sync_from_rm,
                                 predirect_zone, bucket_index_max_shards,
				 static_cast<rgw::sal::RadosStore*>(store)->svc()->sync_modules->get_manager(),
				 null_yield);
	if (ret < 0) {
	  cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name() << ": "
	       << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        encode_json("zonegroup", zonegroup, formatter.get());
        formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_CREATE:
      {
	if (zonegroup_name.empty()) {
	  cerr << "Missing zonegroup name" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup(zonegroup_name, is_master, g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm.get_id(), endpoints);
        zonegroup.api_name = (api_name.empty() ? zonegroup_name : api_name);
	ret = zonegroup.create(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to create zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = zonegroup.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_DEFAULT:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zonegroup.set_as_default(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to set zonegroup as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_DELETE:
      {
	if (empty_opt(opt_zonegroup_id) && empty_opt(opt_zonegroup_name)) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup(safe_opt(opt_zonegroup_id), safe_opt(opt_zonegroup_name));
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj,
				 null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zonegroup.delete_obj(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_GET:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("zonegroup", zonegroup, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_LIST:
      {
	RGWZoneGroup zonegroup;
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj,
				 null_yield, false);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	list<string> zonegroups;
	ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_zonegroups(dpp(), zonegroups);
	if (ret < 0) {
	  cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	string default_zonegroup;
	ret = zonegroup.read_default_id(dpp(), default_zonegroup, null_yield);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zonegroup: " << cpp_strerror(-ret) << std::endl;
	}
	formatter->open_object_section("zonegroups_list");
	encode_json("default_info", default_zonegroup, formatter.get());
	encode_json("zonegroups", zonegroups, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_MODIFY:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
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
	  zonegroup.update_master(dpp(), is_master, null_yield);
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
          RGWRealm realm{g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj};
          ret = realm.read_id(dpp(), realm_name, zonegroup.realm_id, null_yield);
          if (ret < 0) {
            cerr << "failed to find realm by name " << realm_name << std::endl;
            return -ret;
          }
          need_update = true;
        }

        if (bucket_index_max_shards) {
          for (auto& [name, zone] : zonegroup.zones) {
            zone.bucket_index_max_shards = *bucket_index_max_shards;
          }
          need_update = true;
        }

        if (need_update) {
	  ret = zonegroup.update(dpp(), null_yield);
	  if (ret < 0) {
	    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = zonegroup.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zonegroup", zonegroup, formatter.get());
        formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_SET:
      {
	RGWRealm realm(realm_id, realm_name);
	int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	bool default_realm_not_exist = (ret == -ENOENT && realm_id.empty() && realm_name.empty());

	if (ret < 0 && !default_realm_not_exist ) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
	ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj,
			     null_yield, false);
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
	ret = zonegroup.create(dpp(), null_yield);
	if (ret < 0 && ret != -EEXIST) {
	  cerr << "ERROR: couldn't create zonegroup info: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	} else if (ret == -EEXIST) {
	  ret = zonegroup.update(dpp(), null_yield);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't store zonegroup info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = zonegroup.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set zonegroup " << zonegroup_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zonegroup", zonegroup, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_REMOVE:
      {
        RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
        int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
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

        ret = zonegroup.remove_zone(dpp(), zone_id, null_yield);
        if (ret < 0) {
          cerr << "failed to remove zone: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zonegroup", zonegroup, formatter.get());
        formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_RENAME:
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
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zonegroup.rename(dpp(), zonegroup_new_name, null_yield);
	if (ret < 0) {
	  cerr << "failed to rename zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_LIST:
      {
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj,
				 null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("placement_targets", zonegroup.placement_targets, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_GET:
      {
	if (placement_id.empty()) {
	  cerr << "ERROR: --placement-id not specified" << std::endl;
	  return EINVAL;
	}

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	auto p = zonegroup.placement_targets.find(placement_id);
	if (p == zonegroup.placement_targets.end()) {
	  cerr << "failed to find a zonegroup placement target named '" << placement_id << "'" << std::endl;
	  return -ENOENT;
	}
	encode_json("placement_targets", p->second, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_ADD:
    case OPT::ZONEGROUP_PLACEMENT_MODIFY:
    case OPT::ZONEGROUP_PLACEMENT_RM:
    case OPT::ZONEGROUP_PLACEMENT_DEFAULT:
      {
    if (placement_id.empty()) {
      cerr << "ERROR: --placement-id not specified" << std::endl;
      return EINVAL;
    }

    rgw_placement_rule rule;
    rule.from_str(placement_id);

    if (!rule.storage_class.empty() && opt_storage_class &&
        rule.storage_class != *opt_storage_class) {
      cerr << "ERROR: provided contradicting storage class configuration" << std::endl;
      return EINVAL;
    } else if (rule.storage_class.empty()) {
      rule.storage_class = opt_storage_class.value_or(string());
    }

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

    if (opt_cmd == OPT::ZONEGROUP_PLACEMENT_ADD ||
      opt_cmd == OPT::ZONEGROUP_PLACEMENT_MODIFY) {
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

      /* Tier options */
      bool tier_class = false;
      std::string storage_class = rule.get_storage_class();
      RGWZoneGroupPlacementTier t{storage_class};
      RGWZoneGroupPlacementTier *pt = &t;

	  auto ptiter = target.tier_targets.find(storage_class);
	  if (ptiter != target.tier_targets.end()) {
        pt = &ptiter->second;
        tier_class = true;
      } else if (tier_type_specified) {
        if (tier_type == "cloud-s3") {
          /* we support only cloud-s3 tier-type for now.
           * Once set cant be reset. */
          tier_class = true;
          pt->tier_type = tier_type;
          pt->storage_class = storage_class;
        } else {
	      cerr << "ERROR: Invalid tier-type specified" << std::endl;
	      return EINVAL;
        }
      }

      if (tier_class) {
        if (tier_config_add.size() > 0) {
          JSONFormattable tconfig;
          for (auto add : tier_config_add) {
            int r = tconfig.set(add.first, add.second);
            if (r < 0) {
              cerr << "ERROR: failed to set configurable: " << add << std::endl;
              return EINVAL;
            }
          }
          int r = pt->update_params(tconfig);
          if (r < 0) {
            cerr << "ERROR: failed to update tier_config options"<< std::endl;
          }
        }
        if (tier_config_rm.size() > 0) {
          JSONFormattable tconfig;
          for (auto add : tier_config_rm) {
            int r = tconfig.set(add.first, add.second);
            if (r < 0) {
              cerr << "ERROR: failed to set configurable: " << add << std::endl;
              return EINVAL;
            }
          }
          int r = pt->clear_params(tconfig);
          if (r < 0) {
            cerr << "ERROR: failed to update tier_config options"<< std::endl;
          }
        }

        target.tier_targets.emplace(std::make_pair(storage_class, *pt));
      }

    } else if (opt_cmd == OPT::ZONEGROUP_PLACEMENT_RM) {
      if (!opt_storage_class || opt_storage_class->empty()) {
        zonegroup.placement_targets.erase(placement_id);
      } else {
        auto iter = zonegroup.placement_targets.find(placement_id);
        if (iter != zonegroup.placement_targets.end()) {
          RGWZoneGroupPlacementTarget& info = zonegroup.placement_targets[placement_id];
          info.storage_classes.erase(*opt_storage_class);

	      auto ptiter = info.tier_targets.find(*opt_storage_class);
	      if (ptiter != info.tier_targets.end()) {
		    info.tier_targets.erase(ptiter);
	      }
        }
      }
    } else if (opt_cmd == OPT::ZONEGROUP_PLACEMENT_DEFAULT) {
      if (!zonegroup.placement_targets.count(placement_id)) {
        cerr << "failed to find a zonegroup placement target named '"
             << placement_id << "'" << std::endl;
        return -ENOENT;
      }
      zonegroup.default_placement = rule;
    }

    zonegroup.post_process_params(dpp(), null_yield);
    ret = zonegroup.update(dpp(), null_yield);
    if (ret < 0) {
      cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("placement_targets", zonegroup.placement_targets, formatter.get());
    formatter->flush(cout);
      }
      break;
    case OPT::ZONE_CREATE:
      {
        if (zone_name.empty()) {
	  cerr << "zone name not provided" << std::endl;
	  return EINVAL;
        }
	int ret;
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	/* if the user didn't provide zonegroup info , create stand alone zone */
	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
	  ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	  if (ret < 0) {
	    cerr << "unable to initialize zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  if (realm_id.empty() && realm_name.empty()) {
	    realm_id = zonegroup.realm_id;
	  }
	}

	RGWZoneParams zone(zone_id, zone_name);
	ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield, false);
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

	ret = zone.create(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to create zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
          string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
          bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
          string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);
	  ret = zonegroup.add_zone(dpp(), zone,
                                   (is_master_set ? &is_master : NULL),
                                   (is_read_only_set ? &read_only : NULL),
                                   endpoints,
                                   ptier_type,
                                   psync_from_all,
                                   sync_from, sync_from_rm,
                                   predirect_zone, bucket_index_max_shards,
				   static_cast<rgw::sal::RadosStore*>(store)->svc()->sync_modules->get_manager(),
				   null_yield);
	  if (ret < 0) {
	    cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name()
		 << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = zone.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_DEFAULT:
      {
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.set_as_default(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to set zone as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONE_DELETE:
      {
	if (empty_opt(opt_zone_id) && empty_opt(opt_zone_name)) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(safe_opt(opt_zone_id), safe_opt(opt_zone_name));
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        list<string> zonegroups;
	ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_zonegroups(dpp(), zonegroups);
	if (ret < 0) {
	  cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        for (list<string>::iterator iter = zonegroups.begin(); iter != zonegroups.end(); ++iter) {
          RGWZoneGroup zonegroup(string(), *iter);
          int ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
          if (ret < 0) {
            cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
            continue;
          }
          ret = zonegroup.remove_zone(dpp(), zone.get_id(), null_yield);
          if (ret < 0 && ret != -ENOENT) {
            cerr << "failed to remove zone " << zone.get_name() << " from zonegroup " << zonegroup.get_name() << ": "
              << cpp_strerror(-ret) << std::endl;
          }
        }

	ret = zone.delete_obj(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to delete zone " << zone.get_name() << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONE_GET:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("zone", zone, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_SET:
      {
	RGWZoneParams zone(zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield,
			    false);
	if (ret < 0) {
	  return -ret;
	}

        ret = zone.read(dpp(), null_yield);
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
	  int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	  if (ret < 0 && ret != -ENOENT) {
	    cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  zone.realm_id = realm.get_id();
	}

	if( !zone_name.empty() && !zone.get_name().empty() && zone.get_name() != zone_name) {
	  cerr << "Error: zone name " << zone_name << " is different than the zone name " << zone.get_name() << " in the provided json " << std::endl;
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
	ret = zone.fix_pool_names(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't fix zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.write(dpp(), false, null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create zone: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	}

        if (set_default) {
          ret = zone.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_LIST:
      {
	list<string> zones;
	int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_zones(dpp(), zones);
	if (ret < 0) {
	  cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneParams zone;
	ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield, false);
	if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	string default_zone;
	ret = zone.read_default_id(dpp(), default_zone, null_yield);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zone: " << cpp_strerror(-ret) << std::endl;
	}
	formatter->open_object_section("zones_list");
	encode_json("default_info", default_zone, formatter.get());
	encode_json("zones", zones, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_MODIFY:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
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
          RGWRealm realm{g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj};
          ret = realm.read_id(dpp(), realm_name, zone.realm_id, null_yield);
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
          ret = zone.update(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        string *ptier_type = (tier_type_specified ? &tier_type : nullptr);

        bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        ret = zonegroup.add_zone(dpp(), zone,
                                 (is_master_set ? &is_master : NULL),
                                 (is_read_only_set ? &read_only : NULL),
                                 endpoints, ptier_type,
                                 psync_from_all, sync_from, sync_from_rm,
                                 predirect_zone, bucket_index_max_shards,
				 static_cast<rgw::sal::RadosStore*>(store)->svc()->sync_modules->get_manager(),
				 null_yield);
	if (ret < 0) {
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zonegroup.update(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = zone.set_as_default(dpp(), null_yield);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zone", zone, formatter.get());
        formatter->flush(cout);
      }
      break;
    case OPT::ZONE_RENAME:
      {
	if (zone_new_name.empty()) {
	  cerr << " missing zone new name" << std::endl;
	  return EINVAL;
	}
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone(zone_id,zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	ret = zone.rename(dpp(), zone_new_name, null_yield);
	if (ret < 0) {
	  cerr << "failed to rename zone " << zone_name << " to " << zone_new_name << ": " << cpp_strerror(-ret)
	       << std::endl;
	  return -ret;
	}
	RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "WARNING: failed to initialize zonegroup " << zonegroup_name << std::endl;
	} else {
	  ret = zonegroup.rename_zone(dpp(), zone, null_yield);
	  if (ret < 0) {
	    cerr << "Error in zonegroup rename for " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}
      }
      break;
    case OPT::ZONE_PLACEMENT_ADD:
    case OPT::ZONE_PLACEMENT_MODIFY:
    case OPT::ZONE_PLACEMENT_RM:
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
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opt_cmd == OPT::ZONE_PLACEMENT_ADD ||
	    opt_cmd == OPT::ZONE_PLACEMENT_MODIFY) {
	  RGWZoneGroup zonegroup(zonegroup_id, zonegroup_name);
	  ret = zonegroup.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	  if (ret < 0) {
	    cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }

	  auto ptiter = zonegroup.placement_targets.find(placement_id);
	  if (ptiter == zonegroup.placement_targets.end()) {
	    cerr << "ERROR: placement id '" << placement_id << "' is not configured in zonegroup placement targets" << std::endl;
	    return EINVAL;
	  }

	  string storage_class = rgw_placement_rule::get_canonical_storage_class(opt_storage_class.value_or(string()));
	  if (ptiter->second.storage_classes.find(storage_class) == ptiter->second.storage_classes.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is not defined in zonegroup '" << placement_id << "' placement target" << std::endl;
	    return EINVAL;
	  }
	  if (ptiter->second.tier_targets.find(storage_class) != ptiter->second.tier_targets.end()) {
	    cerr << "ERROR: storage class '" << storage_class << "' is of tier type in zonegroup '" << placement_id << "' placement target" << std::endl;
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
        } else if (opt_cmd == OPT::ZONE_PLACEMENT_RM) {
          if (!opt_storage_class ||
              opt_storage_class->empty()) {
            zone.placement_pools.erase(placement_id);
          } else {
            auto iter = zone.placement_pools.find(placement_id);
            if (iter != zone.placement_pools.end()) {
              RGWZonePlacementInfo& info = zone.placement_pools[placement_id];
              info.storage_classes.remove_storage_class(*opt_storage_class);
            }
          }
        }

        ret = zone.update(dpp(), null_yield);
        if (ret < 0) {
          cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        encode_json("zone", zone, formatter.get());
        formatter->flush(cout);
      }
      break;
    case OPT::ZONE_PLACEMENT_LIST:
      {
	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("placement_pools", zone.placement_pools, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_PLACEMENT_GET:
      {
	if (placement_id.empty()) {
	  cerr << "ERROR: --placement-id not specified" << std::endl;
	  return EINVAL;
	}

	RGWZoneParams zone(zone_id, zone_name);
	int ret = zone.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	auto p = zone.placement_pools.find(placement_id);
	if (p == zone.placement_pools.end()) {
	  cerr << "ERROR: zone placement target '" << placement_id << "' not found" << std::endl;
	  return -ENOENT;
	}
	encode_json("placement_pools", p->second, formatter.get());
	formatter->flush(cout);
      }
    default:
      break;
    }
    return 0;
  }

  resolve_zone_id_opt(opt_effective_zone_name, opt_effective_zone_id);
  resolve_zone_id_opt(opt_source_zone_name, opt_source_zone_id);
  resolve_zone_id_opt(opt_dest_zone_name, opt_dest_zone_id);
  resolve_zone_ids_opt(opt_zone_names, opt_zone_ids);
  resolve_zone_ids_opt(opt_source_zone_names, opt_source_zone_ids);
  resolve_zone_ids_opt(opt_dest_zone_names, opt_dest_zone_ids);

  bool non_master_cmd = (!store->is_meta_master() && !yes_i_really_mean_it);
  std::set<OPT> non_master_ops_list = {OPT::USER_CREATE, OPT::USER_RM, 
                                        OPT::USER_MODIFY, OPT::USER_ENABLE,
                                        OPT::USER_SUSPEND, OPT::SUBUSER_CREATE,
                                        OPT::SUBUSER_MODIFY, OPT::SUBUSER_RM,
                                        OPT::BUCKET_LINK, OPT::BUCKET_UNLINK,
                                        OPT::BUCKET_RESHARD, OPT::BUCKET_RM,
                                        OPT::BUCKET_CHOWN, OPT::METADATA_PUT,
                                        OPT::METADATA_RM, OPT::RESHARD_CANCEL,
                                        OPT::RESHARD_ADD, OPT::MFA_CREATE,
                                        OPT::MFA_REMOVE, OPT::MFA_RESYNC,
                                        OPT::CAPS_ADD, OPT::CAPS_RM};

  bool print_warning_message = (non_master_ops_list.find(opt_cmd) != non_master_ops_list.end() &&
                                non_master_cmd);

  if (print_warning_message) {
      cerr << "Please run the command on master zone. Performing this operation on non-master zone leads to inconsistent metadata between zones" << std::endl;
      cerr << "Are you sure you want to go ahead? (requires --yes-i-really-mean-it)" << std::endl;
      return EINVAL;
  }

  if (!rgw::sal::User::empty(user)) {
    user_op.set_user_id(user->get_id());
    bucket_op.set_user_id(user->get_id());
  }

  if (!display_name.empty())
    user_op.set_display_name(display_name);

  if (!user_email.empty())
    user_op.set_user_email(user_email);

  if (!rgw::sal::User::empty(user)) {
    user_op.set_new_user_id(new_user_id);
  }

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
  if (opt_cmd == OPT::USER_ENABLE)
    user_op.set_suspension(false);
  else if (opt_cmd == OPT::USER_SUSPEND)
    user_op.set_suspension(true);

  if (!placement_id.empty() ||
      (opt_storage_class && !opt_storage_class->empty())) {
    rgw_placement_rule target_rule;
    target_rule.name = placement_id;
    target_rule.storage_class = *opt_storage_class;
    if (!store->get_zone()->get_params().valid_placement(target_rule)) {
      cerr << "NOTICE: invalid dest placement: " << target_rule.to_str() << std::endl;
      return EINVAL;
    }
    user_op.set_default_placement(target_rule);
  }

  if (!tags.empty()) {
    user_op.set_placement_tags(tags);
  }

  // RGWUser to use for user operations
  RGWUser ruser;
  int ret = 0;
  if (!(rgw::sal::User::empty(user) && access_key.empty()) || !subuser.empty()) {
    ret = ruser.init(dpp(), store, user_op, null_yield);
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
  case OPT::USER_INFO:
    if (rgw::sal::User::empty(user) && access_key.empty()) {
      cerr << "ERROR: --uid or --access-key required" << std::endl;
      return EINVAL;
    }
    break;
  case OPT::USER_CREATE:
    if (!user_op.has_existing_user()) {
      user_op.set_generate_key(); // generate a new key by default
    }
    ret = ruser.add(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      if (ret == -ERR_INVALID_TENANT_NAME)
	ret = -EINVAL;

      return -ret;
    }
    if (!subuser.empty()) {
      ret = ruser.subusers.add(dpp(),user_op, null_yield, &err_msg);
      if (ret < 0) {
        cerr << "could not create subuser: " << err_msg << std::endl;
        return -ret;
      }
    }
    break;
  case OPT::USER_RM:
    ret = ruser.remove(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove user: " << err_msg << std::endl;
      return -ret;
    }

    output_user_info = false;
    break;
  case OPT::USER_RENAME:
    if (yes_i_really_mean_it) {
      user_op.set_overwrite_new_user(true);
    }
    ret = ruser.rename(user_op, null_yield, dpp(), &err_msg);
    if (ret < 0) {
      if (ret == -EEXIST) {
        err_msg += ". to overwrite this user, add --yes-i-really-mean-it";
      }
      cerr << "could not rename user: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::USER_ENABLE:
  case OPT::USER_SUSPEND:
  case OPT::USER_MODIFY:
    ret = ruser.modify(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not modify user: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::SUBUSER_CREATE:
    ret = ruser.subusers.add(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not create subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::SUBUSER_MODIFY:
    ret = ruser.subusers.modify(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not modify subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::SUBUSER_RM:
    ret = ruser.subusers.remove(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove subuser: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::CAPS_ADD:
    ret = ruser.caps.add(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not add caps: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::CAPS_RM:
    ret = ruser.caps.remove(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove caps: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::KEY_CREATE:
    ret = ruser.keys.add(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not create key: " << err_msg << std::endl;
      return -ret;
    }

    break;
  case OPT::KEY_RM:
    ret = ruser.keys.remove(dpp(), user_op, null_yield, &err_msg);
    if (ret < 0) {
      cerr << "could not remove key: " << err_msg << std::endl;
      return -ret;
    }
    break;
  case OPT::PERIOD_PUSH:
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
      int ret = period.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
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
      ret = send_to_remote_or_url(nullptr, url, opt_region,
                                  access_key, secret_key,
                                  info, bl, p);
      if (ret < 0) {
        cerr << "request failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
    return 0;
  case OPT::PERIOD_UPDATE:
    {
      int ret = update_period(realm_id, realm_name, period_id, period_epoch,
                              commit, remote, url, opt_region,
                              access_key, secret_key,
                              formatter.get(), yes_i_really_mean_it);
      if (ret < 0) {
	return -ret;
      }
    }
    return 0;
  case OPT::PERIOD_COMMIT:
    {
      // read realm and staging period
      RGWRealm realm(realm_id, realm_name);
      int ret = realm.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, null_yield);
      if (ret < 0) {
        cerr << "Error initializing realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      RGWPeriod period(RGWPeriod::get_staging_id(realm.get_id()), 1);
      ret = period.init(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->sysobj, realm.get_id(), null_yield);
      if (ret < 0) {
        cerr << "period init failed: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      ret = commit_period(realm, period, remote, url, opt_region, access_key, secret_key,
                          yes_i_really_mean_it);
      if (ret < 0) {
        cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("period", period, formatter.get());
      formatter->flush(cout);
    }
    return 0;
  case OPT::ROLE_CREATE:
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
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant, path, assume_role_doc);
      ret = role->create(dpp(), true, null_yield);
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role.get(), formatter.get());
      return 0;
    }
  case OPT::ROLE_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      ret = role->delete_obj(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "role: " << role_name << " successfully deleted" << std::endl;
      return 0;
    }
  case OPT::ROLE_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: empty role name" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role.get(), formatter.get());
      return 0;
    }
  case OPT::ROLE_MODIFY:
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

      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      role->update_trust_policy(assume_role_doc);
      ret = role->update(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Assume role policy document updated successfully for role: " << role_name << std::endl;
      return 0;
    }
  case OPT::ROLE_LIST:
    {
      vector<std::unique_ptr<rgw::sal::RGWRole>> result;
      ret = store->get_roles(dpp(), null_yield, path_prefix, tenant, result);
      if (ret < 0) {
        return -ret;
      }
      show_roles_info(result, formatter.get());
      return 0;
    }
  case OPT::ROLE_POLICY_PUT:
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

      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      role->set_perm_policy(policy_name, perm_policy_doc);
      ret = role->update(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Permission policy attached successfully" << std::endl;
      return 0;
    }
  case OPT::ROLE_POLICY_LIST:
    {
      if (role_name.empty()) {
        cerr << "ERROR: Role name is empty" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      std::vector<string> policy_names = role->get_role_policy_names();
      show_policy_names(policy_names, formatter.get());
      return 0;
    }
  case OPT::ROLE_POLICY_GET:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      int ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      string perm_policy;
      ret = role->get_role_policy(dpp(), policy_name, perm_policy);
      if (ret < 0) {
        return -ret;
      }
      show_perm_policy(perm_policy, formatter.get());
      return 0;
    }
  case OPT::ROLE_POLICY_DELETE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      if (policy_name.empty()) {
        cerr << "ERROR: policy name is empty" << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = store->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      ret = role->delete_policy(dpp(), policy_name);
      if (ret < 0) {
        return -ret;
      }
      ret = role->update(dpp(), null_yield);
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
    ret = ruser.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }
    show_user_info(info, formatter.get());
  }

  if (opt_cmd == OPT::POLICY) {
    if (format == "xml") {
      int ret = RGWBucketAdminOp::dump_s3_policy(store, bucket_op, cout, dpp());
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      int ret = RGWBucketAdminOp::get_policy(store, bucket_op, stream_flusher, dpp());
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  if (opt_cmd == OPT::BUCKET_LIMIT_CHECK) {
    void *handle;
    std::list<std::string> user_ids;
    metadata_key = "user";
    int max = 1000;

    bool truncated;

    if (!rgw::sal::User::empty(user)) {
      user_ids.push_back(user->get_id().id);
      ret =
	RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, stream_flusher,
				      null_yield, dpp(), warnings_only);
    } else {
      /* list users in groups of max-keys, then perform user-bucket
       * limit-check on each group */
     ret = store->meta_list_keys_init(dpp(), metadata_key, string(), &handle);
      if (ret < 0) {
	cerr << "ERROR: buckets limit check can't get user metadata_key: "
	     << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      do {
	ret = store->meta_list_keys_next(dpp(), handle, max, user_ids,
					      &truncated);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "ERROR: buckets limit check lists_keys_next(): "
	       << cpp_strerror(-ret) << std::endl;
	  break;
	} else {
	  /* ok, do the limit checks for this group */
	  ret =
	    RGWBucketAdminOp::limit_check(store, bucket_op, user_ids, stream_flusher,
					  null_yield, dpp(), warnings_only);
	  if (ret < 0)
	    break;
	}
	user_ids.clear();
      } while (truncated);
      store->meta_list_keys_complete(handle);
    }
    return -ret;
  } /* OPT::BUCKET_LIMIT_CHECK */

  if (opt_cmd == OPT::BUCKETS_LIST) {
    if (bucket_name.empty()) {
      if (!rgw::sal::User::empty(user)) {
        if (!user_op.has_existing_user()) {
          cerr << "ERROR: could not find user: " << user << std::endl;
          return -ENOENT;
        }
      }
      RGWBucketAdminOp::info(store, bucket_op, stream_flusher, null_yield, dpp());
    } else {
      int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      formatter->open_array_section("entries");

      int count = 0;

      static constexpr int MAX_PAGINATE_SIZE = 10000;
      static constexpr int DEFAULT_MAX_ENTRIES = 1000;

      if (max_entries < 0) {
	max_entries = DEFAULT_MAX_ENTRIES;
      }
      const int paginate_size = std::min(max_entries, MAX_PAGINATE_SIZE);

      string prefix;
      string delim;
      string ns;

      rgw::sal::Bucket::ListParams params;
      rgw::sal::Bucket::ListResults results;

      params.prefix = prefix;
      params.delim = delim;
      params.marker = rgw_obj_key(marker);
      params.ns = ns;
      params.enforce_ns = false;
      params.list_versions = true;
      params.allow_unordered = bool(allow_unordered);

      do {
        const int remaining = max_entries - count;
	ret = bucket->list(dpp(), params, std::min(remaining, paginate_size), results,
			   null_yield);
        if (ret < 0) {
          cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        count += results.objs.size();

        for (const auto& entry : results.objs) {
          encode_json("entry", entry, formatter.get());
        }
        formatter->flush(cout);
      } while (results.is_truncated && count < max_entries);

      formatter->close_section();
      formatter->flush(cout);
    } /* have bucket_name */
  } /* OPT::BUCKETS_LIST */

  if (opt_cmd == OPT::BUCKET_RADOS_LIST) {
    RGWRadosList lister(static_cast<rgw::sal::RadosStore*>(store),
			max_concurrent_ios, orphan_stale_secs, tenant);
    if (rgw_obj_fs) {
      lister.set_field_separator(*rgw_obj_fs);
    }

    if (bucket_name.empty()) {
      ret = lister.run(dpp());
    } else {
      ret = lister.run(dpp(), bucket_name);
    }

    if (ret < 0) {
      std::cerr <<
	"ERROR: bucket radoslist failed to finish before " <<
	"encountering error: " << cpp_strerror(-ret) << std::endl;
      std::cerr << "************************************"
	"************************************" << std::endl;
      std::cerr << "WARNING: THE RESULTS ARE NOT RELIABLE AND SHOULD NOT " <<
	"BE USED IN DELETING ORPHANS" << std::endl;
      std::cerr << "************************************"
	"************************************" << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BUCKET_STATS) {
    if (bucket_name.empty() && !bucket_id.empty()) {
      rgw_bucket bucket;
      if (!rgw_find_bucket_by_id(dpp(), store->ctx(), store, marker, bucket_id, &bucket)) {
        cerr << "failure: no such bucket id" << std::endl;
        return -ENOENT;
      }
      bucket_op.set_tenant(bucket.tenant);
      bucket_op.set_bucket_name(bucket.name);
    }
    bucket_op.set_fetch_stats(true);

    int r = RGWBucketAdminOp::info(store, bucket_op, stream_flusher, null_yield, dpp());
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT::BUCKET_LINK) {
    bucket_op.set_bucket_id(bucket_id);
    bucket_op.set_new_bucket_name(new_bucket_name);
    string err;
    int r = RGWBucketAdminOp::link(store, bucket_op, dpp(), &err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT::BUCKET_UNLINK) {
    int r = RGWBucketAdminOp::unlink(store, bucket_op, dpp());
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT::BUCKET_CHOWN) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }

    bucket_op.set_bucket_name(bucket_name);
    bucket_op.set_new_bucket_name(new_bucket_name);
    string err;
    string marker;

    int r = RGWBucketAdminOp::chown(store, bucket_op, marker, dpp(), &err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT::LOG_LIST) {
    // filter by date?
    if (date.size() && date.size() != 10) {
      cerr << "bad date format for '" << date << "', expect YYYY-MM-DD" << std::endl;
      return EINVAL;
    }

    formatter->reset();
    formatter->open_array_section("logs");
    RGWAccessHandle h;
    int r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->log_list_init(dpp(), date, &h);
    if (r == -ENOENT) {
      // no logs.
    } else {
      if (r < 0) {
        cerr << "log list: error " << r << std::endl;
        return -r;
      }
      while (true) {
        string name;
        int r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->log_list_next(h, &name);
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

  if (opt_cmd == OPT::LOG_SHOW || opt_cmd == OPT::LOG_RM) {
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

    if (opt_cmd == OPT::LOG_SHOW) {
      RGWAccessHandle h;

      int r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->log_show_init(dpp(), oid, &h);
      if (r < 0) {
	cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }

      formatter->reset();
      formatter->open_object_section("log");

      struct rgw_log_entry entry;

      // peek at first entry to get bucket metadata
      r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->log_show_next(dpp(), h, &entry);
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

	  rgw_format_ops_log_entry(entry, formatter.get());
	  formatter->flush(cout);
        }
next:
	r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->log_show_next(dpp(), h, &entry);
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
    if (opt_cmd == OPT::LOG_RM) {
      int r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->log_remove(dpp(), oid);
      if (r < 0) {
	cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
    }
  }

  if (opt_cmd == OPT::POOL_ADD) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to add!" << std::endl;
      exit(1);
    }

    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->add_bucket_placement(dpp(), pool, null_yield);
    if (ret < 0)
      cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT::POOL_RM) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to remove!" << std::endl;
      exit(1);
    }

    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->remove_bucket_placement(dpp(), pool, null_yield);
    if (ret < 0)
      cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT::POOLS_LIST) {
    set<rgw_pool> pools;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_placement_set(dpp(), pools, null_yield);
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

  if (opt_cmd == OPT::USAGE_SHOW) {
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


    if (!bucket_name.empty()) {
      int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
	cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }
    ret = RGWUsage::show(dpp(), store, user.get(), bucket.get(), start_epoch,
			 end_epoch, show_log_entries, show_log_sum, &categories,
			 stream_flusher);
    if (ret < 0) {
      cerr << "ERROR: failed to show usage" << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::USAGE_TRIM) {
    if (rgw::sal::User::empty(user) && bucket_name.empty() &&
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

    if (!bucket_name.empty()) {
      int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
	cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }
    ret = RGWUsage::trim(dpp(), store, user.get(), bucket.get(), start_epoch, end_epoch);
    if (ret < 0) {
      cerr << "ERROR: read_usage() returned ret=" << ret << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::USAGE_CLEAR) {
    if (!yes_i_really_mean_it) {
      cerr << "usage clear would remove *all* users usage data for all time" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return 1;
    }

    ret = RGWUsage::clear(dpp(), store);
    if (ret < 0) {
      return ret;
    }
  }


  if (opt_cmd == OPT::OLH_GET || opt_cmd == OPT::OLH_READLOG) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
  }

  if (opt_cmd == OPT::OLH_GET) {
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWOLHInfo olh;
    rgw_obj obj(bucket->get_key(), object);
    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->get_olh(dpp(), bucket->get_info(), obj, &olh);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("olh", olh, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::OLH_READLOG) {
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
    bool is_truncated;

    RGWObjectCtx rctx(store);
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);

    RGWObjState *state;

    ret = obj->get_obj_state(dpp(), &rctx, &state, null_yield);
    if (ret < 0) {
      return -ret;
    }

    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bucket_index_read_olh_log(dpp(), bucket->get_info(), *state, obj->get_obj(), 0, &log, &is_truncated);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("is_truncated", is_truncated, formatter.get());
    encode_json("log", log, formatter.get());
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BI_GET) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket->get_key(), object);
    if (!object_version.empty()) {
      obj.key.set_instance(object_version);
    }

    rgw_cls_bi_entry entry;

    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_get(dpp(), bucket->get_info(), obj, bi_index_type, &entry);
    if (ret < 0) {
      cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("entry", entry, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BI_PUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
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

    rgw_obj obj(bucket->get_key(), key);

    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_put(dpp(), bucket->get_key(), obj, entry);
    if (ret < 0) {
      cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BI_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int max_shards = (bucket->get_info().layout.current_index.layout.normal.num_shards > 0 ? bucket->get_info().layout.current_index.layout.normal.num_shards : 1);

    formatter->open_array_section("entries");

    int i = (specified_shard_id ? shard_id : 0);
    for (; i < max_shards; i++) {
      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(store)->getRados());
      int shard_id = (bucket->get_info().layout.current_index.layout.normal.num_shards > 0  ? i : -1);

      int ret = bs.init(bucket->get_key(), shard_id, bucket->get_info().layout.current_index, nullptr /* no RGWBucketInfo */, dpp());
      marker.clear();

      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << shard_id << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      do {
        entries.clear();
	// if object is specified, we use that as a filter to only retrieve some some entries
        ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_list(bs, object, marker, max_entries, &entries, &is_truncated);
        if (ret < 0) {
          cerr << "ERROR: bi_list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        list<rgw_cls_bi_entry>::iterator iter;
        for (iter = entries.begin(); iter != entries.end(); ++iter) {
          rgw_cls_bi_entry& entry = *iter;
          encode_json("entry", entry, formatter.get());
          marker = entry.idx;
        }
        formatter->flush(cout);
      } while (is_truncated);
      formatter->flush(cout);

      if (specified_shard_id)
        break;
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BI_PURGE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Bucket> cur_bucket;
    ret = init_bucket(user.get(), tenant, bucket_name, string(), &cur_bucket);
    if (ret == -ENOENT) {
      // no bucket entrypoint
    } else if (ret < 0) {
      cerr << "ERROR: could not init current bucket info for bucket_name=" << bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    } else if (cur_bucket->get_bucket_id() == bucket->get_bucket_id() &&
               !yes_i_really_mean_it) {
      cerr << "specified bucket instance points to a current bucket instance" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EINVAL;
    }

    ret = bucket->purge_instance(dpp());
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT::OBJECT_PUT) {
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

    int ret = data_access.get_bucket(dpp(), tenant, bucket_name, bucket_id, &b, null_yield);
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
    ret = obj->put(bl, attrs, dpp(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: put object returned error: " << cpp_strerror(-ret) << std::endl;
    }
  }

  if (opt_cmd == OPT::OBJECT_RM) {
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj_key key(object, object_version);
    ret = rgw_remove_object(dpp(), store, bucket.get(), key);

    if (ret < 0) {
      cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::OBJECT_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    if (object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }

    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);
    bool need_rewrite = true;
    if (min_rewrite_stripe_size > 0) {
      ret = check_min_obj_stripe_size(store, obj.get(), min_rewrite_stripe_size, &need_rewrite);
      if (ret < 0) {
        ldpp_dout(dpp(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << ret << dendl;
      }
    }
    if (need_rewrite) {
      ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->rewrite_obj(obj.get(), dpp(), null_yield);
      if (ret < 0) {
        cerr << "ERROR: object rewrite returned: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ldpp_dout(dpp(), 20) << "skipped object" << dendl;
    }
  }

  if (opt_cmd == OPT::OBJECTS_EXPIRE) {
    if (!static_cast<rgw::sal::RadosStore*>(store)->getRados()->process_expire_objects(dpp())) {
      cerr << "ERROR: process_expire_objects() processing returned error." << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::OBJECTS_EXPIRE_STALE_LIST) {
    ret = RGWBucketAdminOp::fix_obj_expiry(store, bucket_op, stream_flusher, dpp(), true);
    if (ret < 0) {
      cerr << "ERROR: listing returned " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::OBJECTS_EXPIRE_STALE_RM) {
    ret = RGWBucketAdminOp::fix_obj_expiry(store, bucket_op, stream_flusher, dpp(), false);
    if (ret < 0) {
      cerr << "ERROR: removing returned " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BUCKET_REWRITE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
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
    bool cls_filtered = true;

    rgw_obj_index_key marker;
    string empty_prefix;
    string empty_delimiter;

    formatter->open_object_section("result");
    formatter->dump_string("bucket", bucket_name);
    formatter->open_array_section("objects");

    constexpr uint32_t NUM_ENTRIES = 1000;
    uint16_t expansion_factor = 1;
    while (is_truncated) {
      RGWRados::ent_map_t result;
      result.reserve(NUM_ENTRIES);

      int r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->cls_bucket_list_ordered(
	dpp(), bucket->get_info(), RGW_NO_SHARD,
	marker, empty_prefix, empty_delimiter,
	NUM_ENTRIES, true, expansion_factor,
	result, &is_truncated, &cls_filtered, &marker,
	null_yield,
	rgw_bucket_object_check_filter);
      if (r < 0 && r != -ENOENT) {
        cerr << "ERROR: failed operation r=" << r << std::endl;
      } else if (r == -ENOENT) {
        break;
      }

      if (result.size() < NUM_ENTRIES / 8) {
	++expansion_factor;
      } else if (result.size() > NUM_ENTRIES * 7 / 8 &&
		 expansion_factor > 1) {
	--expansion_factor;
      }

      for (auto iter = result.begin(); iter != result.end(); ++iter) {
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
	  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);

          bool need_rewrite = true;
          if (min_rewrite_stripe_size > 0) {
            r = check_min_obj_stripe_size(store, obj.get(), min_rewrite_stripe_size, &need_rewrite);
            if (r < 0) {
              ldpp_dout(dpp(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
            }
          }
          if (!need_rewrite) {
            formatter->dump_string("status", "Skipped");
          } else {
            r = static_cast<rgw::sal::RadosStore*>(store)->getRados()->rewrite_obj(obj.get(), dpp(), null_yield);
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

  if (opt_cmd == OPT::BUCKET_RESHARD) {
    int ret = check_reshard_bucket_params(static_cast<rgw::sal::RadosStore*>(store),
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  &bucket);
    if (ret < 0) {
      return ret;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(store), bucket->get_info(), bucket->get_attrs(), nullptr /* no callback */);

#define DEFAULT_RESHARD_MAX_ENTRIES 1000
    if (max_entries < 1) {
      max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
    }

    return br.execute(num_shards, max_entries, dpp(),
                      verbose, &cout, formatter.get());
  }

  if (opt_cmd == OPT::RESHARD_ADD) {
    int ret = check_reshard_bucket_params(static_cast<rgw::sal::RadosStore*>(store),
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  &bucket);
    if (ret < 0) {
      return ret;
    }

    int num_source_shards = (bucket->get_info().layout.current_index.layout.normal.num_shards > 0 ? bucket->get_info().layout.current_index.layout.normal.num_shards : 1);

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), dpp());
    cls_rgw_reshard_entry entry;
    entry.time = real_clock::now();
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    entry.bucket_id = bucket->get_info().bucket.bucket_id;
    entry.old_num_shards = num_source_shards;
    entry.new_num_shards = num_shards;

    return reshard.add(dpp(), entry);
  }

  if (opt_cmd == OPT::RESHARD_LIST) {
    list<cls_rgw_reshard_entry> entries;
    int ret;
    int count = 0;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int num_logshards =
      store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), dpp());

    formatter->open_array_section("reshard");
    for (int i = 0; i < num_logshards; i++) {
      bool is_truncated = true;
      string marker;
      do {
        entries.clear();
        ret = reshard.list(dpp(), i, marker, max_entries - count, entries, &is_truncated);
        if (ret < 0) {
          cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
          return ret;
        }
        for (auto iter=entries.begin(); iter != entries.end(); ++iter) {
          cls_rgw_reshard_entry& entry = *iter;
          encode_json("entry", entry, formatter.get());
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

  if (opt_cmd == OPT::RESHARD_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(store), bucket->get_info(), bucket->get_attrs(), nullptr /* no callback */);
    list<cls_rgw_bucket_instance_entry> status;
    int r = br.get_status(dpp(), &status);
    if (r < 0) {
      cerr << "ERROR: could not get resharding status for bucket " <<
	bucket_name << std::endl;
      return -r;
    }

    show_reshard_status(status, formatter.get());
  }

  if (opt_cmd == OPT::RESHARD_PROCESS) {
    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), true, &cout);

    int ret = reshard.process_all_logshards(dpp());
    if (ret < 0) {
      cerr << "ERROR: failed to process reshard logs, error=" << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::RESHARD_CANCEL) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    bool bucket_initable = true;
    ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      if (yes_i_really_mean_it) {
        bucket_initable = false;
      } else {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
          "; if you want to cancel the reshard request nonetheless, please "
          "use the --yes-i-really-mean-it option" << std::endl;
        return -ret;
      }
    }

    if (bucket_initable) {
      // we did not encounter an error, so let's work with the bucket
      RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(store), bucket->get_info(), bucket->get_attrs(),
                          nullptr /* no callback */);
      int ret = br.cancel(dpp());
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
    }

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), dpp());

    cls_rgw_reshard_entry entry;
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    //entry.bucket_id = bucket_id;

    ret = reshard.remove(dpp(), entry);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "Error in updating reshard log with bucket " <<
        bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  } // OPT_RESHARD_CANCEL

  if (opt_cmd == OPT::OBJECT_UNLINK) {
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<rgw_obj_index_key> oid_list;
    rgw_obj_key key(object, object_version);
    rgw_obj_index_key index_key;
    key.get_index_key(&index_key);
    oid_list.push_back(index_key);
    ret = bucket->remove_objs_from_index(dpp(), oid_list);
    if (ret < 0) {
      cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::OBJECT_STAT) {
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);

    RGWObjectCtx obj_ctx(store);

    ret = obj->get_obj_attrs(&obj_ctx, null_yield, dpp());
    if (ret < 0) {
      cerr << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
    formatter->open_object_section("object_metadata");
    formatter->dump_string("name", object);
    formatter->dump_unsigned("size", obj->get_obj_size());

    map<string, bufferlist>::iterator iter;
    map<string, bufferlist> other_attrs;
    for (iter = obj->get_attrs().begin(); iter != obj->get_attrs().end(); ++iter) {
      bufferlist& bl = iter->second;
      bool handled = false;
      if (iter->first == RGW_ATTR_MANIFEST) {
        handled = decode_dump<RGWObjManifest>("manifest", bl, formatter.get());
      } else if (iter->first == RGW_ATTR_ACL) {
        handled = decode_dump<RGWAccessControlPolicy>("policy", bl, formatter.get());
      } else if (iter->first == RGW_ATTR_ID_TAG) {
        handled = dump_string("tag", bl, formatter.get());
      } else if (iter->first == RGW_ATTR_ETAG) {
        handled = dump_string("etag", bl, formatter.get());
      } else if (iter->first == RGW_ATTR_COMPRESSION) {
        handled = decode_dump<RGWCompressionInfo>("compression", bl, formatter.get());
      } else if (iter->first == RGW_ATTR_DELETE_AT) {
        handled = decode_dump<utime_t>("delete_at", bl, formatter.get());
      }

      if (!handled)
        other_attrs[iter->first] = bl;
    }

    formatter->open_object_section("attrs");
    for (iter = other_attrs.begin(); iter != other_attrs.end(); ++iter) {
      dump_string(iter->first.c_str(), iter->second, formatter.get());
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BUCKET_CHECK) {
    if (check_head_obj_locator) {
      if (bucket_name.empty()) {
        cerr << "ERROR: need to specify bucket name" << std::endl;
        return EINVAL;
      }
      do_check_object_locator(tenant, bucket_name, fix, remove_bad, formatter.get());
    } else {
      RGWBucketAdminOp::check_index(store, bucket_op, stream_flusher, null_yield, dpp());
    }
  }

  if (opt_cmd == OPT::BUCKET_RM) {
    if (!inconsistent_index) {
      RGWBucketAdminOp::remove_bucket(store, bucket_op, null_yield, dpp(), bypass_gc, true);
    } else {
      if (!yes_i_really_mean_it) {
	cerr << "using --inconsistent_index can corrupt the bucket index " << std::endl
	<< "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
	return 1;
      }
      RGWBucketAdminOp::remove_bucket(store, bucket_op, null_yield, dpp(), bypass_gc, false);
    }
  }

  if (opt_cmd == OPT::GC_LIST) {
    int index = 0;
    bool truncated;
    bool processing_queue = false;
    formatter->open_array_section("entries");

    do {
      list<cls_rgw_gc_obj_info> result;
      int ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->list_gc_objs(&index, marker, 1000, !include_all, result, &truncated, processing_queue);
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
          encode_json("obj", obj, formatter.get());
	}
	formatter->close_section(); // objs
	formatter->close_section(); // obj_chain
	formatter->flush(cout);
      }
    } while (truncated);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::GC_PROCESS) {
    int ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->process_gc(!include_all);
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::LC_LIST) {
    formatter->open_array_section("lifecycle_list");
    vector<rgw::sal::Lifecycle::LCEntry> bucket_lc_map;
    string marker;
    int index{0};
#define MAX_LC_LIST_ENTRIES 100
    if (max_entries < 0) {
      max_entries = MAX_LC_LIST_ENTRIES;
    }
    do {
      int ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->list_lc_progress(marker, max_entries,
						    bucket_lc_map, index);
      if (ret < 0) {
        cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret)
	     << std::endl;
        return 1;
      }
      for (const auto& entry : bucket_lc_map) {
        formatter->open_object_section("bucket_lc_info");
        formatter->dump_string("bucket", entry.bucket);
	char exp_buf[100];
	time_t t{time_t(entry.start_time)};
	if (std::strftime(
	      exp_buf, sizeof(exp_buf),
	      "%a, %d %b %Y %T %Z", std::gmtime(&t))) {
	  formatter->dump_string("started", exp_buf);
	}
        string lc_status = LC_STATUS[entry.status];
        formatter->dump_string("status", lc_status);
        formatter->close_section(); // objs
        formatter->flush(cout);
      }
    } while (!bucket_lc_map.empty());

    formatter->close_section(); //lifecycle list
    formatter->flush(cout);
  }


  if (opt_cmd == OPT::LC_GET) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    RGWLifecycleConfiguration config;
    ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto aiter = bucket->get_attrs().find(RGW_ATTR_LC);
    if (aiter == bucket->get_attrs().end()) {
      return -ENOENT;
    }

    bufferlist::const_iterator iter{&aiter->second};
    try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      cerr << "ERROR: decode life cycle config failed" << std::endl;
      return -EIO;
    }

    encode_json("result", config, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::LC_PROCESS) {
    if ((! bucket_name.empty()) ||
	(! bucket_id.empty())) {
        int ret = init_bucket(nullptr, tenant, bucket_name, bucket_id, &bucket);
	if (ret < 0) {
	  cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret)
	       << std::endl;
	  return ret;
	}
    }

    int ret =
      static_cast<rgw::sal::RadosStore*>(store)->getRados()->process_lc(bucket);
    if (ret < 0) {
      cerr << "ERROR: lc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::LC_RESHARD_FIX) {
    ret = RGWBucketAdminOp::fix_lc_shards(store, bucket_op, stream_flusher, dpp());
    if (ret < 0) {
      cerr << "ERROR: fixing lc shards: " << cpp_strerror(-ret) << std::endl;
    }

  }

  if (opt_cmd == OPT::ORPHANS_FIND) {
    if (!yes_i_really_mean_it) {
      cerr << "this command is now deprecated; please consider using the rgw-orphan-list tool; "
	   << "accidental removal of active objects cannot be reversed; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    } else {
      cerr << "IMPORTANT: this command is now deprecated; please consider using the rgw-orphan-list tool"
	   << std::endl;
    }

    RGWOrphanSearch search(static_cast<rgw::sal::RadosStore*>(store), max_concurrent_ios, orphan_stale_secs);

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

    int ret = search.init(dpp(), job_id, &info, detail);
    if (ret < 0) {
      cerr << "could not init search, ret=" << ret << std::endl;
      return -ret;
    }
    ret = search.run(dpp());
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT::ORPHANS_FINISH) {
    if (!yes_i_really_mean_it) {
      cerr << "this command is now deprecated; please consider using the rgw-orphan-list tool; "
	   << "accidental removal of active objects cannot be reversed; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    } else {
      cerr << "IMPORTANT: this command is now deprecated; please consider using the rgw-orphan-list tool"
	   << std::endl;
    }

    RGWOrphanSearch search(static_cast<rgw::sal::RadosStore*>(store), max_concurrent_ios, orphan_stale_secs);

    if (job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    int ret = search.init(dpp(), job_id, NULL);
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

  if (opt_cmd == OPT::ORPHANS_LIST_JOBS){
    if (!yes_i_really_mean_it) {
      cerr << "this command is now deprecated; please consider using the rgw-orphan-list tool; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    } else {
      cerr << "IMPORTANT: this command is now deprecated; please consider using the rgw-orphan-list tool"
	   << std::endl;
    }

    RGWOrphanStore orphan_store(static_cast<rgw::sal::RadosStore*>(store));
    int ret = orphan_store.init(dpp());
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
	encode_json("orphan_search_state", it.second, formatter.get());
      }
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::USER_CHECK) {
    check_bad_user_bucket_mapping(store, user.get(), fix, null_yield, dpp());
  }

  if (opt_cmd == OPT::USER_STATS) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: uid not specified" << std::endl;
      return EINVAL;
    }
    if (reset_stats) {
      if (!bucket_name.empty()) {
	cerr << "ERROR: --reset-stats does not work on buckets and "
	  "bucket specified" << std::endl;
	return EINVAL;
      }
      if (sync_stats) {
	cerr << "ERROR: sync-stats includes the reset-stats functionality, "
	  "so at most one of the two should be specified" << std::endl;
	return EINVAL;
      }
      ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->user->reset_stats(dpp(), user->get_id(), null_yield);
      if (ret < 0) {
	cerr << "ERROR: could not reset user stats: " << cpp_strerror(-ret) <<
	  std::endl;
	return -ret;
      }
    }

    if (sync_stats) {
      if (!bucket_name.empty()) {
        int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
        if (ret < 0) {
          cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        ret = bucket->sync_user_stats(dpp(), null_yield);
        if (ret < 0) {
          cerr << "ERROR: could not sync bucket stats: " <<
	    cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      } else {
        int ret = rgw_user_sync_all_stats(dpp(), store, user.get(), null_yield);
        if (ret < 0) {
          cerr << "ERROR: could not sync user stats: " <<
	    cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      }
    }

    constexpr bool omit_utilized_stats = false;
    RGWStorageStats stats(omit_utilized_stats);
    ceph::real_time last_stats_sync;
    ceph::real_time last_stats_update;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->user->read_stats(dpp(), user->get_id(), &stats, null_yield,
										 &last_stats_sync,
										 &last_stats_update);
    if (ret < 0) {
      if (ret == -ENOENT) { /* in case of ENOENT */
        cerr << "User has not been initialized or user does not exist" << std::endl;
      } else {
        cerr << "ERROR: can't read user: " << cpp_strerror(ret) << std::endl;
      }
      return -ret;
    }


    {
      Formatter::ObjectSection os(*formatter, "result");
      encode_json("stats", stats, formatter.get());
      utime_t last_sync_ut(last_stats_sync);
      encode_json("last_stats_sync", last_sync_ut, formatter.get());
      utime_t last_update_ut(last_stats_update);
      encode_json("last_stats_update", last_update_ut, formatter.get());
    }
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::METADATA_GET) {
    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->get(metadata_key, formatter.get(), null_yield, dpp());
    if (ret < 0) {
      cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    formatter->flush(cout);
  }

  if (opt_cmd == OPT::METADATA_PUT) {
    bufferlist bl;
    int ret = read_input(infile, bl);
    if (ret < 0) {
      cerr << "ERROR: failed to read input: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->put(metadata_key, bl, null_yield, dpp(), RGWMDLogSyncType::APPLY_ALWAYS, false);
    if (ret < 0) {
      cerr << "ERROR: can't put key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::METADATA_RM) {
    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->remove(metadata_key, null_yield, dpp());
    if (ret < 0) {
      cerr << "ERROR: can't remove key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::METADATA_LIST || opt_cmd == OPT::USER_LIST) {
    if (opt_cmd == OPT::USER_LIST) {
      metadata_key = "user";
    }
    void *handle;
    int max = 1000;
    int ret = store->meta_list_keys_init(dpp(), metadata_key, marker, &handle);
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
      ret = store->meta_list_keys_next(dpp(), handle, left, keys, &truncated);
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
      encode_json("truncated", truncated, formatter.get());
      encode_json("count", count, formatter.get());
      if (truncated) {
        encode_json("marker", store->meta_get_marker(handle), formatter.get());
      }
      formatter->close_section();
    }
    formatter->flush(cout);

    store->meta_list_keys_complete(handle);
  }

  if (opt_cmd == OPT::MDLOG_LIST) {
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      std::cerr << "end-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      if (marker.empty()) {
	marker = start_marker;
      } else {
	std::cerr << "start-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    int i = (specified_shard_id ? shard_id : 0);

    if (period_id.empty()) {
      int ret = read_current_period_id(static_cast<rgw::sal::RadosStore*>(store), realm_id, realm_name, &period_id);
      if (ret < 0) {
        return -ret;
      }
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(store)->svc()->mdlog->get_log(period_id);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      void *handle;
      list<cls_log_entry> entries;

      meta_log->init_list_entries(i, {}, {}, marker, &handle);
      bool truncated;
      do {
	  int ret = meta_log->list_entries(dpp(), handle, 1000, entries, NULL, &truncated);
        if (ret < 0) {
          cerr << "ERROR: meta_log->list_entries(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        for (list<cls_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
          cls_log_entry& entry = *iter;
          static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->dump_log_entry(entry, formatter.get());
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

  if (opt_cmd == OPT::MDLOG_STATUS) {
    int i = (specified_shard_id ? shard_id : 0);

    if (period_id.empty()) {
      int ret = read_current_period_id(static_cast<rgw::sal::RadosStore*>(store), realm_id, realm_name, &period_id);
      if (ret < 0) {
        return -ret;
      }
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(store)->svc()->mdlog->get_log(period_id);

    formatter->open_array_section("entries");

    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      RGWMetadataLogInfo info;
      meta_log->get_info(dpp(), i, &info);

      ::encode_json("info", info, formatter.get());

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::MDLOG_AUTOTRIM) {
    // need a full history for purging old mdlog periods
    static_cast<rgw::sal::RadosStore*>(store)->svc()->mdlog->init_oldest_log_period(null_yield, dpp());

    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf()->rgw_md_log_max_shards;
    ret = crs.run(dpp(), create_admin_meta_log_trim_cr(dpp(), static_cast<rgw::sal::RadosStore*>(store), &http, num_shards));
    if (ret < 0) {
      cerr << "automated mdlog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::MDLOG_TRIM) {
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      std::cerr << "start-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      if (marker.empty()) {
	marker = end_marker;
      } else {
	std::cerr << "end-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for trim operation" << std::endl;
      return EINVAL;
    }

    if (marker.empty()) {
      cerr << "ERROR: marker must be specified for trim operation" << std::endl;
      return EINVAL;
    }

    if (period_id.empty()) {
      std::cerr << "missing --period argument" << std::endl;
      return EINVAL;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(store)->svc()->mdlog->get_log(period_id);

    // trim until -ENODATA
    do {
      ret = meta_log->trim(dpp(), shard_id, {}, {}, {}, marker);
    } while (ret == 0);
    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: meta_log->trim(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::SYNC_INFO) {
    sync_info(opt_effective_zone_id, opt_bucket, zone_formatter.get());
  }

  if (opt_cmd == OPT::SYNC_STATUS) {
    sync_status(formatter.get());
  }

  if (opt_cmd == OPT::METADATA_SYNC_STATUS) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_meta_sync_status sync_status;
    ret = sync.read_sync_status(dpp(), &sync_status);
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    formatter->open_object_section("summary");
    encode_json("sync_status", sync_status, formatter.get());

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
    encode_json("total", full_total, formatter.get());
    encode_json("complete", full_complete, formatter.get());
    formatter->close_section();
    formatter->close_section();

    formatter->flush(cout);

  }

  if (opt_cmd == OPT::METADATA_SYNC_INIT) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.init_sync_status(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }


  if (opt_cmd == OPT::METADATA_SYNC_RUN) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run(dpp(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATA_SYNC_STATUS) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor(), source_zone, nullptr);

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_data_sync_status sync_status;
    if (specified_shard_id) {
      set<string> pending_buckets;
      set<string> recovering_buckets;
      rgw_data_sync_marker sync_marker;
      ret = sync.read_shard_status(dpp(), shard_id, pending_buckets, recovering_buckets, &sync_marker, 
                                   max_entries_specified ? max_entries : 20);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: sync.read_shard_status() returned ret=" << ret << std::endl;
        return -ret;
      }
      formatter->open_object_section("summary");
      encode_json("shard_id", shard_id, formatter.get());
      encode_json("marker", sync_marker, formatter.get());
      encode_json("pending_buckets", pending_buckets, formatter.get());
      encode_json("recovering_buckets", recovering_buckets, formatter.get());
      formatter->close_section();
      formatter->flush(cout);
    } else {
      ret = sync.read_sync_status(dpp(), &sync_status);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
        return -ret;
      }

      formatter->open_object_section("summary");
      encode_json("sync_status", sync_status, formatter.get());

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
      encode_json("total", full_total, formatter.get());
      encode_json("complete", full_complete, formatter.get());
      formatter->close_section();
      formatter->close_section();

      formatter->flush(cout);
    }
  }

  if (opt_cmd == OPT::DATA_SYNC_INIT) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor(), source_zone, nullptr);

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.init_sync_status(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATA_SYNC_RUN) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWSyncModuleInstanceRef sync_module;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->sync_modules->get_manager()->create_instance(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone().tier_type,
        store->get_zone()->get_params().tier_config, &sync_module);
    if (ret < 0) {
      ldpp_dout(dpp(), -1) << "ERROR: failed to init sync module instance, ret=" << ret << dendl;
      return ret;
    }

    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor(), source_zone, nullptr, sync_module);

    ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BUCKET_SYNC_INIT) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket_for_sync(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto opt_sb = opt_source_bucket;
    if (opt_sb && opt_sb->bucket_id.empty()) {
      string sbid;
      std::unique_ptr<rgw::sal::Bucket> sbuck;
      int ret = init_bucket_for_sync(user.get(), opt_sb->tenant, opt_sb->name, sbid, &sbuck);
      if (ret < 0) {
        return -ret;
      }
      opt_sb = sbuck->get_key();
    }

    RGWBucketPipeSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), source_zone, opt_sb, bucket->get_key());

    ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.init_sync_status(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BUCKET_SYNC_CHECKPOINT) {
    std::optional<rgw_zone_id> opt_source_zone;
    if (!source_zone.empty()) {
      opt_source_zone = source_zone;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    if (!static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->bucket_imports_data(bucket->get_key(), null_yield, dpp())) {
      std::cout << "Sync is disabled for bucket " << bucket_name << std::endl;
      return 0;
    }

    RGWBucketSyncPolicyHandlerRef handler;
    ret = store->get_sync_policy_handler(dpp(), std::nullopt, bucket->get_key(), &handler, null_yield);
    if (ret < 0) {
      std::cerr << "ERROR: failed to get policy handler for bucket ("
          << bucket << "): r=" << ret << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto timeout_at = ceph::coarse_mono_clock::now() + opt_timeout_sec;
    ret = rgw_bucket_sync_checkpoint(dpp(), static_cast<rgw::sal::RadosStore*>(store), *handler, bucket->get_info(),
                                     opt_source_zone, opt_source_bucket,
                                     opt_retry_delay_ms, timeout_at);
    if (ret < 0) {
      ldpp_dout(dpp(), -1) << "bucket sync checkpoint failed: " << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  if ((opt_cmd == OPT::BUCKET_SYNC_DISABLE) || (opt_cmd == OPT::BUCKET_SYNC_ENABLE)) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    } 
    if (opt_cmd == OPT::BUCKET_SYNC_DISABLE) {
      bucket_op.set_sync_bucket(false);
    } else {
      bucket_op.set_sync_bucket(true);
    }
    bucket_op.set_tenant(tenant);
    string err_msg;
    ret = RGWBucketAdminOp::sync_bucket(store, bucket_op, dpp(), &err_msg);
    if (ret < 0) {
      cerr << err_msg << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BUCKET_SYNC_INFO) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    bucket_sync_info(static_cast<rgw::sal::RadosStore*>(store), bucket->get_info(), std::cout);
  }

  if (opt_cmd == OPT::BUCKET_SYNC_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    bucket_sync_status(static_cast<rgw::sal::RadosStore*>(store), bucket->get_info(), source_zone, opt_source_bucket, std::cout);
  }

  if (opt_cmd == OPT::BUCKET_SYNC_MARKERS) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket_for_sync(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketPipeSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), source_zone, opt_source_bucket, bucket->get_key());

    ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.read_sync_status(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    map<int, rgw_bucket_shard_sync_info>& sync_status = sync.get_sync_status();

    encode_json("sync_status", sync_status, formatter.get());
    formatter->flush(cout);
  }

 if (opt_cmd == OPT::BUCKET_SYNC_RUN) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket_for_sync(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    RGWBucketPipeSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), source_zone, opt_source_bucket, bucket->get_key());

    ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BILOG_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
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
      ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->log_list(dpp(), bucket->get_info(), shard_id, marker, max_entries - count, entries, &truncated);
      if (ret < 0) {
        cerr << "ERROR: list_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (list<rgw_bi_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
        rgw_bi_log_entry& entry = *iter;
        encode_json("entry", entry, formatter.get());

        marker = entry.id;
      }
      formatter->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::SYNC_ERROR_LIST) {
    if (max_entries < 0) {
      max_entries = 1000;
    }
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      std::cerr << "end-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      if (marker.empty()) {
	marker = start_marker;
      } else {
	std::cerr << "start-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    bool truncated;

    if (shard_id < 0) {
      shard_id = 0;
    }

    formatter->open_array_section("entries");

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      formatter->open_object_section("shard");
      encode_json("shard_id", shard_id, formatter.get());
      formatter->open_array_section("entries");

      int count = 0;
      string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);

      do {
        list<cls_log_entry> entries;
        ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->timelog.list(dpp(), oid, {}, {}, max_entries - count, entries, marker, &marker, &truncated,
					      null_yield);
	if (ret == -ENOENT) {
	  break;
        }
        if (ret < 0) {
          cerr << "ERROR: svc.cls->timelog.list(): " << cpp_strerror(-ret) << std::endl;
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
          encode_json("id", cls_entry.id, formatter.get());
          encode_json("section", cls_entry.section, formatter.get());
          encode_json("name", cls_entry.name, formatter.get());
          encode_json("timestamp", cls_entry.timestamp, formatter.get());
          encode_json("info", log_entry, formatter.get());
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

  if (opt_cmd == OPT::SYNC_ERROR_TRIM) {
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }

    if (shard_id < 0) {
      shard_id = 0;
    }

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      ret = trim_sync_error_log(shard_id, marker, trim_delay_ms);
      if (ret < 0) {
        cerr << "ERROR: sync error trim: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (specified_shard_id) {
        break;
      }
    }
  }

  if (opt_cmd == OPT::SYNC_GROUP_CREATE ||
      opt_cmd == OPT::SYNC_GROUP_MODIFY) {
    CHECK_TRUE(require_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_status), "ERROR: --status is not specified (options: forbidden, allowed, enabled)", EINVAL);

    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    if (opt_cmd == OPT::SYNC_GROUP_MODIFY) {
      auto iter = sync_policy.groups.find(*opt_group_id);
      if (iter == sync_policy.groups.end()) {
        cerr << "ERROR: could not find group '" << *opt_group_id << "'" << std::endl;
        return ENOENT;
      }
    }

    auto& group = sync_policy.groups[*opt_group_id];
    group.id = *opt_group_id;

    if (opt_status) {
      if (!group.set_status(*opt_status)) {
        cerr << "ERROR: unrecognized status (options: forbidden, allowed, enabled)" << std::endl;
        return EINVAL;
      }
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter.get(), cout);
  }

  if (opt_cmd == OPT::SYNC_GROUP_GET) {
    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto& groups = sync_policy.groups;

    if (!opt_group_id) {
      show_result(groups, zone_formatter.get(), cout);
    } else {
      auto iter = sync_policy.groups.find(*opt_group_id);
      if (iter == sync_policy.groups.end()) {
        cerr << "ERROR: could not find group '" << *opt_group_id << "'" << std::endl;
        return ENOENT;
      }

      show_result(iter->second, zone_formatter.get(), cout);
    }
  }

  if (opt_cmd == OPT::SYNC_GROUP_REMOVE) {
    CHECK_TRUE(require_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);

    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    sync_policy.groups.erase(*opt_group_id);

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    {
      Formatter::ObjectSection os(*zone_formatter.get(), "result");
      encode_json("sync_policy", sync_policy, zone_formatter.get());
    }

    zone_formatter->flush(cout);
  }

  if (opt_cmd == OPT::SYNC_GROUP_FLOW_CREATE) {
    CHECK_TRUE(require_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_flow_id), "ERROR: --flow-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_flow_type,
                           (symmetrical_flow_opt(*opt_flow_type) ||
                            directional_flow_opt(*opt_flow_type))),
                           "ERROR: --flow-type not specified or invalid (options: symmetrical, directional)", EINVAL);

    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    if (symmetrical_flow_opt(*opt_flow_type)) {
      CHECK_TRUE(require_non_empty_opt(opt_zone_ids), "ERROR: --zones not provided for symmetrical flow, or is empty", EINVAL);

      rgw_sync_symmetric_group *flow_group;

      group.data_flow.find_or_create_symmetrical(*opt_flow_id, &flow_group);

      for (auto& z : *opt_zone_ids) {
        flow_group->zones.insert(z);
      }
    } else { /* directional */
      CHECK_TRUE(require_non_empty_opt(opt_source_zone_id), "ERROR: --source-zone not provided for directional flow rule, or is empty", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opt_dest_zone_id), "ERROR: --dest-zone not provided for directional flow rule, or is empty", EINVAL);

      rgw_sync_directional_rule *flow_rule;

      group.data_flow.find_or_create_directional(*opt_source_zone_id, *opt_dest_zone_id, &flow_rule);
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter.get(), cout);
  }

  if (opt_cmd == OPT::SYNC_GROUP_FLOW_REMOVE) {
    CHECK_TRUE(require_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_flow_id), "ERROR: --flow-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_flow_type,
                           (symmetrical_flow_opt(*opt_flow_type) ||
                            directional_flow_opt(*opt_flow_type))),
                           "ERROR: --flow-type not specified or invalid (options: symmetrical, directional)", EINVAL);

    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    if (symmetrical_flow_opt(*opt_flow_type)) {
      group.data_flow.remove_symmetrical(*opt_flow_id, opt_zone_ids);
    } else { /* directional */
      CHECK_TRUE(require_non_empty_opt(opt_source_zone_id), "ERROR: --source-zone not provided for directional flow rule, or is empty", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opt_dest_zone_id), "ERROR: --dest-zone not provided for directional flow rule, or is empty", EINVAL);

      group.data_flow.remove_directional(*opt_source_zone_id, *opt_dest_zone_id);
    }
    
    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter.get(), cout);
  }

  if (opt_cmd == OPT::SYNC_GROUP_PIPE_CREATE ||
      opt_cmd == OPT::SYNC_GROUP_PIPE_MODIFY) {
    CHECK_TRUE(require_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_pipe_id), "ERROR: --pipe-id not specified", EINVAL);
    if (opt_cmd == OPT::SYNC_GROUP_PIPE_CREATE) {
      CHECK_TRUE(require_non_empty_opt(opt_source_zone_ids), "ERROR: --source-zones not provided or is empty; should be list of zones or '*'", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opt_dest_zone_ids), "ERROR: --dest-zones not provided or is empty; should be list of zones or '*'", EINVAL);
    }

    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    rgw_sync_bucket_pipes *pipe;

    if (opt_cmd == OPT::SYNC_GROUP_PIPE_CREATE) {
      group.find_pipe(*opt_pipe_id, true, &pipe);
    } else {
      if (!group.find_pipe(*opt_pipe_id, false, &pipe)) {
        cerr << "ERROR: could not find pipe '" << *opt_pipe_id << "'" << std::endl;
        return ENOENT;
      }
    }

    pipe->source.add_zones(*opt_source_zone_ids);
    pipe->source.set_bucket(opt_source_tenant,
                            opt_source_bucket_name,
                            opt_source_bucket_id);
    pipe->dest.add_zones(*opt_dest_zone_ids);
    pipe->dest.set_bucket(opt_dest_tenant,
                            opt_dest_bucket_name,
                            opt_dest_bucket_id);

    pipe->params.source.filter.set_prefix(opt_prefix, !!opt_prefix_rm);
    pipe->params.source.filter.set_tags(tags_add, tags_rm);
    if (opt_dest_owner) {
      pipe->params.dest.set_owner(*opt_dest_owner);
    }
    if (opt_storage_class) {
      pipe->params.dest.set_storage_class(*opt_storage_class);
    }
    if (opt_priority) {
      pipe->params.priority = *opt_priority;
    }
    if (opt_mode) {
      if (*opt_mode == "system") {
        pipe->params.mode = rgw_sync_pipe_params::MODE_SYSTEM;
      } else if (*opt_mode == "user") {
        pipe->params.mode = rgw_sync_pipe_params::MODE_USER;
      } else {
        cerr << "ERROR: bad mode value: should be one of the following: system, user" << std::endl;
        return EINVAL;
      }
    }

    if (!rgw::sal::User::empty(user)) {
      pipe->params.user = user->get_id();
    } else if (pipe->params.user.empty()) {
      auto owner = sync_policy_ctx.get_owner();
      if (owner) {
        pipe->params.user = *owner;
      }
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter.get(), cout);
  }

  if (opt_cmd == OPT::SYNC_GROUP_PIPE_REMOVE) {
    CHECK_TRUE(require_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_pipe_id), "ERROR: --pipe-id not specified", EINVAL);

    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    rgw_sync_bucket_pipes *pipe;

    if (!group.find_pipe(*opt_pipe_id, false, &pipe)) {
      cerr << "ERROR: could not find pipe '" << *opt_pipe_id << "'" << std::endl;
      return ENOENT;
    }

    if (opt_source_zone_ids) {
      pipe->source.remove_zones(*opt_source_zone_ids);
    }

    pipe->source.remove_bucket(opt_source_tenant,
                               opt_source_bucket_name,
                               opt_source_bucket_id);
    if (opt_dest_zone_ids) {
      pipe->dest.remove_zones(*opt_dest_zone_ids);
    }
    pipe->dest.remove_bucket(opt_dest_tenant,
                             opt_dest_bucket_name,
                             opt_dest_bucket_id);

    if (!(opt_source_zone_ids ||
          opt_source_tenant ||
          opt_source_bucket ||
          opt_source_bucket_id ||
          opt_dest_zone_ids ||
          opt_dest_tenant ||
          opt_dest_bucket ||
          opt_dest_bucket_id)) {
      group.remove_pipe(*opt_pipe_id);
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter.get(), cout);
  }

  if (opt_cmd == OPT::SYNC_POLICY_GET) {
    SyncPolicyContext sync_policy_ctx(zonegroup_id, zonegroup_name, opt_bucket);
    ret = sync_policy_ctx.init();
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    show_result(sync_policy, zone_formatter.get(), cout);
  }

  if (opt_cmd == OPT::BILOG_TRIM) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->log_trim(dpp(), bucket->get_info(), shard_id, start_marker, end_marker);
    if (ret < 0) {
      cerr << "ERROR: trim_bi_log_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BILOG_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<int, string> markers;
    ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->get_log_status(dpp(), bucket->get_info(), shard_id,
						    &markers, null_yield);
    if (ret < 0) {
      cerr << "ERROR: get_bi_log_status(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("entries");
    encode_json("markers", markers, formatter.get());
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BILOG_AUTOTRIM) {
    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    rgw::BucketTrimConfig config;
    configure_bucket_trim(store->ctx(), config);

    rgw::BucketTrimManager trim(static_cast<rgw::sal::RadosStore*>(store), config);
    ret = trim.init();
    if (ret < 0) {
      cerr << "trim manager init failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
    ret = crs.run(dpp(), trim.create_admin_bucket_trim_cr(&http));
    if (ret < 0) {
      cerr << "automated bilog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATALOG_LIST) {
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      std::cerr << "end-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      if (marker.empty()) {
	marker = start_marker;
      } else {
	std::cerr << "start-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    auto datalog_svc = static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados;
    RGWDataChangesLog::LogMarker log_marker;

    do {
      std::vector<rgw_data_change_log_entry> entries;
      if (specified_shard_id) {
        ret = datalog_svc->list_entries(dpp(), shard_id, max_entries - count,
					entries, marker,
					&marker, &truncated);
      } else {
        ret = datalog_svc->list_entries(dpp(), max_entries - count, entries,
					log_marker, &truncated);
      }
      if (ret < 0) {
        cerr << "ERROR: datalog_svc->list_entries(): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      count += entries.size();

      for (const auto& entry : entries) {
        if (!extra_info) {
          encode_json("entry", entry.entry, formatter.get());
        } else {
          encode_json("entry", entry, formatter.get());
        }
      }
      formatter.get()->flush(cout);
    } while (truncated && count < max_entries);

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::DATALOG_STATUS) {
    int i = (specified_shard_id ? shard_id : 0);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_data_log_num_shards; i++) {
      list<cls_log_entry> entries;

      RGWDataChangesLogInfo info;
      static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados->get_info(dpp(), i, &info);

      ::encode_json("info", info, formatter.get());

      if (specified_shard_id)
        break;
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::DATALOG_AUTOTRIM) {
    RGWCoroutinesManager crs(store->ctx(), store->get_cr_registry());
    RGWHTTPManager http(store->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf()->rgw_data_log_num_shards;
    std::vector<std::string> markers(num_shards);
    ret = crs.run(dpp(), create_admin_data_log_trim_cr(dpp(), static_cast<rgw::sal::RadosStore*>(store), &http, num_shards, markers));
    if (ret < 0) {
      cerr << "automated datalog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATALOG_TRIM) {
    if (!start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!start_marker.empty()) {
      std::cerr << "start-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      if (marker.empty()) {
	marker = end_marker;
      } else {
	std::cerr << "end-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    if (!specified_shard_id) {
      cerr << "ERROR: requires a --shard-id" << std::endl;
      return EINVAL;
    }

    if (marker.empty()) {
      cerr << "ERROR: requires a --marker" << std::endl;
      return EINVAL;
    }

    auto datalog = static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados;
    ret = datalog->trim_entries(dpp(), shard_id, marker);

    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: trim_entries(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATALOG_TYPE) {
    if (!opt_log_type) {
      std::cerr << "log-type not specified." << std::endl;
      return -EINVAL;
    }
    auto datalog = static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados;
    ret = datalog->change_format(dpp(), *opt_log_type, null_yield);
    if (ret < 0) {
      cerr << "ERROR: change_format(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATALOG_PRUNE) {
    auto datalog = static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados;
    std::optional<uint64_t> through;
    ret = datalog->trim_generations(dpp(), through);

    if (ret < 0) {
      cerr << "ERROR: trim_generations(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    if (through) {
      std::cout << "Pruned " << *through << " empty generations." << std::endl;
    } else {
      std::cout << "No empty generations." << std::endl;
    }
  }

  bool quota_op = (opt_cmd == OPT::QUOTA_SET || opt_cmd == OPT::QUOTA_ENABLE || opt_cmd == OPT::QUOTA_DISABLE);

  if (quota_op) {
    if (bucket_name.empty() && rgw::sal::User::empty(user)) {
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
    } else if (!rgw::sal::User::empty(user)) {
      if (quota_scope == "bucket") {
        return set_user_bucket_quota(opt_cmd, ruser, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else if (quota_scope == "user") {
        return set_user_quota(opt_cmd, ruser, user_op, max_size, max_objects, have_max_size, have_max_objects);
      } else {
        cerr << "ERROR: invalid quota scope specification. Please specify either --quota-scope=bucket, or --quota-scope=user" << std::endl;
        return EINVAL;
      }
    }
  }

  bool ratelimit_op_set = (opt_cmd == OPT::RATELIMIT_SET || opt_cmd == OPT::RATELIMIT_ENABLE || opt_cmd == OPT::RATELIMIT_DISABLE);
  bool ratelimit_op_get = opt_cmd == OPT::RATELIMIT_GET;
  if (ratelimit_op_set) {
    if (bucket_name.empty() && rgw::sal::User::empty(user)) {
      cerr << "ERROR: bucket name or uid is required for ratelimit operation" << std::endl;
      return EINVAL;
    }

    if (!bucket_name.empty()) {
      if (!ratelimit_scope.empty() && ratelimit_scope != "bucket") {
        cerr << "ERROR: invalid ratelimit scope specification. (bucket scope is not bucket but bucket has been specified)" << std::endl;
        return EINVAL;
      }
      return set_bucket_ratelimit(store, opt_cmd, tenant, bucket_name,
                           max_read_ops, max_write_ops,
                           max_read_bytes, max_write_bytes,
                           have_max_read_ops, have_max_write_ops,
                           have_max_read_bytes, have_max_write_bytes);
    } else if (!rgw::sal::User::empty(user)) {
      } if (ratelimit_scope == "user") {
        return set_user_ratelimit(opt_cmd, user, max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
      } else {
        cerr << "ERROR: invalid ratelimit scope specification. Please specify either --ratelimit-scope=bucket, or --ratelimit-scope=user" << std::endl;
        return EINVAL;
      }
  }

  if (ratelimit_op_get) {
    if (bucket_name.empty() && rgw::sal::User::empty(user)) {
      cerr << "ERROR: bucket name or uid is required for ratelimit operation" << std::endl;
      return EINVAL;
    }

    if (!bucket_name.empty()) {
      if (!ratelimit_scope.empty() && ratelimit_scope != "bucket") {
        cerr << "ERROR: invalid ratelimit scope specification. (bucket scope is not bucket but bucket has been specified)" << std::endl;
        return EINVAL;
      }
      return show_bucket_ratelimit(store, tenant, bucket_name, formatter.get());
    } else if (!rgw::sal::User::empty(user)) {
      } if (ratelimit_scope == "user") {
        return show_user_ratelimit(user, formatter.get());
      } else {
        cerr << "ERROR: invalid ratelimit scope specification. Please specify either --ratelimit-scope=bucket, or --ratelimit-scope=user" << std::endl;
        return EINVAL;
      }
  }

  if (opt_cmd == OPT::MFA_CREATE) {
    rados::cls::otp::otp_info_t config;

    if (rgw::sal::User::empty(user)) {
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
    string oid = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.get_mfa_oid(user->get_id());

    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(user->get_id()),
					     mtime, &objv_tracker,
					     null_yield, dpp(),
					     MDLOG_STATUS_WRITE,
					     [&] {
      return static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.create_mfa(dpp(), user->get_id(), config, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA creation failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    
    RGWUserInfo& user_info = user_op.get_user_info();
    user_info.mfa_ids.insert(totp_serial);
    user_op.set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = ruser.modify(dpp(), user_op, null_yield, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT::MFA_REMOVE) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    real_time mtime = real_clock::now();

    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(user->get_id()),
					     mtime, &objv_tracker,
					     null_yield, dpp(),
					     MDLOG_STATUS_WRITE,
					     [&] {
      return static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.remove_mfa(dpp(), user->get_id(), totp_serial, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA removal failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWUserInfo& user_info = user_op.get_user_info();
    user_info.mfa_ids.erase(totp_serial);
    user_op.set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = ruser.modify(dpp(), user_op, null_yield, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT::MFA_GET) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t result;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.get_mfa(dpp(), user->get_id(), totp_serial, &result, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("entry", result, formatter.get());
    formatter->close_section();
    formatter->flush(cout);
  }

 if (opt_cmd == OPT::MFA_LIST) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.list_mfa(dpp(), user->get_id(), &result, null_yield);
    if (ret < 0) {
      cerr << "MFA listing failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("result");
    encode_json("entries", result, formatter.get());
    formatter->close_section();
    formatter->flush(cout);
  }

 if (opt_cmd == OPT::MFA_CHECK) {
    if (rgw::sal::User::empty(user)) {
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
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.check_mfa(dpp(), user->get_id(), totp_serial, totp_pin.front(), null_yield);
    if (ret < 0) {
      cerr << "MFA check failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    cout << "ok" << std::endl;
  }

 if (opt_cmd == OPT::MFA_RESYNC) {
    if (rgw::sal::User::empty(user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (totp_pin.size() != 2) {
      cerr << "ERROR: missing two --totp-pin params (--totp-pin=<first> --totp-pin=<second>)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t config;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.get_mfa(dpp(), user->get_id(), totp_serial, &config, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    ceph::real_time now;

    ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.otp_get_current_time(dpp(), user->get_id(), &now, null_yield);
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

    ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(user->get_id()),
				         mtime, &objv_tracker,
				         null_yield, dpp(),
				         MDLOG_STATUS_WRITE,
				         [&] {
      return static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.create_mfa(dpp(), user->get_id(), config, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA update failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

 }

 if (opt_cmd == OPT::RESHARD_STALE_INSTANCES_LIST) {
   if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->can_reshard() && !yes_i_really_mean_it) {
     cerr << "Resharding disabled in a multisite env, stale instances unlikely from resharding" << std::endl;
     cerr << "These instances may not be safe to delete." << std::endl;
     cerr << "Use --yes-i-really-mean-it to force displaying these instances." << std::endl;
     return EINVAL;
   }

   ret = RGWBucketAdminOp::list_stale_instances(store, bucket_op, stream_flusher, dpp());
   if (ret < 0) {
     cerr << "ERROR: listing stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

 if (opt_cmd == OPT::RESHARD_STALE_INSTANCES_DELETE) {
   if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->can_reshard()) {
     cerr << "Resharding disabled in a multisite env. Stale instances are not safe to be deleted." << std::endl;
     return EINVAL;
   }

   ret = RGWBucketAdminOp::clear_stale_instances(store, bucket_op, stream_flusher, dpp());
   if (ret < 0) {
     cerr << "ERROR: deleting stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

  if (opt_cmd == OPT::PUBSUB_TOPICS_LIST) {

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    if (!bucket_name.empty()) {
      rgw_pubsub_bucket_topics result;
      int ret = init_bucket(user.get(), tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      auto b = ps.get_bucket(bucket->get_key());
      ret = b->get_topics(&result);
      if (ret < 0) {
        cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("result", result, formatter.get());
    } else {
      rgw_pubsub_topics result;
      int ret = ps.get_topics(&result);
      if (ret < 0) {
        cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("result", result, formatter.get());
    }
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_GET) {
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    rgw_pubsub_topic_subs topic;
    ret = ps.get_topic(topic_name, &topic);
    if (ret < 0) {
      cerr << "ERROR: could not get topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("topic", topic, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_RM) {
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    ret = ps.remove_topic(dpp(), topic_name, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not remove topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::PUBSUB_SUB_GET) {
    if (get_tier_type(static_cast<rgw::sal::RadosStore*>(store)) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    rgw_pubsub_sub_config sub_conf;

    auto sub = ps.get_sub(sub_name);
    ret = sub->get_conf(&sub_conf);
    if (ret < 0) {
      cerr << "ERROR: could not get subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("sub", sub_conf, formatter.get());
    formatter->flush(cout);
  }

 if (opt_cmd == OPT::PUBSUB_SUB_RM) {
    if (get_tier_type(static_cast<rgw::sal::RadosStore*>(store)) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    auto sub = ps.get_sub(sub_name);
    ret = sub->unsubscribe(dpp(), topic_name, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not get subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT::PUBSUB_SUB_PULL) {
    if (get_tier_type(static_cast<rgw::sal::RadosStore*>(store)) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    if (!max_entries_specified) {
      max_entries = RGWPubSub::Sub::DEFAULT_MAX_EVENTS;
    }
    auto sub = ps.get_sub_with_events(sub_name);
    ret = sub->list_events(dpp(), marker, max_entries);
    if (ret < 0) {
      cerr << "ERROR: could not list events: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("result", *sub, formatter.get());
    formatter->flush(cout);
 }

 if (opt_cmd == OPT::PUBSUB_EVENT_RM) {
    if (get_tier_type(static_cast<rgw::sal::RadosStore*>(store)) != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }
    if (event_id.empty()) {
      cerr << "ERROR: event id was not provided (via --event-id)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), tenant);

    auto sub = ps.get_sub_with_events(sub_name);
    ret = sub->remove_event(dpp(), event_id);
    if (ret < 0) {
      cerr << "ERROR: could not remove event: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::SCRIPT_PUT) {
    if (!str_script_ctx) {
      cerr << "ERROR: context was not provided (via --context)" << std::endl;
      return EINVAL;
    }
    if (infile.empty()) {
      cerr << "ERROR: infile was not provided (via --infile)" << std::endl;
      return EINVAL;
    }
    bufferlist bl;
    auto rc = read_input(infile, bl);
    if (rc < 0) {
      cerr << "ERROR: failed to read script: '" << infile << "'. error: " << rc << std::endl;
      return -rc;
    }
    const std::string script = bl.to_str();
    std::string err_msg;
    if (!rgw::lua::verify(script, err_msg)) {
      cerr << "ERROR: script: '" << infile << "' has error: " << std::endl << err_msg << std::endl;
      return EINVAL;
    }
    const rgw::lua::context script_ctx = rgw::lua::to_context(*str_script_ctx);
    if (script_ctx == rgw::lua::context::none) {
      cerr << "ERROR: invalid script context: " << *str_script_ctx << ". must be one of: preRequest, postRequest" <<  std::endl;
      return EINVAL;
    }
    rc = rgw::lua::write_script(dpp(), store, tenant, null_yield, script_ctx, script);
    if (rc < 0) {
      cerr << "ERROR: failed to put script. error: " << rc << std::endl;
      return -rc;
    }
  }

  if (opt_cmd == OPT::SCRIPT_GET) {
    if (!str_script_ctx) {
      cerr << "ERROR: context was not provided (via --context)" << std::endl;
      return EINVAL;
    }
    const rgw::lua::context script_ctx = rgw::lua::to_context(*str_script_ctx);
    if (script_ctx == rgw::lua::context::none) {
      cerr << "ERROR: invalid script context: " << *str_script_ctx << ". must be one of: preRequest, postRequest" <<  std::endl;
      return EINVAL;
    }
    std::string script;
    const auto rc = rgw::lua::read_script(dpp(), store, tenant, null_yield, script_ctx, script);
    if (rc == -ENOENT) {
      std::cout << "no script exists for context: " << *str_script_ctx << 
        (tenant.empty() ? "" : (" in tenant: " + tenant)) << std::endl;
    } else if (rc < 0) {
      cerr << "ERROR: failed to read script. error: " << rc << std::endl;
      return -rc;
    } else {
      std::cout << script << std::endl;
    }
  }
  
  if (opt_cmd == OPT::SCRIPT_RM) {
    if (!str_script_ctx) {
      cerr << "ERROR: context was not provided (via --context)" << std::endl;
      return EINVAL;
    }
    const rgw::lua::context script_ctx = rgw::lua::to_context(*str_script_ctx);
    if (script_ctx == rgw::lua::context::none) {
      cerr << "ERROR: invalid script context: " << *str_script_ctx << ". must be one of: preRequest, postRequest" <<  std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::delete_script(dpp(), store, tenant, null_yield, script_ctx);
    if (rc < 0) {
      cerr << "ERROR: failed to remove script. error: " << rc << std::endl;
      return -rc;
    }
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_ADD) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    if (!script_package) {
      cerr << "ERROR: lua package name was not provided (via --package)" << std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::add_package(dpp(), static_cast<rgw::sal::RadosStore*>(store), null_yield, *script_package, bool(allow_compilation));
    if (rc < 0) {
      cerr << "ERROR: failed to add lua package: " << script_package << " .error: " << rc << std::endl;
      return -rc;
    }
#else
    cerr << "ERROR: adding lua packages is not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_RM) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    if (!script_package) {
      cerr << "ERROR: lua package name was not provided (via --package)" << std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::remove_package(dpp(), static_cast<rgw::sal::RadosStore*>(store), null_yield, *script_package);
    if (rc == -ENOENT) {
      cerr << "WARNING: package " << script_package << " did not exists or already removed" << std::endl;
      return 0;
    }
    if (rc < 0) {
      cerr << "ERROR: failed to remove lua package: " << script_package << " .error: " << rc << std::endl;
      return -rc;
    }
#else
    cerr << "ERROR: removing lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_LIST) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    rgw::lua::packages_t packages;
    const auto rc = rgw::lua::list_packages(dpp(), static_cast<rgw::sal::RadosStore*>(store), null_yield, packages);
    if (rc == -ENOENT) {
      std::cout << "no lua packages in allowlist" << std::endl;
    } else if (rc < 0) {
      cerr << "ERROR: failed to read lua packages allowlist. error: " << rc << std::endl;
      return rc;
    } else {
      for (const auto& package : packages) {
          std::cout << package << std::endl;
      }
    }
#else
    cerr << "ERROR: listing lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  return 0;
}

