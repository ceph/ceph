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

#include <fmt/format.h>

#include "auth/Crypto.h"
#include "compressor/Compressor.h"

#include "common/async/context_pool.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/fault_injector.h"

#include "include/util.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
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
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_data_access.h"

#include "services/svc_sync_modules.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta_be_otp.h"
#include "services/svc_user.h"
#include "services/svc_zone.h"

#include "driver/rados/rgw_bucket.h"
#include "driver/rados/rgw_sal_rados.h"

#define dout_context g_ceph_context

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

using namespace std;

static rgw::sal::Driver* driver = NULL;
static constexpr auto dout_subsys = ceph_subsys_rgw;

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

static inline int posix_errortrans(int r)
{
  switch(r) {
  case ERR_NO_SUCH_BUCKET:
    r = ENOENT;
    break;
  default:
    break;
  }
  return r;
}


static const std::string LUA_CONTEXT_LIST("prerequest, postrequest, background, getdata, putdata");

void usage()
{
  cout << "usage: radosgw-admin <cmd> [options...]" << std::endl;
  cout << "commands:\n";
  cout << "  user create                      create a new user\n" ;
  cout << "  user modify                      modify user\n";
  cout << "  user info                        get user info\n";
  cout << "  user rename                      rename user\n";
  cout << "  user rm                          remove user\n";
  cout << "  user suspend                     suspend a user\n";
  cout << "  user enable                      re-enable user after suspension\n";
  cout << "  user check                       check user info\n";
  cout << "  user stats                       show user stats as accounted by quota subsystem\n";
  cout << "  user list                        list users\n";
  cout << "  caps add                         add user capabilities\n";
  cout << "  caps rm                          remove user capabilities\n";
  cout << "  subuser create                   create a new subuser\n" ;
  cout << "  subuser modify                   modify subuser\n";
  cout << "  subuser rm                       remove subuser\n";
  cout << "  key create                       create access key\n";
  cout << "  key rm                           remove access key\n";
  cout << "  bucket list                      list buckets (specify --allow-unordered for faster, unsorted listing)\n";
  cout << "  bucket limit check               show bucket sharding stats\n";
  cout << "  bucket link                      link bucket to specified user\n";
  cout << "  bucket unlink                    unlink bucket from specified user\n";
  cout << "  bucket stats                     returns bucket statistics\n";
  cout << "  bucket rm                        remove bucket\n";
  cout << "  bucket check                     check bucket index by verifying size and object count stats\n";
  cout << "  bucket check olh                 check for olh index entries and objects that are pending removal\n";
  cout << "  bucket check unlinked            check for object versions that are not visible in a bucket listing \n";
  cout << "  bucket chown                     link bucket to specified user and update its object ACLs\n";
  cout << "  bucket reshard                   reshard bucket\n";
  cout << "  bucket rewrite                   rewrite all objects in the specified bucket\n";
  cout << "  bucket sync checkpoint           poll a bucket's sync status until it catches up to its remote\n";
  cout << "  bucket sync disable              disable bucket sync\n";
  cout << "  bucket sync enable               enable bucket sync\n";
  cout << "  bucket radoslist                 list rados objects backing bucket's objects\n";
  cout << "  bi get                           retrieve bucket index object entries\n";
  cout << "  bi put                           store bucket index object entries\n";
  cout << "  bi list                          list raw bucket index entries\n";
  cout << "  bi purge                         purge bucket index entries\n";
  cout << "  object rm                        remove object\n";
  cout << "  object put                       put object\n";
  cout << "  object stat                      stat an object for its metadata\n";
  cout << "  object unlink                    unlink object from bucket index\n";
  cout << "  object rewrite                   rewrite the specified object\n";
  cout << "  object reindex                   reindex the object(s) indicated by --bucket and either --object or --objects-file\n";
  cout << "  objects expire                   run expired objects cleanup\n";
  cout << "  objects expire-stale list        list stale expired objects (caused by reshard)\n";
  cout << "  objects expire-stale rm          remove stale expired objects\n";
  cout << "  period rm                        remove a period\n";
  cout << "  period get                       get period info\n";
  cout << "  period get-current               get current period info\n";
  cout << "  period pull                      pull a period\n";
  cout << "  period push                      push a period\n";
  cout << "  period list                      list all periods\n";
  cout << "  period update                    update the staging period\n";
  cout << "  period commit                    commit the staging period\n";
  cout << "  quota set                        set quota params\n";
  cout << "  quota enable                     enable quota\n";
  cout << "  quota disable                    disable quota\n";
  cout << "  ratelimit get                    get ratelimit params\n";
  cout << "  ratelimit set                    set ratelimit params\n";
  cout << "  ratelimit enable                 enable ratelimit\n";
  cout << "  ratelimit disable                disable ratelimit\n";
  cout << "  global quota get                 view global quota params\n";
  cout << "  global quota set                 set global quota params\n";
  cout << "  global quota enable              enable a global quota\n";
  cout << "  global quota disable             disable a global quota\n";
  cout << "  global ratelimit get             view global ratelimit params\n";
  cout << "  global ratelimit set             set global ratelimit params\n";
  cout << "  global ratelimit enable          enable a ratelimit quota\n";
  cout << "  global ratelimit disable         disable a ratelimit quota\n";
  cout << "  realm create                     create a new realm\n";
  cout << "  realm rm                         remove a realm\n";
  cout << "  realm get                        show realm info\n";
  cout << "  realm get-default                get default realm name\n";
  cout << "  realm list                       list realms\n";
  cout << "  realm list-periods               list all realm periods\n";
  cout << "  realm rename                     rename a realm\n";
  cout << "  realm set                        set realm info (requires infile)\n";
  cout << "  realm default                    set realm as default\n";
  cout << "  realm pull                       pull a realm and its current period\n";
  cout << "  zonegroup add                    add a zone to a zonegroup\n";
  cout << "  zonegroup create                 create a new zone group info\n";
  cout << "  zonegroup default                set default zone group\n";
  cout << "  zonegroup delete                 delete a zone group info\n";
  cout << "  zonegroup get                    show zone group info\n";
  cout << "  zonegroup modify                 modify an existing zonegroup\n";
  cout << "  zonegroup set                    set zone group info (requires infile)\n";
  cout << "  zonegroup rm                     remove a zone from a zonegroup\n";
  cout << "  zonegroup rename                 rename a zone group\n";
  cout << "  zonegroup list                   list all zone groups set on this cluster\n";
  cout << "  zonegroup placement list         list zonegroup's placement targets\n";
  cout << "  zonegroup placement get          get a placement target of a specific zonegroup\n";
  cout << "  zonegroup placement add          add a placement target id to a zonegroup\n";
  cout << "  zonegroup placement modify       modify a placement target of a specific zonegroup\n";
  cout << "  zonegroup placement rm           remove a placement target from a zonegroup\n";
  cout << "  zonegroup placement default      set a zonegroup's default placement target\n";
  cout << "  zone create                      create a new zone\n";
  cout << "  zone rm                          remove a zone\n";
  cout << "  zone get                         show zone cluster params\n";
  cout << "  zone modify                      modify an existing zone\n";
  cout << "  zone set                         set zone cluster params (requires infile)\n";
  cout << "  zone list                        list all zones set on this cluster\n";
  cout << "  zone rename                      rename a zone\n";
  cout << "  zone placement list              list zone's placement targets\n";
  cout << "  zone placement get               get a zone placement target\n";
  cout << "  zone placement add               add a zone placement target\n";
  cout << "  zone placement modify            modify a zone placement target\n";
  cout << "  zone placement rm                remove a zone placement target\n";
  cout << "  metadata sync status             get metadata sync status\n";
  cout << "  metadata sync init               init metadata sync\n";
  cout << "  metadata sync run                run metadata sync\n";
  cout << "  data sync status                 get data sync status of the specified source zone\n";
  cout << "  data sync init                   init data sync for the specified source zone\n";
  cout << "  data sync run                    run data sync for the specified source zone\n";
  cout << "  pool add                         add an existing pool for data placement\n";
  cout << "  pool rm                          remove an existing pool from data placement set\n";
  cout << "  pools list                       list placement active set\n";
  cout << "  policy                           read bucket/object policy\n";
  cout << "  log list                         list log objects\n";
  cout << "  log show                         dump a log from specific object or (bucket + date + bucket-id)\n";
  cout << "                                   (NOTE: required to specify formatting of date to \"YYYY-MM-DD-hh\")\n";
  cout << "  log rm                           remove log object\n";
  cout << "  usage show                       show usage (by user, by bucket, date range)\n";
  cout << "  usage trim                       trim usage (by user, by bucket, date range)\n";
  cout << "  usage clear                      reset all the usage stats for the cluster\n";
  cout << "  gc list                          dump expired garbage collection objects (specify\n";
  cout << "                                   --include-all to list all entries, including unexpired)\n";
  cout << "  gc process                       manually process garbage (specify\n";
  cout << "                                   --include-all to process all entries, including unexpired)\n";
  cout << "  lc list                          list all bucket lifecycle progress\n";
  cout << "  lc get                           get a lifecycle bucket configuration\n";
  cout << "  lc process                       manually process lifecycle\n";
  cout << "  lc reshard fix                   fix LC for a resharded bucket\n";
  cout << "  metadata get                     get metadata info\n";
  cout << "  metadata put                     put metadata info\n";
  cout << "  metadata rm                      remove metadata info\n";
  cout << "  metadata list                    list metadata info\n";
  cout << "  mdlog list                       list metadata log\n";
  cout << "  mdlog autotrim                   auto trim metadata log\n";
  cout << "  mdlog trim                       trim metadata log (use marker)\n";
  cout << "  mdlog status                     read metadata log status\n";
  cout << "  bilog list                       list bucket index log\n";
  cout << "  bilog trim                       trim bucket index log (use start-marker, end-marker)\n";
  cout << "  bilog status                     read bucket index log status\n";
  cout << "  bilog autotrim                   auto trim bucket index log\n";
  cout << "  datalog list                     list data log\n";
  cout << "  datalog trim                     trim data log\n";
  cout << "  datalog status                   read data log status\n";
  cout << "  datalog type                     change datalog type to --log_type={fifo,omap}\n";
  cout << "  orphans find                     deprecated -- init and run search for leaked rados objects (use job-id, pool)\n";
  cout << "  orphans finish                   deprecated -- clean up search for leaked rados objects\n";
  cout << "  orphans list-jobs                deprecated -- list the current job-ids for orphans search\n";
  cout << "    * the three 'orphans' sub-commands are now deprecated; consider using the `rgw-orphan-list` tool\n";
  cout << "  role create                      create a AWS role for use with STS\n";
  cout << "  role delete                      remove a role\n";
  cout << "  role get                         get a role\n";
  cout << "  role list                        list roles with specified path prefix\n";
  cout << "  role-trust-policy modify         modify the assume role policy of an existing role\n";
  cout << "  role-policy put                  add/update permission policy to role\n";
  cout << "  role-policy list                 list policies attached to a role\n";
  cout << "  role-policy get                  get the specified inline policy document embedded with the given role\n";
  cout << "  role-policy delete               remove policy attached to a role\n";
  cout << "  role update                      update max_session_duration of a role\n";
  cout << "  reshard add                      schedule a resharding of a bucket\n";
  cout << "  reshard list                     list all bucket resharding or scheduled to be resharded\n";
  cout << "  reshard status                   read bucket resharding status\n";
  cout << "  reshard process                  process of scheduled reshard jobs\n";
  cout << "  reshard cancel                   cancel resharding a bucket\n";
  cout << "  reshard stale-instances list     list stale-instances from bucket resharding\n";
  cout << "  reshard stale-instances delete   cleanup stale-instances from bucket resharding\n";
  cout << "  sync error list                  list sync error\n";
  cout << "  sync error trim                  trim sync error\n";
  cout << "  mfa create                       create a new MFA TOTP token\n";
  cout << "  mfa list                         list MFA TOTP tokens\n";
  cout << "  mfa get                          show MFA TOTP token\n";
  cout << "  mfa remove                       delete MFA TOTP token\n";
  cout << "  mfa check                        check MFA TOTP token\n";
  cout << "  mfa resync                       re-sync MFA TOTP token\n";
  cout << "  topic list                       list bucket notifications topics\n";
  cout << "  topic get                        get a bucket notifications topic\n";
  cout << "  topic rm                         remove a bucket notifications topic\n";
  cout << "  topic stats                      get a bucket notifications persistent topic stats (i.e. reservations, entries & size)\n";
  cout << "  script put                       upload a Lua script to a context\n";
  cout << "  script get                       get the Lua script of a context\n";
  cout << "  script rm                        remove the Lua scripts of a context\n";
  cout << "  script-package add               add a Lua package to the scripts allowlist\n";
  cout << "  script-package rm                remove a Lua package from the scripts allowlist\n";
  cout << "  script-package list              get the Lua packages allowlist\n";
  cout << "  script-package reload            install/remove Lua packages according to allowlist\n";
  cout << "  notification list                list bucket notifications configuration\n";
  cout << "  notification get                 get a bucket notifications configuration\n";
  cout << "  notification rm                  remove a bucket notifications configuration\n";
  cout << "options:\n";
  cout << "   --tenant=<tenant>                 tenant name\n";
  cout << "   --user_ns=<namespace>             namespace of user (oidc in case of users authenticated with oidc provider)\n";
  cout << "   --uid=<id>                        user id\n";
  cout << "   --new-uid=<id>                    new user id\n";
  cout << "   --subuser=<name>                  subuser name\n";
  cout << "   --access-key=<key>                S3 access key\n";
  cout << "   --email=<email>                   user's email address\n";
  cout << "   --secret/--secret-key=<key>       specify secret key\n";
  cout << "   --gen-access-key                  generate random access key (for S3)\n";
  cout << "   --gen-secret                      generate random secret key\n";
  cout << "   --key-type=<type>                 key type, options are: swift, s3\n";
  cout << "   --key-active=<bool>               activate or deactivate a key\n";
  cout << "   --temp-url-key[-2]=<key>          temp url key\n";
  cout << "   --access=<access>                 Set access permissions for sub-user, should be one\n";
  cout << "                                     of read, write, readwrite, full\n";
  cout << "   --display-name=<name>             user's display name\n";
  cout << "   --max-buckets                     max number of buckets for a user\n";
  cout << "   --admin                           set the admin flag on the user\n";
  cout << "   --system                          set the system flag on the user\n";
  cout << "   --op-mask                         set the op mask on the user\n";
  cout << "   --bucket=<bucket>                 Specify the bucket name. Also used by the quota command.\n";
  cout << "   --pool=<pool>                     Specify the pool name. Also used to scan for leaked rados objects.\n";
  cout << "   --object=<object>                 object name\n";
  cout << "   --objects-file=<file>             file containing a list of object names to process\n";
  cout << "   --object-version=<version>        object version\n";
  cout << "   --date=<date>                     date in the format yyyy-mm-dd\n";
  cout << "   --start-date=<date>               start date in the format yyyy-mm-dd\n";
  cout << "   --end-date=<date>                 end date in the format yyyy-mm-dd\n";
  cout << "   --bucket-id=<bucket-id>           bucket id\n";
  cout << "   --bucket-new-name=<bucket>        for bucket link: optional new name\n";
  cout << "   --shard-id=<shard-id>             optional for:\n";
  cout << "                                       mdlog list\n";
  cout << "                                       data sync status\n";
  cout << "                                     required for:\n";
  cout << "                                       mdlog trim\n";
  cout << "   --gen=<gen-id>                    optional for:\n";
  cout << "                                       bilog list\n";
  cout << "                                       bilog trim\n";
  cout << "                                       bilog status\n";
  cout << "   --max-entries=<entries>           max entries for listing operations\n";
  cout << "   --metadata-key=<key>              key to retrieve metadata from with metadata get\n";
  cout << "   --remote=<remote>                 zone or zonegroup id of remote gateway\n";
  cout << "   --period=<id>                     period id\n";
  cout << "   --url=<url>                       url for pushing/pulling period/realm\n";
  cout << "   --epoch=<number>                  period epoch\n";
  cout << "   --commit                          commit the period during 'period update'\n";
  cout << "   --staging                         get staging period info\n";
  cout << "   --master                          set as master\n";
  cout << "   --master-zone=<id>                master zone id\n";
  cout << "   --rgw-realm=<name>                realm name\n";
  cout << "   --realm-id=<id>                   realm id\n";
  cout << "   --realm-new-name=<name>           realm new name\n";
  cout << "   --rgw-zonegroup=<name>            zonegroup name\n";
  cout << "   --zonegroup-id=<id>               zonegroup id\n";
  cout << "   --zonegroup-new-name=<name>       zonegroup new name\n";
  cout << "   --rgw-zone=<name>                 name of zone in which radosgw is running\n";
  cout << "   --zone-id=<id>                    zone id\n";
  cout << "   --zone-new-name=<name>            zone new name\n";
  cout << "   --source-zone                     specify the source zone (for data sync)\n";
  cout << "   --default                         set entity (realm, zonegroup, zone) as default\n";
  cout << "   --read-only                       set zone as read-only (when adding to zonegroup)\n";
  cout << "   --redirect-zone                   specify zone id to redirect when response is 404 (not found)\n";
  cout << "   --placement-id                    placement id for zonegroup placement commands\n";
  cout << "   --storage-class                   storage class for zonegroup placement commands\n";
  cout << "   --tags=<list>                     list of tags for zonegroup placement add and modify commands\n";
  cout << "   --tags-add=<list>                 list of tags to add for zonegroup placement modify command\n";
  cout << "   --tags-rm=<list>                  list of tags to remove for zonegroup placement modify command\n";
  cout << "   --endpoints=<list>                zone endpoints\n";
  cout << "   --index-pool=<pool>               placement target index pool\n";
  cout << "   --data-pool=<pool>                placement target data pool\n";
  cout << "   --data-extra-pool=<pool>          placement target data extra (non-ec) pool\n";
  cout << "   --placement-index-type=<type>     placement target index type (normal, indexless, or #id)\n";
  cout << "   --placement-inline-data=<true>    set whether the placement target is configured to store a data\n";
  cout << "                                     chunk inline in head objects\n";
  cout << "   --compression=<type>              placement target compression type (plugin name or empty/none)\n";
  cout << "   --tier-type=<type>                zone tier type\n";
  cout << "   --tier-config=<k>=<v>[,...]       set zone tier config keys, values\n";
  cout << "   --tier-config-rm=<k>[,...]        unset zone tier config keys\n";
  cout << "   --sync-from-all[=false]           set/reset whether zone syncs from all zonegroup peers\n";
  cout << "   --sync-from=[zone-name][,...]     set list of zones to sync from\n";
  cout << "   --sync-from-rm=[zone-name][,...]  remove zones from list of zones to sync from\n";
  cout << "   --bucket-index-max-shards         override a zone/zonegroup's default bucket index shard count\n";
  cout << "   --fix                             besides checking bucket index, will also fix it\n";
  cout << "   --check-objects                   bucket check: rebuilds bucket index according to actual objects state\n";
  cout << "   --format=<format>                 specify output format for certain operations: xml, json\n";
  cout << "   --purge-data                      when specified, user removal will also purge all the\n";
  cout << "                                     user data\n";
  cout << "   --purge-keys                      when specified, subuser removal will also purge all the\n";
  cout << "                                     subuser keys\n";
  cout << "   --purge-objects                   remove a bucket's objects before deleting it\n";
  cout << "                                     (NOTE: required to delete a non-empty bucket)\n";
  cout << "   --sync-stats                      option to 'user stats', update user stats with current\n";
  cout << "                                     stats reported by user's buckets indexes\n";
  cout << "   --reset-stats                     option to 'user stats', reset stats in accordance with user buckets\n";
  cout << "   --show-config                     show configuration\n";
  cout << "   --show-log-entries=<flag>         enable/disable dump of log entries on log show\n";
  cout << "   --show-log-sum=<flag>             enable/disable dump of log summation on log show\n";
  cout << "   --skip-zero-entries               log show only dumps entries that don't have zero value\n";
  cout << "                                     in one of the numeric field\n";
  cout << "   --infile=<file>                   file to read in when setting data\n";
  cout << "   --categories=<list>               comma separated list of categories, used in usage show\n";
  cout << "   --caps=<caps>                     list of caps (e.g., \"usage=read, write; user=read\")\n";
  cout << "   --op-mask=<op-mask>               permission of user's operations (e.g., \"read, write, delete, *\")\n";
  cout << "   --yes-i-really-mean-it            required for certain operations\n";
  cout << "   --warnings-only                   when specified with bucket limit check, list\n";
  cout << "                                     only buckets nearing or over the current max\n";
  cout << "                                     objects per shard value\n";
  cout << "   --bypass-gc                       when specified with bucket deletion, triggers\n";
  cout << "                                     object deletions by not involving GC\n";
  cout << "   --inconsistent-index              when specified with bucket deletion and bypass-gc set to true,\n";
  cout << "                                     ignores bucket index consistency\n";
  cout << "   --min-rewrite-size                min object size for bucket rewrite (default 4M)\n";
  cout << "   --max-rewrite-size                max object size for bucket rewrite (default ULLONG_MAX)\n";
  cout << "   --min-rewrite-stripe-size         min stripe size for object rewrite (default 0)\n";
  cout << "   --trim-delay-ms                   time interval in msec to limit the frequency of sync error log entries trimming operations,\n";
  cout << "                                     the trimming process will sleep the specified msec for every 1000 entries trimmed\n";
  cout << "   --max-concurrent-ios              maximum concurrent ios for bucket operations (default: 32)\n";
  cout << "   --enable-feature                  enable a zone/zonegroup feature\n";
  cout << "   --disable-feature                 disable a zone/zonegroup feature\n";
  cout << "\n";
  cout << "<date> := \"YYYY-MM-DD[ hh:mm:ss]\"\n";
  cout << "\nQuota options:\n";
  cout << "   --max-objects                 specify max objects (negative value to disable)\n";
  cout << "   --max-size                    specify max size (in B/K/M/G/T, negative value to disable)\n";
  cout << "   --quota-scope                 scope of quota (bucket, user)\n";
  cout << "\nRate limiting options:\n";
  cout << "   --max-read-ops                specify max requests per minute for READ ops per RGW (GET and HEAD request methods), 0 means unlimited\n";
  cout << "   --max-read-bytes              specify max bytes per minute for READ ops per RGW (GET and HEAD request methods), 0 means unlimited\n";
  cout << "   --max-write-ops               specify max requests per minute for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited\n";
  cout << "   --max-write-bytes             specify max bytes per minute for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited\n";
  cout << "   --ratelimit-scope             scope of rate limiting: bucket, user, anonymous\n";
  cout << "                                 anonymous can be configured only with global rate limit\n";
  cout << "\nOrphans search options:\n";
  cout << "   --num-shards                  num of shards to use for keeping the temporary scan info\n";
  cout << "   --orphan-stale-secs           num of seconds to wait before declaring an object to be an orphan (default: 86400)\n";
  cout << "   --job-id                      set the job id (for orphans find)\n";
  cout << "   --detail                      detailed mode, log and stat head objects as well\n";
  cout << "\nOrphans list-jobs options:\n";
  cout << "   --extra-info                  provide extra info in job list\n";
  cout << "\nRole options:\n";
  cout << "   --role-name                   name of the role to create\n";
  cout << "   --path                        path to the role\n";
  cout << "   --assume-role-policy-doc      the trust relationship policy document that grants an entity permission to assume the role\n";
  cout << "   --policy-name                 name of the policy document\n";
  cout << "   --policy-doc                  permission policy document\n";
  cout << "   --path-prefix                 path prefix for filtering roles\n";
  cout << "\nMFA options:\n";
  cout << "   --totp-serial                 a string that represents the ID of a TOTP token\n";
  cout << "   --totp-seed                   the secret seed that is used to calculate the TOTP\n";
  cout << "   --totp-seconds                the time resolution that is being used for TOTP generation\n";
  cout << "   --totp-window                 the number of TOTP tokens that are checked before and after the current token when validating token\n";
  cout << "   --totp-pin                    the valid value of a TOTP token at a certain time\n";
  cout << "\nBucket notifications options:\n";
  cout << "   --topic                       bucket notifications topic name\n";
  cout << "   --notification-id             bucket notifications id\n";
  cout << "\nScript options:\n";
  cout << "   --context                     context in which the script runs. one of: "+LUA_CONTEXT_LIST+"\n";
  cout << "   --package                     name of the Lua package that should be added/removed to/from the allowlist\n";
  cout << "   --allow-compilation           package is allowed to compile C code as part of its installation\n";
  cout << "\nBucket check olh/unlinked options:\n";
  cout << "   --min-age-hours               minimum age of unlinked objects to consider for bucket check unlinked (default: 1)\n";
  cout << "   --dump-keys                   when specified, all keys identified as problematic are printed to stdout\n";
  cout << "   --hide-progress               when specified, per-shard progress details are not printed to stderr\n";
  cout << "\nradoslist options:\n";
  cout << "   --rgw-obj-fs                  the field separator that will separate the rados object name from the rgw object name;\n";
  cout << "                                 additionally rados objects for incomplete multipart uploads will not be output\n";
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
  BUCKET_LAYOUT,
  BUCKET_STATS,
  BUCKET_CHECK,
  BUCKET_CHECK_OLH,
  BUCKET_CHECK_UNLINKED,
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
  BUCKET_SHARD_OBJECTS,
  BUCKET_OBJECT_SHARD,
  BUCKET_RESYNC_ENCRYPTED_MULTIPART,
  POLICY,
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
  OBJECT_MANIFEST,
  OBJECT_REWRITE,
  OBJECT_REINDEX,
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
  ROLE_TRUST_POLICY_MODIFY,
  ROLE_LIST,
  ROLE_POLICY_PUT,
  ROLE_POLICY_LIST,
  ROLE_POLICY_GET,
  ROLE_POLICY_DELETE,
  ROLE_UPDATE,
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
  PUBSUB_TOPIC_LIST,
  PUBSUB_TOPIC_GET,
  PUBSUB_TOPIC_RM,
  PUBSUB_NOTIFICATION_LIST,
  PUBSUB_NOTIFICATION_GET,
  PUBSUB_NOTIFICATION_RM,
  PUBSUB_TOPIC_STATS,
  SCRIPT_PUT,
  SCRIPT_GET,
  SCRIPT_RM,
  SCRIPT_PACKAGE_ADD,
  SCRIPT_PACKAGE_RM,
  SCRIPT_PACKAGE_LIST,
  SCRIPT_PACKAGE_RELOAD
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
  { "bucket layout", OPT::BUCKET_LAYOUT },
  { "bucket stats", OPT::BUCKET_STATS },
  { "bucket check", OPT::BUCKET_CHECK },
  { "bucket check olh", OPT::BUCKET_CHECK_OLH },
  { "bucket check unlinked", OPT::BUCKET_CHECK_UNLINKED },
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
  { "bucket shard objects", OPT::BUCKET_SHARD_OBJECTS },
  { "bucket shard object", OPT::BUCKET_SHARD_OBJECTS },
  { "bucket object shard", OPT::BUCKET_OBJECT_SHARD },
  { "bucket resync encrypted multipart", OPT::BUCKET_RESYNC_ENCRYPTED_MULTIPART },
  { "policy", OPT::POLICY },
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
  { "object manifest", OPT::OBJECT_MANIFEST },
  { "object rewrite", OPT::OBJECT_REWRITE },
  { "object reindex", OPT::OBJECT_REINDEX },
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
  { "role-trust-policy modify", OPT::ROLE_TRUST_POLICY_MODIFY },
  { "role list", OPT::ROLE_LIST },
  { "role policy put", OPT::ROLE_POLICY_PUT },
  { "role-policy put", OPT::ROLE_POLICY_PUT },
  { "role policy list", OPT::ROLE_POLICY_LIST },
  { "role-policy list", OPT::ROLE_POLICY_LIST },
  { "role policy get", OPT::ROLE_POLICY_GET },
  { "role-policy get", OPT::ROLE_POLICY_GET },
  { "role policy delete", OPT::ROLE_POLICY_DELETE },
  { "role-policy delete", OPT::ROLE_POLICY_DELETE },
  { "role update", OPT::ROLE_UPDATE },
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
  { "topic list", OPT::PUBSUB_TOPIC_LIST },
  { "topic get", OPT::PUBSUB_TOPIC_GET },
  { "topic rm", OPT::PUBSUB_TOPIC_RM },
  { "notification list", OPT::PUBSUB_NOTIFICATION_LIST },
  { "notification get", OPT::PUBSUB_NOTIFICATION_GET },
  { "notification rm", OPT::PUBSUB_NOTIFICATION_RM },
  { "topic stats", OPT::PUBSUB_TOPIC_STATS },
  { "script put", OPT::SCRIPT_PUT },
  { "script get", OPT::SCRIPT_GET },
  { "script rm", OPT::SCRIPT_RM },
  { "script-package add", OPT::SCRIPT_PACKAGE_ADD },
  { "script-package rm", OPT::SCRIPT_PACKAGE_RM },
  { "script-package list", OPT::SCRIPT_PACKAGE_LIST },
  { "script-package reload", OPT::SCRIPT_PACKAGE_RELOAD },
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
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
}

class StoreDestructor {
  rgw::sal::Driver* driver;
public:
  explicit StoreDestructor(rgw::sal::Driver* _s) : driver(_s) {}
  ~StoreDestructor() {
    DriverManager::close_storage(driver);
    rgw_http_client_cleanup();
  }
};

static int init_bucket(const rgw_bucket& b,
                       std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  return driver->load_bucket(dpp(), b, bucket, null_yield);
}

static int init_bucket(const string& tenant_name,
		       const string& bucket_name,
		       const string& bucket_id,
                       std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  rgw_bucket b{tenant_name, bucket_name, bucket_id};
  return init_bucket(b, bucket);
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

int set_bucket_quota(rgw::sal::Driver* driver, OPT opt_cmd,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_size, int64_t max_objects,
                     bool have_max_size, bool have_max_objects)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = driver->load_bucket(dpp(), rgw_bucket(tenant_name, bucket_name),
                              &bucket, null_yield);
  if (r < 0) {
    cerr << "could not get bucket info for bucket=" << bucket_name << ": " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  set_quota_info(bucket->get_info().quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  r = bucket->put_info(dpp(), false, real_time(), null_yield);
  if (r < 0) {
    cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  return 0;
}

int set_bucket_ratelimit(rgw::sal::Driver* driver, OPT opt_cmd,
                     const string& tenant_name, const string& bucket_name,
                     int64_t max_read_ops, int64_t max_write_ops,
                     int64_t max_read_bytes, int64_t max_write_bytes,
                     bool have_max_read_ops, bool have_max_write_ops,
                     bool have_max_read_bytes, bool have_max_write_bytes)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = driver->load_bucket(dpp(), rgw_bucket(tenant_name, bucket_name),
                              &bucket, null_yield);
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

int show_bucket_ratelimit(rgw::sal::Driver* driver, const string& tenant_name,
                          const string& bucket_name, Formatter *formatter)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int r = driver->load_bucket(dpp(), rgw_bucket(tenant_name, bucket_name),
                              &bucket, null_yield);
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

  set_quota_info(user_info.quota.bucket_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_bucket_quota(user_info.quota.bucket_quota);

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

  set_quota_info(user_info.quota.user_quota, opt_cmd, max_size, max_objects, have_max_size, have_max_objects);

  op_state.set_user_quota(user_info.quota.user_quota);

  string err;
  int r = user.modify(dpp(), op_state, null_yield, &err);
  if (r < 0) {
    cerr << "ERROR: failed updating user info: " << cpp_strerror(-r) << ": " << err << std::endl;
    return -r;
  }
  return 0;
}

int check_min_obj_stripe_size(rgw::sal::Driver* driver, rgw::sal::Object* obj, uint64_t min_stripe_size, bool *need_rewrite)
{
  int ret = obj->get_obj_attrs(null_yield, dpp());
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

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj->get_read_op();

  int ret = read_op->prepare(null_yield, dpp());
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->fix_head_obj_locator(dpp(), obj->get_bucket()->get_info(), needs_fixing, remove_bad, obj->get_key(), null_yield);
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

  int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->fix_tail_obj_locator(dpp(), bucket_info, key, fix, &needs_fixing, null_yield);
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
  int ret = init_bucket(tenant_name, bucket_name, bucket_id, &bucket);
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
      cerr << "ERROR: driver->list_objects(): " << cpp_strerror(-ret) << std::endl;
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
static boost::optional<RGWRESTConn> get_remote_conn(rgw::sal::RadosStore* driver,
                                                    const RGWZoneGroup& zonegroup,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  if (remote == zonegroup.get_id()) {
    conn.emplace(driver->ctx(), driver, remote, zonegroup.endpoints, zonegroup.api_name);
  } else {
    for (const auto& z : zonegroup.zones) {
      const auto& zone = z.second;
      if (remote == zone.id) {
        conn.emplace(driver->ctx(), driver, remote, zone.endpoints, zonegroup.api_name);
        break;
      }
    }
  }
  return conn;
}

/// search each zonegroup for a connection
static boost::optional<RGWRESTConn> get_remote_conn(rgw::sal::RadosStore* driver,
                                                    const RGWPeriodMap& period_map,
                                                    const std::string& remote)
{
  boost::optional<RGWRESTConn> conn;
  for (const auto& zg : period_map.zonegroups) {
    conn = get_remote_conn(driver, zg.second, remote);
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

static int commit_period(rgw::sal::ConfigStore* cfgstore,
                         RGWRealm& realm, rgw::sal::RealmWriter& realm_writer,
                         RGWPeriod& period, string remote, const string& url,
                         std::optional<string> opt_region,
                         const string& access, const string& secret,
                         bool force)
{
  auto& master_zone = period.get_master_zone().id;
  if (master_zone.empty()) {
    cerr << "cannot commit period: period does not have a master zone of a master zonegroup" << std::endl;
    return -EINVAL;
  }
  // are we the period's master zone?
  if (driver->get_zone()->get_id() == master_zone) {
    // read the current period
    RGWPeriod current_period;
    int ret = cfgstore->read_period(dpp(), null_yield, realm.current_period,
                                    std::nullopt, current_period);
    if (ret < 0) {
      cerr << "failed to load current period: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    // the master zone can commit locally
    ret = rgw::commit_period(dpp(), null_yield, cfgstore, driver,
                             realm, realm_writer, current_period,
                             period, cerr, force);
    if (ret < 0) {
      cerr << "failed to commit period: " << cpp_strerror(-ret) << std::endl;
    }
    return ret;
  }

  if (remote.empty() && url.empty()) {
    // use the new master zone's connection
    remote = master_zone;
    cerr << "Sending period to new master zone " << remote << std::endl;
  }
  boost::optional<RGWRESTConn> conn;
  RGWRESTConn *remote_conn = nullptr;
  if (!remote.empty()) {
    conn = get_remote_conn(static_cast<rgw::sal::RadosStore*>(driver), period.get_map(), remote);
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

  // decode the response and driver it back
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
  constexpr bool exclusive = false;
  ret = cfgstore->create_period(dpp(), null_yield, exclusive, period);
  if (ret < 0) {
    cerr << "Error storing committed period " << period.get_id() << ": "
        << cpp_strerror(ret) << std::endl;
    return ret;
  }
  ret = rgw::reflect_period(dpp(), null_yield, cfgstore, period);
  if (ret < 0) {
    cerr << "Error updating local objects: " << cpp_strerror(ret) << std::endl;
    return ret;
  }
  (void) cfgstore->realm_notify_new_period(dpp(), null_yield, period);
  return ret;
}

static int update_period(rgw::sal::ConfigStore* cfgstore,
                         const string& realm_id, const string& realm_name,
                         const string& period_epoch, bool commit,
                         const string& remote, const string& url,
                         std::optional<string> opt_region,
                         const string& access, const string& secret,
                         Formatter *formatter, bool force)
{
  RGWRealm realm;
  std::unique_ptr<rgw::sal::RealmWriter> realm_writer;
  int ret = rgw::read_realm(dpp(), null_yield, cfgstore,
                            realm_id, realm_name,
                            realm, &realm_writer);
  if (ret < 0) {
    cerr << "failed to load realm " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  std::optional<epoch_t> epoch;
  if (!period_epoch.empty()) {
    epoch = atoi(period_epoch.c_str());
  }
  RGWPeriod period;
  ret = cfgstore->read_period(dpp(), null_yield, realm.current_period,
                              epoch, period);
  if (ret < 0) {
    cerr << "failed to load current period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  // convert to the realm's staging period
  rgw::fork_period(dpp(), period);
  // update the staging period with all of the realm's zonegroups
  ret = rgw::update_period(dpp(), null_yield, cfgstore, period);
  if (ret < 0) {
    return ret;
  }

  constexpr bool exclusive = false;
  ret = cfgstore->create_period(dpp(), null_yield, exclusive, period);
  if (ret < 0) {
    cerr << "failed to driver period: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  if (commit) {
    ret = commit_period(cfgstore, realm, *realm_writer, period, remote, url,
                        opt_region, access, secret, force);
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
                                const string& bucket_id,
				std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  int ret = init_bucket(tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  return 0;
}

static int do_period_pull(rgw::sal::ConfigStore* cfgstore,
                          RGWRESTConn *remote_conn, const string& url,
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
  try {
    decode_json_obj(*period, &p);
  } catch (const JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.what() << std::endl;
    return -EINVAL;
  }
  constexpr bool exclusive = false;
  ret = cfgstore->create_period(dpp(), null_yield, exclusive, *period);
  if (ret < 0) {
    cerr << "Error storing period " << period->get_id() << ": " << cpp_strerror(ret) << std::endl;
  }
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
  RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

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
  string master_period = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_current_period_id();

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

  if (!(sz = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->find_zone(source_zone))) {
    push_ss(ss, status, tab) << string("zone not found");
    flush_ss(ss, status);
    return;
  }

  if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->zone_syncs_from(*sz)) {
    push_ss(ss, status, tab) << string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, source_zone, nullptr);

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

  std::optional<std::pair<int, ceph::real_time>> oldest;
  if (!shards_behind.empty()) {
    map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(dpp(), shards_behind, &master_pos);

    if (ret < 0) {
      derr << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;
        if (shard_data.entries.empty()) {
          // there aren't any entries in this shard, so we're not really behind
          shards_behind.erase(iter.first);
          shards_behind_set.erase(iter.first);
        } else {
          rgw_datalog_entry& entry = shard_data.entries.front();
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
  int total_recovering = recovering_shards.size();

  if (total_behind == 0 && total_recovering == 0) {
    push_ss(ss, status, tab) << "data is caught up with source";
  } else if (total_behind > 0) {
    push_ss(ss, status, tab) << "data is behind on " << total_behind << " shards";
    push_ss(ss, status, tab) << "behind shards: " << "[" << shards_behind_set << "]";
    if (oldest) {
      push_ss(ss, status, tab) << "oldest incremental change not applied: "
          << oldest->second << " [" << oldest->first << ']';
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

// return features that are supported but not enabled
static auto get_disabled_features(const rgw::zone_features::set& enabled) {
  auto features = rgw::zone_features::set{rgw::zone_features::supported.begin(),
                                          rgw::zone_features::supported.end()};
  for (const auto& feature : enabled) {
    features.erase(feature);
  }
  return features;
}


static void sync_status(Formatter *formatter)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << zone->get_realm_id() << " (" << zone->get_realm_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone->get_id() << " (" << zone->get_name() << ")" << std::endl;
  cout << std::setw(width) << "current time" << std::setw(1) << " "
       << to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms) << std::endl;

  const auto& rzg =
    static_cast<const rgw::sal::RadosZoneGroup&>(zonegroup).get_group();

  cout << std::setw(width) << "zonegroup features enabled: " << rzg.enabled_features << std::endl;
  if (auto d = get_disabled_features(rzg.enabled_features); !d.empty()) {
    cout << std::setw(width) << "                   disabled: " << d << std::endl;
  }

  list<string> md_status;

  if (driver->is_meta_master()) {
    md_status.push_back("no sync (zone is master)");
  } else {
    get_md_sync_status(md_status);
  }

  tab_dump("metadata sync", width, md_status);

  list<string> data_status;

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_conn_map();

  for (auto iter : zone_conn_map) {
    const rgw_zone_id& source_id = iter.first;
    string source_str = "source: ";
    string s = source_str + source_id.id;
    std::unique_ptr<rgw::sal::Zone> sz;
    if (driver->get_zone()->get_zonegroup().get_zone_by_id(source_id.id, &sz) == 0) {
      s += string(" (") + sz->get_name() + ")";
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

static int bucket_source_sync_status(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* driver, const RGWZone& zone,
                                     const RGWZone& source, RGWRESTConn *conn,
                                     const RGWBucketInfo& bucket_info,
                                     rgw_sync_bucket_pipe pipe,
                                     int width, std::ostream& out)
{
  out << indented{width, "source zone"} << source.id << " (" << source.name << ")" << std::endl;

  // syncing from this zone?
  if (!driver->svc()->zone->zone_syncs_from(zone, source)) {
    out << indented{width} << "does not sync from zone\n";
    return 0;
  }

  if (!pipe.source.bucket) {
    ldpp_dout(dpp, -1) << __func__ << "(): missing source bucket" << dendl;
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> source_bucket;
  int r = init_bucket(*pipe.source.bucket, &source_bucket);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to read source bucket info: " << cpp_strerror(r) << dendl;
    return r;
  }

  out << indented{width, "source bucket"} << source_bucket->get_key() << std::endl;
  pipe.source.bucket = source_bucket->get_key();

  pipe.dest.bucket = bucket_info.bucket;

  uint64_t gen = 0;
  std::vector<rgw_bucket_shard_sync_info> shard_status;

  // check for full sync status
  rgw_bucket_sync_status full_status;
  r = rgw_read_bucket_full_sync_status(dpp, driver, pipe, &full_status, null_yield);
  if (r >= 0) {
    if (full_status.state == BucketSyncState::Init) {
      out << indented{width} << "init: bucket sync has not started\n";
      return 0;
    }
    if (full_status.state == BucketSyncState::Stopped) {
      out << indented{width} << "stopped: bucket sync is disabled\n";
      return 0;
    }
    if (full_status.state == BucketSyncState::Full) {
      out << indented{width} << "full sync: " << full_status.full.count << " objects completed\n";
      return 0;
    }
    gen = full_status.incremental_gen;
    shard_status.resize(full_status.shards_done_with_gen.size());
  } else if (r == -ENOENT) {
    // no full status, but there may be per-shard status from before upgrade
    const auto& logs = source_bucket->get_info().layout.logs;
    if (logs.empty()) {
      out << indented{width} << "init: bucket sync has not started\n";
      return 0;
    }
    const auto& log = logs.front();
    if (log.gen > 0) {
      // this isn't the backward-compatible case, so we just haven't started yet
      out << indented{width} << "init: bucket sync has not started\n";
      return 0;
    }
    if (log.layout.type != rgw::BucketLogType::InIndex) {
      ldpp_dout(dpp, -1) << "unrecognized log layout type " << log.layout.type << dendl;
      return -EINVAL;
    }
    // use shard count from our log gen=0
    shard_status.resize(rgw::num_shards(log.layout.in_index));
  } else {
    lderr(driver->ctx()) << "failed to read bucket full sync status: " << cpp_strerror(r) << dendl;
    return r;
  }

  r = rgw_read_bucket_inc_sync_status(dpp, driver, pipe, gen, &shard_status);
  if (r < 0) {
    lderr(driver->ctx()) << "failed to read bucket incremental sync status: " << cpp_strerror(r) << dendl;
    return r;
  }

  const int total_shards = shard_status.size();

  out << indented{width} << "incremental sync on " << total_shards << " shards\n";

  rgw_bucket_index_marker_info remote_info;
  BucketIndexShardsManager remote_markers;
  r = rgw_read_remote_bilog_info(dpp, conn, source_bucket->get_key(),
                                 remote_info, remote_markers, null_yield);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "failed to read remote log: " << cpp_strerror(r) << dendl;
    return r;
  }

  std::set<int> shards_behind;
  for (const auto& r : remote_markers.get()) {
    auto shard_id = r.first;
    if (r.second.empty()) {
      continue; // empty bucket index shard
    }
    if (shard_id >= total_shards) {
      // unexpected shard id. we don't have status for it, so we're behind
      shards_behind.insert(shard_id);
      continue;
    }
    auto& m = shard_status[shard_id];
    const auto pos = BucketIndexShardsManager::get_shard_marker(m.inc_marker.position);
    if (pos < r.second) {
      shards_behind.insert(shard_id);
    }
  }
  if (!shards_behind.empty()) {
    out << indented{width} << "bucket is behind on " << shards_behind.size() << " shards\n";
    out << indented{width} << "behind shards: [" << shards_behind << "]\n";
  } else {
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
      int ret = init_bucket(b, &hint_bucket);
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
  std::unique_ptr<rgw::sal::Zone> zone;
  int ret = driver->get_zone()->get_zonegroup().get_zone_by_id(s, &zone);
  if (ret < 0)
    ret = driver->get_zone()->get_zonegroup().get_zone_by_name(s, &zone);
  if (ret < 0)
    return rgw_zone_id(s);

  return rgw_zone_id(zone->get_id());
}

rgw_zone_id validate_zone_id(const rgw_zone_id& zone_id)
{
  return resolve_zone_id(zone_id.id);
}

static int sync_info(std::optional<rgw_zone_id> opt_target_zone, std::optional<rgw_bucket> opt_bucket, Formatter *formatter)
{
  rgw_zone_id zone_id = opt_target_zone.value_or(driver->get_zone()->get_id());

  auto zone_policy_handler = driver->get_zone()->get_sync_policy_handler();

  RGWBucketSyncPolicyHandlerRef bucket_handler;

  std::optional<rgw_bucket> eff_bucket = opt_bucket;

  auto handler = zone_policy_handler;

  if (eff_bucket) {
    std::unique_ptr<rgw::sal::Bucket> bucket;

    int ret = init_bucket(*eff_bucket, &bucket);
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
    int r = driver->get_sync_policy_handler(dpp(), zid, hint_bucket, &hint_bucket_handler, null_yield);
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

static int bucket_sync_info(rgw::sal::Driver* driver, const RGWBucketInfo& info,
                              std::ostream& out)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();
  constexpr int width = 15;

  out << indented{width, "realm"} << zone->get_realm_id() << " (" << zone->get_realm_name() << ")\n";
  out << indented{width, "zonegroup"} << zonegroup.get_id() << " (" << zonegroup.get_name() << ")\n";
  out << indented{width, "zone"} << zone->get_id() << " (" << zone->get_name() << ")\n";
  out << indented{width, "bucket"} << info.bucket << "\n\n";

  if (!static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp())) {
    out << "Sync is disabled for bucket " << info.bucket.name << '\n';
    return 0;
  }

  RGWBucketSyncPolicyHandlerRef handler;

  int r = driver->get_sync_policy_handler(dpp(), std::nullopt, info.bucket, &handler, null_yield);
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

static int bucket_sync_status(rgw::sal::Driver* driver, const RGWBucketInfo& info,
                              const rgw_zone_id& source_zone_id,
			      std::optional<rgw_bucket>& opt_source_bucket,
                              std::ostream& out)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();
  constexpr int width = 15;

  out << indented{width, "realm"} << zone->get_realm_id() << " (" << zone->get_realm_name() << ")\n";
  out << indented{width, "zonegroup"} << zonegroup.get_id() << " (" << zonegroup.get_name() << ")\n";
  out << indented{width, "zone"} << zone->get_id() << " (" << zone->get_name() << ")\n";
  out << indented{width, "bucket"} << info.bucket << "\n";
  out << indented{width, "current time"}
    << to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms) << "\n\n";


  if (!static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp())) {
    out << "Sync is disabled for bucket " << info.bucket.name << " or bucket has no sync sources" << std::endl;
    return 0;
  }

  RGWBucketSyncPolicyHandlerRef handler;

  int r = driver->get_sync_policy_handler(dpp(), std::nullopt, info.bucket, &handler, null_yield);
  if (r < 0) {
    ldpp_dout(dpp(), -1) << "ERROR: failed to get policy handler for bucket (" << info.bucket << "): r=" << r << ": " << cpp_strerror(-r) << dendl;
    return r;
  }

  auto sources = handler->get_all_sources();

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_conn_map();
  set<rgw_zone_id> zone_ids;

  if (!source_zone_id.empty()) {
    std::unique_ptr<rgw::sal::Zone> zone;
    int ret = driver->get_zone()->get_zonegroup().get_zone_by_id(source_zone_id.id, &zone);
    if (ret < 0) {
      ldpp_dout(dpp(), -1) << "Source zone not found in zonegroup "
          << zonegroup.get_name() << dendl;
      return -EINVAL;
    }
    auto c = zone_conn_map.find(source_zone_id);
    if (c == zone_conn_map.end()) {
      ldpp_dout(dpp(), -1) << "No connection to zone " << zone->get_name() << dendl;
      return -EINVAL;
    }
    zone_ids.insert(source_zone_id);
  } else {
    std::list<std::string> ids;
    int ret = driver->get_zone()->get_zonegroup().list_zones(ids);
    if (ret == 0) {
      for (const auto& entry : ids) {
	zone_ids.insert(entry);
      }
    }
  }

  for (auto& zone_id : zone_ids) {
    auto z = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zonegroup().zones.find(zone_id.id);
    if (z == static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zonegroup().zones.end()) { /* shouldn't happen */
      continue;
    }
    auto c = zone_conn_map.find(zone_id.id);
    if (c == zone_conn_map.end()) { /* shouldn't happen */
      continue;
    }

    for (auto& entry : sources) {
      auto& pipe = entry.second;
      if (opt_source_bucket &&
	  pipe.source.bucket != opt_source_bucket) {
	continue;
      }
      if (pipe.source.zone.value_or(rgw_zone_id()) == z->second.id) {
	bucket_source_sync_status(dpp(), static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone(), z->second,
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
  int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_rados_handle()->ioctx_create(pool.to_str().c_str(), io_ctx);
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

int check_reshard_bucket_params(rgw::sal::Driver* driver,
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

  if (num_shards > (int)static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_max_bucket_shards()) {
    cerr << "ERROR: num_shards too high, max value: " << static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_max_bucket_shards() << std::endl;
    return -EINVAL;
  }

  if (num_shards < 0) {
    cerr << "ERROR: num_shards must be non-negative integer" << std::endl;
    return -EINVAL;
  }

  int ret = init_bucket(tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  if (! is_layout_reshardable((*bucket)->get_info().layout)) {
    std::cerr << "Bucket '" << (*bucket)->get_name() <<
      "' currently has layout '" <<
      current_layout_desc((*bucket)->get_info().layout) <<
      "', which does not support resharding." << std::endl;
    return -EINVAL;
  }

  int num_source_shards = rgw::current_num_shards((*bucket)->get_info().layout);

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
    // coverity supression: oath_totp_validate2 is an external library function, cannot fix internally
    // Further, step_size is a small number and unlikely to overflow
    int rc = oath_totp_validate2(totp.seed_bin.c_str(), totp.seed_bin.length(),
                             start_time, 
                             // coverity[store_truncates_time_t:SUPPRESS]
                             step_size,
                             time_ofs,
                             1,
                             nullptr,
                             pins[0].c_str());
    if (rc != OATH_INVALID_OTP) {
      rc = oath_totp_validate2(totp.seed_bin.c_str(), totp.seed_bin.length(),
                               start_time, 
                               // coverity[store_truncates_time_t:SUPPRESS]
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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->timelog.trim(dpp(), oid, {}, {}, {}, marker, nullptr,
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
  rgw::sal::ConfigStore* cfgstore;
  RGWZoneGroup zonegroup;
  std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;

  std::optional<rgw_bucket> b;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  rgw_sync_policy_info *policy{nullptr};

  std::optional<rgw_user> owner;

public:
  SyncPolicyContext(rgw::sal::ConfigStore* cfgstore,
                    std::optional<rgw_bucket> _bucket)
      : cfgstore(cfgstore), b(std::move(_bucket)) {}

  int init(const string& zonegroup_id, const string& zonegroup_name) {
    int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore,
                                  zonegroup_id, zonegroup_name,
                                  zonegroup, &zonegroup_writer);
    if (ret < 0) {
      cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (!b) {
      policy = &zonegroup.sync_policy;
      return 0;
    }

    ret = init_bucket(*b, &bucket);
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
      int ret = zonegroup_writer->write(dpp(), null_yield, zonegroup);
      if (ret < 0) {
        cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      return 0;
    }

    int ret = bucket->put_info(dpp(), false, real_time(), null_yield);
    if (ret < 0) {
      cerr << "failed to driver bucket info: " << cpp_strerror(-ret) << std::endl;
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
  std::unique_ptr<rgw::sal::Zone> zone;
  int ret = driver->get_zone()->get_zonegroup().get_zone_by_name(*zone_name, &zone);
  if (ret < 0) {
    cerr << "WARNING: cannot find source zone id for name=" << *zone_name << std::endl;
    zone_id = rgw_zone_id(*zone_name);
  } else {
    zone_id->id = zone->get_id();
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
    std::unique_ptr<rgw::sal::Zone> zone;
    int ret = driver->get_zone()->get_zonegroup().get_zone_by_name(name, &zone);
    if (ret < 0) {
      cerr << "WARNING: cannot find source zone id for name=" << name << std::endl;
      zid = rgw_zone_id(name);
    } else {
      zid.id = zone->get_id();
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
      std::unique_ptr<rgw::sal::Zone> zone;
      if (driver->get_zone()->get_zonegroup().get_zone_by_id(zone_id.id, &zone) == 0) {
        zone_name = zone->get_name();
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

void init_realm_param(CephContext *cct, string& var, std::optional<string>& opt_var, const string& conf_name)
{
  var = cct->_conf.get_val<string>(conf_name);
  if (!var.empty()) {
    opt_var = var;
  }
}

// This has an uncaught exception. Even if the exception is caught, the program
// would need to be terminated, so the warning is simply suppressed.
// coverity[root_function:SUPPRESS]
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

  auto cct = rgw_global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_UTILITY, 0);
  ceph::async::io_context_pool context_pool(cct->_conf->rgw_thread_pool_size);

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
  std::string role_name, path, assume_role_doc, policy_name, perm_policy_doc, path_prefix, max_session_duration;
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
  int key_active = true;
  bool key_active_specified = false;
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
  std::string objects_file;
  string object_version;
  string placement_id;
  std::optional<string> opt_storage_class;
  list<string> tags;
  list<string> tags_add;
  list<string> tags_rm;
  int placement_inline_data = true;
  bool placement_inline_data_specified = false;

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
  ceph::timespan min_age = std::chrono::hours(1);
  bool hide_progress = false;
  bool dump_keys = false;
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
  string notification_id;
  string sub_name;
  string event_id;

  std::optional<uint64_t> gen;
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

  std::optional<std::string> inject_error_at;
  std::optional<int> inject_error_code;
  std::optional<std::string> inject_abort_at;
  std::optional<std::string> inject_delay_at;
  ceph::timespan inject_delay = std::chrono::milliseconds(2000);

  rgw::zone_features::set enable_features;
  rgw::zone_features::set disable_features;

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
    } else if (ceph_argparse_witharg(args, i, &val, "--objects-file", (char*)NULL)) {
      objects_file = val;
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
    } else if (ceph_argparse_binary_flag(args, i, &key_active, NULL, "--key-active", (char*)NULL)) {
      key_active_specified = true;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--min-age-hours", (char*)NULL)) {
      min_age = std::chrono::hours(atoi(val.c_str()));
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
    } else if (ceph_argparse_witharg(args, i, &val, "--gen", (char*)NULL)) {
      gen = strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse gen id: " << err << std::endl;
        return EINVAL;
      }
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
    } else if (ceph_argparse_flag(args, i, "--hide-progress", (char*)NULL)) {
      hide_progress = true;
    } else if (ceph_argparse_flag(args, i, "--dump-keys", (char*)NULL)) {
      dump_keys = true;
    } else if (ceph_argparse_binary_flag(args, i, &placement_inline_data, NULL, "--placement-inline-data", (char*)NULL)) {
      placement_inline_data_specified = true;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--max-session-duration", (char*)NULL)) {
      max_session_duration = val;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--notification-id", (char*)NULL)) {
      notification_id = val;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--inject-error-at", (char*)NULL)) {
      inject_error_at = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--inject-error-code", (char*)NULL)) {
      inject_error_code = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--inject-abort-at", (char*)NULL)) {
      inject_abort_at = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--inject-delay-at", (char*)NULL)) {
      inject_delay_at = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--inject-delay-ms", (char*)NULL)) {
      inject_delay = std::chrono::milliseconds(atoi(val.c_str()));
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
    } else if (ceph_argparse_witharg(args, i, &val, "--enable-feature", (char*)NULL)) {
      if (!rgw::zone_features::supports(val)) {
        std::cerr << "ERROR: Cannot enable unrecognized zone feature \"" << val << "\"" << std::endl;
        return EINVAL;
      }
      enable_features.insert(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--disable-feature", (char*)NULL)) {
      disable_features.insert(val);
    } else if (strncmp(*i, "-", 1) == 0) {
      cerr << "ERROR: invalid flag " << *i << std::endl;
      return EINVAL;
    } else {
      ++i;
    }
  }

  /* common_init_finish needs to be called after g_conf().set_val() */
  common_init_finish(g_ceph_context);

  std::unique_ptr<rgw::sal::ConfigStore> cfgstore;

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

    // Before a period commit or pull, our zonegroup may not be in the
    // period, causing `load_period_zonegroup` to fail.
    bool localzonegroup_op = ((opt_cmd == OPT::PERIOD_UPDATE && commit) ||
			      (opt_cmd == OPT::PERIOD_PULL && url.empty()));

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
			 OPT::BUCKET_LAYOUT,
			 OPT::BUCKET_STATS,
			 OPT::BUCKET_SYNC_CHECKPOINT,
			 OPT::BUCKET_SYNC_INFO,
			 OPT::BUCKET_SYNC_STATUS,
			 OPT::BUCKET_SYNC_MARKERS,
			 OPT::BUCKET_SHARD_OBJECTS,
			 OPT::BUCKET_OBJECT_SHARD,
			 OPT::LOG_LIST,
			 OPT::LOG_SHOW,
			 OPT::USAGE_SHOW,
			 OPT::OBJECT_STAT,
			 OPT::OBJECT_MANIFEST,
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
			 OPT::PUBSUB_TOPIC_LIST,
       OPT::PUBSUB_NOTIFICATION_LIST,
			 OPT::PUBSUB_TOPIC_GET,
       OPT::PUBSUB_NOTIFICATION_GET,
       OPT::PUBSUB_TOPIC_STATS  ,
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
			 OPT::LC_PROCESS,
       OPT::BUCKET_SYNC_RUN,
       OPT::DATA_SYNC_RUN,
       OPT::BUCKET_REWRITE,
       OPT::OBJECT_REWRITE
    };

    raw_storage_op = (raw_storage_ops_list.find(opt_cmd) != raw_storage_ops_list.end() ||
			   raw_period_update || raw_period_pull);
    bool need_cache = readonly_ops_list.find(opt_cmd) == readonly_ops_list.end();
    bool need_gc = (gc_ops_list.find(opt_cmd) != gc_ops_list.end()) && !bypass_gc;

    DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);

    auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
    cfgstore = DriverManager::create_config_store(dpp(), config_store_type);
    if (!cfgstore) {
      cerr << "couldn't init config storage provider" << std::endl;
      return EIO;
    }

    std::unique_ptr<rgw::SiteConfig> site;

    if (raw_storage_op) {
      site = rgw::SiteConfig::make_fake();
      driver = DriverManager::get_raw_storage(dpp(), g_ceph_context,
					      cfg, context_pool, *site);
    } else {
      site = std::make_unique<rgw::SiteConfig>();
      auto r = site->load(dpp(), null_yield, cfgstore.get(), localzonegroup_op);
      if (r < 0) {
	std::cerr << "Unable to initialize site config." << std::endl;
	exit(1);
      }

      driver = DriverManager::get_storage(dpp(),
					g_ceph_context,
					cfg,
					context_pool,
					*site,
					false,
					false,
					false,
					false,
					false,
                                        false,
                                        null_yield,
					need_cache && g_conf()->rgw_cache_enabled,
					need_gc);
    }
    if (!driver) {
      cerr << "couldn't init storage provider" << std::endl;
      return EIO;
    }

    /* Needs to be after the driver is initialized.  Note, user could be empty here. */
    user = driver->get_user(user_id_arg);

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
                          && opt_cmd != OPT::ROLE_TRUST_POLICY_MODIFY
                          && opt_cmd != OPT::ROLE_LIST
                          && opt_cmd != OPT::ROLE_POLICY_PUT
                          && opt_cmd != OPT::ROLE_POLICY_LIST
                          && opt_cmd != OPT::ROLE_POLICY_GET
                          && opt_cmd != OPT::ROLE_POLICY_DELETE
                          && opt_cmd != OPT::ROLE_UPDATE
                          && opt_cmd != OPT::RESHARD_ADD
                          && opt_cmd != OPT::RESHARD_CANCEL
                          && opt_cmd != OPT::RESHARD_STATUS
                          && opt_cmd != OPT::PUBSUB_TOPIC_LIST
                          && opt_cmd != OPT::PUBSUB_NOTIFICATION_LIST
                          && opt_cmd != OPT::PUBSUB_TOPIC_GET
                          && opt_cmd != OPT::PUBSUB_NOTIFICATION_GET
                          && opt_cmd != OPT::PUBSUB_TOPIC_RM
                          && opt_cmd != OPT::PUBSUB_NOTIFICATION_RM
                          && opt_cmd != OPT::PUBSUB_TOPIC_STATS
			  && opt_cmd != OPT::SCRIPT_PUT
			  && opt_cmd != OPT::SCRIPT_GET
			  && opt_cmd != OPT::SCRIPT_RM) {
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
    formatter = make_unique<XMLFormatter>(pretty_format);
  else if (format == "json")
    formatter = make_unique<JSONFormatter>(pretty_format);
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

  RGWUserAdminOpState user_op(driver);
  if (!user_email.empty()) {
    user_op.user_email_specified=true;
  }

  if (!source_zone_name.empty()) {
    std::unique_ptr<rgw::sal::Zone> zone;
    if (driver->get_zone()->get_zonegroup().get_zone_by_name(source_zone_name, &zone) < 0) {
      cerr << "WARNING: cannot find source zone id for name=" << source_zone_name << std::endl;
      source_zone = source_zone_name;
    } else {
      source_zone.id = zone->get_id();
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

  StoreDestructor store_destructor(driver);

  if (raw_storage_op) {
    switch (opt_cmd) {
    case OPT::PERIOD_DELETE:
      {
	if (period_id.empty()) {
	  cerr << "missing period id" << std::endl;
	  return EINVAL;
	}
        int ret = cfgstore->delete_period(dpp(), null_yield, period_id);
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT::PERIOD_GET:
      {
        std::optional<epoch_t> epoch;
	if (!period_epoch.empty()) {
	  epoch = atoi(period_epoch.c_str());
	}
        if (staging) {
          RGWRealm realm;
          int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                    realm_id, realm_name, realm);
          if (ret < 0 ) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          realm_id = realm.get_id();
          realm_name = realm.get_name();
          period_id = RGWPeriod::get_staging_id(realm_id);
          epoch = 1;
        }
        if (period_id.empty()) {
          // use realm's current period
          RGWRealm realm;
          int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                    realm_id, realm_name, realm);
          if (ret < 0 ) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          period_id = realm.current_period;
        }

	RGWPeriod period;
        int ret = cfgstore->read_period(dpp(), null_yield, period_id,
                                        epoch, period);
	if (ret < 0) {
	  cerr << "failed to load period: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("period", period, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_GET_CURRENT:
      {
        RGWRealm realm;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm);
	if (ret < 0) {
          std::cerr << "failed to load realm: " << cpp_strerror(ret) << std::endl;
	  return -ret;
	}

	formatter->open_object_section("period_get_current");
	encode_json("current_period", realm.current_period, formatter.get());
	formatter->close_section();
	formatter->flush(cout);
      }
      break;
    case OPT::PERIOD_LIST:
      {
        Formatter::ObjectSection periods_list{*formatter, "periods_list"};
        Formatter::ArraySection periods{*formatter, "periods"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> period_ids; // list in pages of 1000
        do {
          int ret = cfgstore->list_period_ids(dpp(), null_yield, listing.next,
                                              period_ids, listing);
          if (ret < 0) {
            std::cerr << "failed to list periods: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& id : listing.entries) {
            encode_json("id", id, formatter.get());
          }
        } while (!listing.next.empty());
      } // close sections periods and periods_list
      formatter->flush(cout);
      break;
    case OPT::PERIOD_UPDATE:
      {
        int ret = update_period(cfgstore.get(), realm_id, realm_name,
                                period_epoch, commit, remote, url,
                                opt_region, access_key, secret_key,
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
          RGWRealm realm;
          int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                    realm_id, realm_name, realm);
          if (ret < 0 ) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          period_id = realm.current_period;

          RGWPeriod current_period;
          ret = cfgstore->read_period(dpp(), null_yield, period_id,
                                      std::nullopt, current_period);
          if (ret < 0) {
            cerr << "failed to load current period: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          if (remote.empty()) {
            // use realm master zone as remote
            remote = current_period.get_master_zone().id;
          }
          conn = get_remote_conn(static_cast<rgw::sal::RadosStore*>(driver), current_period.get_map(), remote);
          if (!conn) {
            cerr << "failed to find a zone or zonegroup for remote "
                << remote << std::endl;
            return -ENOENT;
          }
          remote_conn = &*conn;
        }

        RGWPeriod period;
        int ret = do_period_pull(cfgstore.get(), remote_conn, url,
                                 opt_region, access_key, secret_key,
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
          if (!realm_name.empty()) {
            // look up realm_id for the given realm_name
            int ret = cfgstore->read_realm_id(dpp(), null_yield,
                                              realm_name, realm_id);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default realm_id when none is given
            int ret = cfgstore->read_default_realm_id(dpp(), null_yield,
                                                      realm_id);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = cfgstore->read_period_config(dpp(), null_yield, realm_id,
                                               period_config);
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
          constexpr bool exclusive = false;
          ret = cfgstore->write_period_config(dpp(), null_yield, exclusive,
                                              realm_id, period_config);
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
          if (!realm_name.empty()) {
            // look up realm_id for the given realm_name
            int ret = cfgstore->read_realm_id(dpp(), null_yield,
                                              realm_name, realm_id);
            if (ret < 0) {
              cerr << "ERROR: failed to read realm for " << realm_name
                  << ": " << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          } else {
            // use default realm_id when none is given
            int ret = cfgstore->read_default_realm_id(dpp(), null_yield,
                                                      realm_id);
            if (ret < 0 && ret != -ENOENT) { // on ENOENT, use empty realm_id
              cerr << "ERROR: failed to read default realm: "
                  << cpp_strerror(-ret) << std::endl;
              return -ret;
            }
          }
        }

        RGWPeriodConfig period_config;
        int ret = cfgstore->read_period_config(dpp(), null_yield, realm_id,
                                               period_config);
        if (ret < 0 && ret != -ENOENT) {
          cerr << "ERROR: failed to read period config: "
              << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        formatter->open_object_section("period_config");
        if (quota_scope == "bucket") {
          set_quota_info(period_config.quota.bucket_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("bucket quota", period_config.quota.bucket_quota, formatter.get());
        } else if (quota_scope == "user") {
          set_quota_info(period_config.quota.user_quota, opt_cmd,
                         max_size, max_objects,
                         have_max_size, have_max_objects);
          encode_json("user quota", period_config.quota.user_quota, formatter.get());
        } else if (quota_scope.empty() && opt_cmd == OPT::GLOBAL_QUOTA_GET) {
          // if no scope is given for GET, print both
          encode_json("bucket quota", period_config.quota.bucket_quota, formatter.get());
          encode_json("user quota", period_config.quota.user_quota, formatter.get());
        } else {
          cerr << "ERROR: invalid quota scope specification. Please specify "
              "either --quota-scope=bucket, or --quota-scope=user" << std::endl;
          return EINVAL;
        }
        formatter->close_section();

        if (opt_cmd != OPT::GLOBAL_QUOTA_GET) {
          // write the modified period config
          constexpr bool exclusive = false;
          ret = cfgstore->write_period_config(dpp(), null_yield, exclusive,
                                              realm_id, period_config);
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

	RGWRealm realm;
        realm.name = realm_name;

        constexpr bool exclusive = true;
	int ret = rgw::create_realm(dpp(), null_yield, cfgstore.get(),
                                    exclusive, realm);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create realm " << realm_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = rgw::set_default_realm(dpp(), null_yield, cfgstore.get(), realm);
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
	if (realm_id.empty() && realm_name.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}
	RGWRealm realm;
        std::unique_ptr<rgw::sal::RealmWriter> writer;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm, &writer);
	if (ret < 0) {
	  cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->remove(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "failed to remove realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

      }
      break;
    case OPT::REALM_GET:
      {
	RGWRealm realm;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm);
	if (ret < 0) {
	  if (ret == -ENOENT && realm_name.empty() && realm_id.empty()) {
	    cerr << "missing realm name or id, or default realm not found" << std::endl;
	  } else {
	    cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
          }
	  return -ret;
	}
	encode_json("realm", realm, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::REALM_GET_DEFAULT:
      {
	string default_id;
	int ret = cfgstore->read_default_realm_id(dpp(), null_yield, default_id);
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
        std::string default_id;
        int ret = cfgstore->read_default_realm_id(dpp(), null_yield,
                                                  default_id);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default realm: " << cpp_strerror(-ret) << std::endl;
	}

        Formatter::ObjectSection realms_list{*formatter, "realms_list"};
        encode_json("default_info", default_id, formatter.get());

        Formatter::ArraySection realms{*formatter, "realms"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> names; // list in pages of 1000
        do {
          ret = cfgstore->list_realm_names(dpp(), null_yield, listing.next,
                                           names, listing);
          if (ret < 0) {
            std::cerr << "failed to list realms: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& name : listing.entries) {
            encode_json("name", name, formatter.get());
          }
        } while (!listing.next.empty());
      } // close sections realms and realms_list
      formatter->flush(cout);
      break;
    case OPT::REALM_LIST_PERIODS:
      {
        // use realm's current period
        RGWRealm realm;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm);
        if (ret < 0) {
          cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        period_id = realm.current_period;

        Formatter::ObjectSection periods_list{*formatter, "realm_periods_list"};
	encode_json("current_period", period_id, formatter.get());

        Formatter::ArraySection periods{*formatter, "periods"};

        while (!period_id.empty()) {
          RGWPeriod period;
          ret = cfgstore->read_period(dpp(), null_yield, period_id,
                                      std::nullopt, period);
          if (ret < 0) {
            cerr << "failed to load period id " << period_id
                << ": " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          encode_json("id", period_id, formatter.get());
          period_id = period.predecessor_uuid;
        }
      } // close sections periods and realm_periods_list
      formatter->flush(cout);
      break;

    case OPT::REALM_RENAME:
      {
	if (realm_new_name.empty()) {
	  cerr << "missing realm new name" << std::endl;
	  return EINVAL;
	}
	if (realm_name.empty() && realm_id.empty()) {
	  cerr << "missing realm name or id" << std::endl;
	  return EINVAL;
	}

        RGWRealm realm;
        std::unique_ptr<rgw::sal::RealmWriter> writer;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm, &writer);
	if (ret < 0) {
	  cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->rename(dpp(), null_yield, realm, realm_new_name);
	if (ret < 0) {
	  cerr << "rename failed: " << cpp_strerror(-ret) << std::endl;
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
	bool new_realm = false;
        RGWRealm realm;
        std::unique_ptr<rgw::sal::RealmWriter> writer;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm, &writer);
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
          constexpr bool exclusive = true;
          ret = rgw::create_realm(dpp(), null_yield, cfgstore.get(),
                                  exclusive, realm);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't create new realm: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	} else {
          ret = writer->write(dpp(), null_yield, realm);
	  if (ret < 0) {
	    cerr << "ERROR: couldn't driver realm info: " << cpp_strerror(-ret) << std::endl;
	    return 1;
	  }
	}

        if (set_default) {
          ret = rgw::set_default_realm(dpp(), null_yield, cfgstore.get(), realm);
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
        RGWRealm realm;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm);
	if (ret < 0) {
	  cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = rgw::set_default_realm(dpp(), null_yield, cfgstore.get(), realm);
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
          ret = do_period_pull(cfgstore.get(), nullptr, url, opt_region,
                               access_key, secret_key,
                               realm_id, realm_name, current_period, "",
                               &period);
          if (ret < 0) {
            cerr << "could not fetch period " << current_period << std::endl;
            return -ret;
          }
        }
        constexpr bool exclusive = false;
        ret = rgw::create_realm(dpp(), null_yield, cfgstore.get(),
                                exclusive, realm);
        if (ret < 0) {
          cerr << "Error storing realm " << realm.get_id() << ": "
            << cpp_strerror(ret) << std::endl;
          return -ret;
        }

        if (set_default) {
          ret = rgw::set_default_realm(dpp(), null_yield, cfgstore.get(), realm);
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

        // load the zonegroup and zone params
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup " << zonegroup_name << " id "
              << zonegroup_id << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                             zone_id, zone_name, zone_params, &zone_writer);
	if (ret < 0) {
	  cerr << "unable to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        // update zone_params if necessary
        bool need_zone_update = false;

        if (zone_params.realm_id != zonegroup.realm_id) {
          if (!zone_params.realm_id.empty()) {
            cerr << "WARNING: overwriting zone realm_id=" << zone_params.realm_id
                << " to match zonegroup realm_id=" << zonegroup.realm_id << std::endl;
          }
          zone_params.realm_id = zonegroup.realm_id;
          need_zone_update = true;
        }

        for (auto a : tier_config_add) {
          ret = zone_params.tier_config.set(a.first, a.second);
          if (ret < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
          need_zone_update = true;
        }

        if (need_zone_update) {
          ret = zone_writer->write(dpp(), null_yield, zone_params);
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

        const bool *pis_master = (is_master_set ? &is_master : nullptr);
        const bool *pread_only = (is_read_only_set ? &read_only : nullptr);
        const bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        const string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        // validate --tier-type if specified
        const string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
        if (ptier_type) {
          auto sync_mgr = static_cast<rgw::sal::RadosStore*>(driver)->svc()->sync_modules->get_manager();
          if (!sync_mgr->get_module(*ptier_type, nullptr)) {
            ldpp_dout(dpp(), -1) << "ERROR: could not find sync module: "
                << *ptier_type << ",  valid sync modules: "
                << sync_mgr->get_registered_module_names() << dendl;
            return EINVAL;
          }
        }

        if (enable_features.empty()) { // enable all features by default
          enable_features.insert(rgw::zone_features::supported.begin(),
                                 rgw::zone_features::supported.end());
        }

        // add/update the public zone information stored in the zonegroup
        ret = rgw::add_zone_to_group(dpp(), zonegroup, zone_params,
                                     pis_master, pread_only, endpoints,
                                     ptier_type, psync_from_all,
                                     sync_from, sync_from_rm,
                                     predirect_zone, bucket_index_max_shards,
                                     enable_features, disable_features);
        if (ret < 0) {
          return -ret;
        }

        // write the updated zonegroup
        ret = zonegroup_writer->write(dpp(), null_yield, zonegroup);
	if (ret < 0) {
	  cerr << "failed to write updated zonegroup " << zonegroup.get_name()
              << ": " << cpp_strerror(-ret) << std::endl;
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
	RGWRealm realm;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm);
	if (ret < 0) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
        zonegroup.name = zonegroup_name;
        zonegroup.is_master = is_master;
        zonegroup.realm_id = realm.get_id();
        zonegroup.endpoints = endpoints;
        zonegroup.api_name = (api_name.empty() ? zonegroup_name : api_name);

        zonegroup.enabled_features = enable_features;
        if (zonegroup.enabled_features.empty()) { // enable features by default
          zonegroup.enabled_features.insert(rgw::zone_features::enabled.begin(),
                                            rgw::zone_features::enabled.end());
        }
        for (const auto& feature : disable_features) {
          auto i = zonegroup.enabled_features.find(feature);
          if (i == zonegroup.enabled_features.end()) {
            ldout(cct, 1) << "WARNING: zone feature \"" << feature
                << "\" was not enabled in zonegroup " << zonegroup_name << dendl;
            continue;
          }
          zonegroup.enabled_features.erase(i);
        }

        constexpr bool exclusive = true;
        ret = rgw::create_zonegroup(dpp(), null_yield, cfgstore.get(),
                                    exclusive, zonegroup);
	if (ret < 0) {
	  cerr << "failed to create zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = rgw::set_default_zonegroup(dpp(), null_yield, cfgstore.get(),
                                           zonegroup);
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

	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup);
	if (ret < 0) {
	  cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        ret = rgw::set_default_zonegroup(dpp(), null_yield, cfgstore.get(),
                                         zonegroup);
	if (ret < 0) {
	  cerr << "failed to set zonegroup as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_DELETE:
      {
	if (zonegroup_id.empty() && zonegroup_name.empty()) {
	  cerr << "no zonegroup name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup, &writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->remove(dpp(), null_yield);
	if (ret < 0) {
	  cerr << "ERROR: couldn't delete zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_GET:
      {
	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name, zonegroup);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	encode_json("zonegroup", zonegroup, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONEGROUP_LIST:
      {
        RGWZoneGroup default_zonegroup;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      {}, {}, default_zonegroup);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zonegroup: " << cpp_strerror(-ret) << std::endl;
	}

        Formatter::ObjectSection zonegroups_list{*formatter, "zonegroups_list"};
        encode_json("default_info", default_zonegroup.id, formatter.get());

        Formatter::ArraySection zonegroups{*formatter, "zonegroups"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> names; // list in pages of 1000
        do {
          ret = cfgstore->list_zonegroup_names(dpp(), null_yield, listing.next,
                                               names, listing);
          if (ret < 0) {
            std::cerr << "failed to list zonegroups: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& name : listing.entries) {
            encode_json("name", name, formatter.get());
          }
        } while (!listing.next.empty());
      } // close sections zonegroups and zonegroups_list
      formatter->flush(cout);
      break;
    case OPT::ZONEGROUP_MODIFY:
      {
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup, &writer);
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
	  zonegroup.is_master = is_master;
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
          ret = cfgstore->read_realm_id(dpp(), null_yield, realm_name,
                                        zonegroup.realm_id);
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

        for (const auto& feature : enable_features) {
          zonegroup.enabled_features.insert(feature);
          need_update = true;
        }
        for (const auto& feature : disable_features) {
          auto i = zonegroup.enabled_features.find(feature);
          if (i == zonegroup.enabled_features.end()) {
            ldout(cct, 1) << "WARNING: zone feature \"" << feature
                << "\" was not enabled in zonegroup "
                << zonegroup.get_name() << dendl;
            continue;
          }
          zonegroup.enabled_features.erase(i);
          need_update = true;
        }

        if (need_update) {
	  ret = writer->write(dpp(), null_yield, zonegroup);
	  if (ret < 0) {
	    cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = rgw::set_default_zonegroup(dpp(), null_yield, cfgstore.get(),
                                           zonegroup);
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
	RGWRealm realm;
        int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                  realm_id, realm_name, realm);
	bool default_realm_not_exist = (ret == -ENOENT && realm_id.empty() && realm_name.empty());

	if (ret < 0 && !default_realm_not_exist) {
	  cerr << "failed to init realm: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
	ret = read_decode_json(infile, zonegroup);
	if (ret < 0) {
	  return 1;
	}
	if (zonegroup.realm_id.empty() && !default_realm_not_exist) {
	  zonegroup.realm_id = realm.get_id();
	}
        // validate zonegroup features
        for (const auto& feature : zonegroup.enabled_features) {
          if (!rgw::zone_features::supports(feature)) {
            std::cerr << "ERROR: Unrecognized zonegroup feature \""
                << feature << "\"" << std::endl;
            return EINVAL;
          }
        }
        for (const auto& [name, zone] : zonegroup.zones) {
          // validate zone features
          for (const auto& feature : zone.supported_features) {
            if (!rgw::zone_features::supports(feature)) {
              std::cerr << "ERROR: Unrecognized zone feature \""
                  << feature << "\" in zone " << zone.name << std::endl;
              return EINVAL;
            }
          }
          // zone must support everything zonegroup does
          for (const auto& feature : zonegroup.enabled_features) {
            if (!zone.supports(feature)) {
              std::cerr << "ERROR: Zone " << name << " does not support feature \""
                  << feature << "\" required by zonegroup" << std::endl;
              return EINVAL;
            }
          }
        }

        // create/overwrite the zonegroup info
        constexpr bool exclusive = false;
        ret = rgw::create_zonegroup(dpp(), null_yield, cfgstore.get(),
                                    exclusive, zonegroup);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create zonegroup info: " << cpp_strerror(-ret) << std::endl;
	  return 1;
	}

        if (set_default) {
          ret = rgw::set_default_zonegroup(dpp(), null_yield, cfgstore.get(),
                                           zonegroup);
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
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup, &writer);
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

        ret = rgw::remove_zone_from_group(dpp(), zonegroup, zone_id);
        if (ret < 0) {
          cerr << "failed to remove zone: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        ret = writer->write(dpp(), null_yield, zonegroup);
        if (ret < 0) {
          cerr << "failed to write zonegroup: " << cpp_strerror(-ret) << std::endl;
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
	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup, &writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
        ret = writer->rename(dpp(), null_yield, zonegroup, zonegroup_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONEGROUP_PLACEMENT_LIST:
      {
	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name, zonegroup);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
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

	RGWZoneGroup zonegroup;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name, zonegroup);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
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

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> writer;
        int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                      zonegroup_id, zonegroup_name,
                                      zonegroup, &writer);
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

      if (zonegroup.default_placement.empty()) {
        zonegroup.default_placement.init(rule.name, RGW_STORAGE_CLASS_STANDARD);
      }
    } else if (opt_cmd == OPT::ZONEGROUP_PLACEMENT_RM) {
      if (!opt_storage_class || opt_storage_class->empty()) {
        zonegroup.placement_targets.erase(placement_id);
        if (zonegroup.default_placement.name == placement_id) {
          // clear default placement
          zonegroup.default_placement.clear();
        }
      } else {
        auto iter = zonegroup.placement_targets.find(placement_id);
        if (iter != zonegroup.placement_targets.end()) {
          RGWZoneGroupPlacementTarget& info = zonegroup.placement_targets[placement_id];
          info.storage_classes.erase(*opt_storage_class);

          if (zonegroup.default_placement == rule) {
            // clear default storage class
            zonegroup.default_placement.storage_class.clear();
          }

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

    ret = writer->write(dpp(), null_yield, zonegroup);
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

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
	/* if the user didn't provide zonegroup info , create stand alone zone */
	if (!zonegroup_id.empty() || !zonegroup_name.empty()) {
          int ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                        zonegroup_id, zonegroup_name,
                                        zonegroup, &zonegroup_writer);
	  if (ret < 0) {
	    cerr << "failed to load zonegroup " << zonegroup_name << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  if (realm_id.empty() && realm_name.empty()) {
	    realm_id = zonegroup.realm_id;
	  }
	}

        // create the local zone params
	RGWZoneParams zone_params;
        zone_params.id = zone_id;
        zone_params.name = zone_name;

        zone_params.system_key.id = access_key;
        zone_params.system_key.key = secret_key;
	zone_params.realm_id = realm_id;
        for (const auto& a : tier_config_add) {
          int r = zone_params.tier_config.set(a.first, a.second);
          if (r < 0) {
            cerr << "ERROR: failed to set configurable: " << a << std::endl;
            return EINVAL;
          }
        }

        if (zone_params.realm_id.empty()) {
          RGWRealm realm;
          int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                    realm_id, realm_name, realm);
          if (ret < 0 && ret != -ENOENT) {
            cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          zone_params.realm_id = realm.id;
          cerr << "NOTICE: set zone's realm_id=" << realm.id << std::endl;
        }

        constexpr bool exclusive = true;
        int ret = rgw::create_zone(dpp(), null_yield, cfgstore.get(),
                                   exclusive, zone_params);
	if (ret < 0) {
	  cerr << "failed to create zone " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	if (zonegroup_writer) {
          const bool *pis_master = (is_master_set ? &is_master : nullptr);
          const bool *pread_only = (is_read_only_set ? &read_only : nullptr);
          const bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
          const string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

          // validate --tier-type if specified
          const string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
          if (ptier_type) {
            auto sync_mgr = static_cast<rgw::sal::RadosStore*>(driver)->svc()->sync_modules->get_manager();
            if (!sync_mgr->get_module(*ptier_type, nullptr)) {
              ldpp_dout(dpp(), -1) << "ERROR: could not find sync module: "
                  << *ptier_type << ",  valid sync modules: "
                  << sync_mgr->get_registered_module_names() << dendl;
              return EINVAL;
            }
          }

          if (enable_features.empty()) { // enable all features by default
            enable_features.insert(rgw::zone_features::supported.begin(),
                                   rgw::zone_features::supported.end());
          }

          // add/update the public zone information stored in the zonegroup
          ret = rgw::add_zone_to_group(dpp(), zonegroup, zone_params,
                                       pis_master, pread_only, endpoints,
                                       ptier_type, psync_from_all,
                                       sync_from, sync_from_rm,
                                       predirect_zone, bucket_index_max_shards,
                                       enable_features, disable_features);
          if (ret < 0) {
            return -ret;
          }

          // write the updated zonegroup
          ret = zonegroup_writer->write(dpp(), null_yield, zonegroup);
	  if (ret < 0) {
	    cerr << "failed to add zone " << zone_name << " to zonegroup " << zonegroup.get_name()
		 << ": " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	}

        if (set_default) {
          ret = rgw::set_default_zone(dpp(), null_yield, cfgstore.get(),
                                      zone_params);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

	encode_json("zone", zone_params, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_DEFAULT:
      {
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone_params;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone_params);
	if (ret < 0) {
	  cerr << "unable to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        ret = rgw::set_default_zone(dpp(), null_yield, cfgstore.get(),
                                    zone_params);
	if (ret < 0) {
	  cerr << "failed to set zone as default: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONE_DELETE:
      {
	if (zone_id.empty() && zone_name.empty()) {
	  cerr << "no zone name or id provided" << std::endl;
	  return EINVAL;
	}
	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone_params, &writer);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        ret = rgw::delete_zone(dpp(), null_yield, cfgstore.get(),
                               zone_params, *writer);
	if (ret < 0) {
	  cerr << "failed to delete zone " << zone_params.get_name()
              << ": " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
      }
      break;
    case OPT::ZONE_GET:
      {
	RGWZoneParams zone_params;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone_params);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	encode_json("zone", zone_params, formatter.get());
	formatter->flush(cout);
      }
      break;
    case OPT::ZONE_SET:
      {
	RGWZoneParams zone;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone, &writer);
        if (ret < 0 && ret != -ENOENT) {
	  cerr << "failed to load zone: " << cpp_strerror(ret) << std::endl;
          return -ret;
        }

        string orig_id = zone.get_id();

	ret = read_decode_json(infile, zone);
	if (ret < 0) {
	  return 1;
	}

	if (zone.realm_id.empty()) {
	  RGWRealm realm;
          ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                realm_id, realm_name, realm);
	  if (ret < 0 && ret != -ENOENT) {
	    cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
	    return -ret;
	  }
	  zone.realm_id = realm.get_id();
          cerr << "NOTICE: set zone's realm_id=" << zone.realm_id << std::endl;
	}

	if (!zone_name.empty() && !zone.get_name().empty() && zone.get_name() != zone_name) {
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

        constexpr bool exclusive = false;
        ret = rgw::create_zone(dpp(), null_yield, cfgstore.get(),
                               exclusive, zone);
	if (ret < 0) {
	  cerr << "ERROR: couldn't create zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = rgw::set_default_zone(dpp(), null_yield, cfgstore.get(), zone);
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
        RGWZoneParams default_zone_params;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 {}, {}, default_zone_params);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "could not determine default zone: " << cpp_strerror(-ret) << std::endl;
	}

        Formatter::ObjectSection zones_list{*formatter, "zones_list"};
        encode_json("default_info", default_zone_params.id, formatter.get());

        Formatter::ArraySection zones{*formatter, "zones"};
        rgw::sal::ListResult<std::string> listing;
        std::array<std::string, 1000> names; // list in pages of 1000
        do {
          ret = cfgstore->list_zone_names(dpp(), null_yield, listing.next,
                                          names, listing);
          if (ret < 0) {
            std::cerr << "failed to list zones: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
          for (const auto& name : listing.entries) {
            encode_json("name", name, formatter.get());
          }
        } while (!listing.next.empty());
      } // close sections zones and zones_list
      formatter->flush(cout);
      break;
    case OPT::ZONE_MODIFY:
      {
	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone_params, &zone_writer);
        if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        bool need_zone_update = false;
        if (!access_key.empty()) {
          zone_params.system_key.id = access_key;
          need_zone_update = true;
        }

        if (!secret_key.empty()) {
          zone_params.system_key.key = secret_key;
          need_zone_update = true;
        }

        if (!realm_id.empty()) {
          zone_params.realm_id = realm_id;
          need_zone_update = true;
        } else if (!realm_name.empty()) {
          // get realm id from name
          ret = cfgstore->read_realm_id(dpp(), null_yield,
                                        realm_name, zone_params.realm_id);
          if (ret < 0) {
            cerr << "failed to find realm by name " << realm_name << std::endl;
            return -ret;
          }
          need_zone_update = true;
        }

        for (const auto& add : tier_config_add) {
          ret = zone_params.tier_config.set(add.first, add.second);
          if (ret < 0) {
            cerr << "ERROR: failed to set configurable: " << add << std::endl;
            return EINVAL;
          }
          need_zone_update = true;
        }

        for (const auto& rm : tier_config_rm) {
          if (!rm.first.empty()) { /* otherwise will remove the entire config */
            zone_params.tier_config.erase(rm.first);
            need_zone_update = true;
          }
        }

        if (need_zone_update) {
          ret = zone_writer->write(dpp(), null_yield, zone_params);
          if (ret < 0) {
            cerr << "failed to save zone info: " << cpp_strerror(-ret) << std::endl;
            return -ret;
          }
        }

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                  zonegroup_id, zonegroup_name,
                                  zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "failed to load zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        const bool *pis_master = (is_master_set ? &is_master : nullptr);
        const bool *pread_only = (is_read_only_set ? &read_only : nullptr);
        const bool *psync_from_all = (sync_from_all_specified ? &sync_from_all : nullptr);
        const string *predirect_zone = (redirect_zone_set ? &redirect_zone : nullptr);

        // validate --tier-type if specified
        const string *ptier_type = (tier_type_specified ? &tier_type : nullptr);
        if (ptier_type) {
          auto sync_mgr = static_cast<rgw::sal::RadosStore*>(driver)->svc()->sync_modules->get_manager();
          if (!sync_mgr->get_module(*ptier_type, nullptr)) {
            ldpp_dout(dpp(), -1) << "ERROR: could not find sync module: "
                << *ptier_type << ",  valid sync modules: "
                << sync_mgr->get_registered_module_names() << dendl;
            return EINVAL;
          }
        }

        if (enable_features.empty()) { // enable all features by default
          enable_features.insert(rgw::zone_features::supported.begin(),
                                 rgw::zone_features::supported.end());
        }

        // add/update the public zone information stored in the zonegroup
        ret = rgw::add_zone_to_group(dpp(), zonegroup, zone_params,
                                     pis_master, pread_only, endpoints,
                                     ptier_type, psync_from_all,
                                     sync_from, sync_from_rm,
                                     predirect_zone, bucket_index_max_shards,
                                     enable_features, disable_features);
        if (ret < 0) {
          return -ret;
        }

        // write the updated zonegroup
        ret = zonegroup_writer->write(dpp(), null_yield, zonegroup);
	if (ret < 0) {
	  cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (set_default) {
          ret = rgw::set_default_zone(dpp(), null_yield, cfgstore.get(),
                                      zone_params);
          if (ret < 0) {
            cerr << "failed to set zone " << zone_name << " as default: " << cpp_strerror(-ret) << std::endl;
          }
        }

        encode_json("zone", zone_params, formatter.get());
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

	RGWZoneParams zone_params;
        std::unique_ptr<rgw::sal::ZoneWriter> zone_writer;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone_params, &zone_writer);
	if (ret < 0) {
	  cerr << "failed to load zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

	ret = zone_writer->rename(dpp(), null_yield, zone_params, zone_new_name);
	if (ret < 0) {
	  cerr << "failed to rename zone " << zone_name << " to " << zone_new_name << ": " << cpp_strerror(-ret)
	       << std::endl;
	  return -ret;
	}

	RGWZoneGroup zonegroup;
        std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;
        ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                  zonegroup_id, zonegroup_name,
                                  zonegroup, &zonegroup_writer);
	if (ret < 0) {
	  cerr << "WARNING: failed to load zonegroup " << zonegroup_name << std::endl;
          return EXIT_SUCCESS;
	}

        auto z = zonegroup.zones.find(zone_params.id);
        if (z == zonegroup.zones.end()) {
          return EXIT_SUCCESS;
        }
        z->second.name = zone_params.name;

        ret = zonegroup_writer->write(dpp(), null_yield, zonegroup);
        if (ret < 0) {
          cerr << "Error in zonegroup rename for " << zone_name << ": " << cpp_strerror(-ret) << std::endl;
          return -ret;
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

	RGWZoneParams zone;
        std::unique_ptr<rgw::sal::ZoneWriter> writer;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone, &writer);
        if (ret < 0) {
	  cerr << "failed to init zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}

        if (opt_cmd == OPT::ZONE_PLACEMENT_ADD ||
	    opt_cmd == OPT::ZONE_PLACEMENT_MODIFY) {
	  RGWZoneGroup zonegroup;
          ret = rgw::read_zonegroup(dpp(), null_yield, cfgstore.get(),
                                    zonegroup_id, zonegroup_name, zonegroup);
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
          if (placement_inline_data_specified) {
            info.inline_data = placement_inline_data;
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

        ret = writer->write(dpp(), null_yield, zone);
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
	RGWZoneParams zone;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone);
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

	RGWZoneParams zone;
        int ret = rgw::read_zone(dpp(), null_yield, cfgstore.get(),
                                 zone_id, zone_name, zone);
	if (ret < 0) {
	  cerr << "unable to initialize zone: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
	}
	auto p = zone.placement_pools.find(placement_id);
	if (p == zone.placement_pools.end()) {
	  cerr << "ERROR: zone placement target '" << placement_id << "' not found" << std::endl;
	  return ENOENT;
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

  bool non_master_cmd = (!driver->is_meta_master() && !yes_i_really_mean_it);
  std::set<OPT> non_master_ops_list = {OPT::USER_CREATE, OPT::USER_RM, 
                                        OPT::USER_MODIFY, OPT::USER_ENABLE,
                                        OPT::USER_SUSPEND, OPT::SUBUSER_CREATE,
                                        OPT::SUBUSER_MODIFY, OPT::SUBUSER_RM,
                                        OPT::BUCKET_LINK, OPT::BUCKET_UNLINK,
                                        OPT::BUCKET_RM,
                                        OPT::BUCKET_CHOWN, OPT::METADATA_PUT,
                                        OPT::METADATA_RM, OPT::MFA_CREATE,
                                        OPT::MFA_REMOVE, OPT::MFA_RESYNC,
                                        OPT::CAPS_ADD, OPT::CAPS_RM,
                                        OPT::ROLE_CREATE, OPT::ROLE_DELETE,
                                        OPT::ROLE_POLICY_PUT, OPT::ROLE_POLICY_DELETE};

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

  if (key_active_specified) {
    user_op.access_key_active = key_active;
  }

  // set suspension operation parameters
  if (opt_cmd == OPT::USER_ENABLE)
    user_op.set_suspension(false);
  else if (opt_cmd == OPT::USER_SUSPEND)
    user_op.set_suspension(true);

  if (!placement_id.empty()) {
    rgw_placement_rule target_rule;
    target_rule.name = placement_id;
    target_rule.storage_class = opt_storage_class.value_or("");
    if (!driver->valid_placement(target_rule)) {
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
    ret = ruser.init(dpp(), driver, user_op, null_yield);
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
  bucket_op.set_min_age(min_age);
  bucket_op.set_dump_keys(dump_keys);
  bucket_op.set_hide_progress(hide_progress);

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
      RGWPeriod period;
      int ret = cfgstore->read_period(dpp(), null_yield, period_id,
                                      std::nullopt, period);
      if (ret < 0) {
        cerr << "failed to load period: " << cpp_strerror(-ret) << std::endl;
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
      int ret = update_period(cfgstore.get(), realm_id, realm_name,
                              period_epoch, commit, remote, url,
                              opt_region, access_key, secret_key,
                              formatter.get(), yes_i_really_mean_it);
      if (ret < 0) {
	return -ret;
      }
    }
    return 0;
  case OPT::PERIOD_COMMIT:
    {
      // read realm and staging period
      RGWRealm realm;
      std::unique_ptr<rgw::sal::RealmWriter> realm_writer;
      int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                realm_id, realm_name,
                                realm, &realm_writer);
      if (ret < 0) {
        cerr << "Error initializing realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      period_id = rgw::get_staging_period_id(realm.id);
      epoch_t epoch = 1;

      RGWPeriod period;
      ret = cfgstore->read_period(dpp(), null_yield, period_id, epoch, period);
      if (ret < 0) {
        cerr << "failed to load period: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      ret = commit_period(cfgstore.get(), realm, *realm_writer, period,
                          remote, url, opt_region, access_key, secret_key,
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
        const rgw::IAM::Policy p(
	  g_ceph_context, tenant, bl,
	  g_ceph_context->_conf.get_val<bool>(
	    "rgw_policy_reject_invalid_principals"));
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse policy: " << e.what() << std::endl;
        return -EINVAL;
      }
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant, path,
                                                                 assume_role_doc, max_session_duration);
      ret = role->create(dpp(), true, "", null_yield);
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
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
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
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      show_role_info(role.get(), formatter.get());
      return 0;
    }
  case OPT::ROLE_TRUST_POLICY_MODIFY:
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
        const rgw::IAM::Policy p(g_ceph_context, tenant, bl,
				 g_ceph_context->_conf.get_val<bool>(
				   "rgw_policy_reject_invalid_principals"));
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse policy: " << e.what() << std::endl;
        return -EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
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
      ret = driver->get_roles(dpp(), null_yield, path_prefix, tenant, result);
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

      if (perm_policy_doc.empty() && infile.empty()) {
        cerr << "permission policy document is empty" << std::endl;
        return -EINVAL;
      }

      bufferlist bl;
      if (!infile.empty()) {
        int ret = read_input(infile, bl);
        if (ret < 0) {
          cerr << "ERROR: failed to read input policy document: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        perm_policy_doc = bl.to_str();
      } else {
        bl = bufferlist::static_from_string(perm_policy_doc);
      }
      try {
        const rgw::IAM::Policy p(g_ceph_context, tenant, bl,
				 g_ceph_context->_conf.get_val<bool>(
				   "rgw_policy_reject_invalid_principals"));
      } catch (rgw::IAM::PolicyParseException& e) {
        cerr << "failed to parse perm policy: " << e.what() << std::endl;
        return -EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
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
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
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
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
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
      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
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
  case OPT::ROLE_UPDATE:
    {
      if (role_name.empty()) {
        cerr << "ERROR: role name is empty" << std::endl;
        return -EINVAL;
      }

      std::unique_ptr<rgw::sal::RGWRole> role = driver->get_role(role_name, tenant);
      ret = role->get(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      role->update_max_session_duration(max_session_duration);
      if (!role->validate_max_session_duration(dpp())) {
        ret = -EINVAL;
        return ret;
      }
      ret = role->update(dpp(), null_yield);
      if (ret < 0) {
        return -ret;
      }
      cout << "Max session duration updated successfully for role: " << role_name << std::endl;
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
      int ret = RGWBucketAdminOp::dump_s3_policy(driver, bucket_op, cout, dpp(), null_yield);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      int ret = RGWBucketAdminOp::get_policy(driver, bucket_op, stream_flusher, dpp(), null_yield);
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
	RGWBucketAdminOp::limit_check(driver, bucket_op, user_ids, stream_flusher,
				      null_yield, dpp(), warnings_only);
    } else {
      /* list users in groups of max-keys, then perform user-bucket
       * limit-check on each group */
     ret = driver->meta_list_keys_init(dpp(), metadata_key, string(), &handle);
      if (ret < 0) {
	cerr << "ERROR: buckets limit check can't get user metadata_key: "
	     << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      do {
	ret = driver->meta_list_keys_next(dpp(), handle, max, user_ids,
					      &truncated);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "ERROR: buckets limit check lists_keys_next(): "
	       << cpp_strerror(-ret) << std::endl;
	  break;
	} else {
	  /* ok, do the limit checks for this group */
	  ret =
	    RGWBucketAdminOp::limit_check(driver, bucket_op, user_ids, stream_flusher,
					  null_yield, dpp(), warnings_only);
	  if (ret < 0)
	    break;
	}
	user_ids.clear();
      } while (truncated);
      driver->meta_list_keys_complete(handle);
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
      bucket_op.marker = marker;
      RGWBucketAdminOp::info(driver, bucket_op, stream_flusher, null_yield, dpp());
    } else {
      int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
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
          cerr << "ERROR: driver->list_objects(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
	ldpp_dout(dpp(), 20) << "INFO: " << __func__ <<
	  ": list() returned without error; results.objs.sizie()=" <<
	  results.objs.size() << "results.is_truncated=" << results.is_truncated << ", marker=" <<
	  params.marker << dendl;

        count += results.objs.size();

        for (const auto& entry : results.objs) {
          encode_json("entry", entry, formatter.get());
        }
        formatter->flush(cout);
      } while (results.is_truncated && count < max_entries);
      ldpp_dout(dpp(), 20) << "INFO: " << __func__ << ": done" << dendl;

      formatter->close_section();
      formatter->flush(cout);
    } /* have bucket_name */
  } /* OPT::BUCKETS_LIST */

  if (opt_cmd == OPT::BUCKET_RADOS_LIST) {
    RGWRadosList lister(static_cast<rgw::sal::RadosStore*>(driver),
			max_concurrent_ios, orphan_stale_secs, tenant);
    if (rgw_obj_fs) {
      lister.set_field_separator(*rgw_obj_fs);
    }

    if (bucket_name.empty()) {
      // yes_i_really_mean_it means continue with listing even if
      // there are indexless buckets
      ret = lister.run(dpp(), yes_i_really_mean_it);
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

  if (opt_cmd == OPT::BUCKET_LAYOUT) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    const auto& bucket_info = bucket->get_info();
    formatter->open_object_section("layout");
    encode_json("layout", bucket_info.layout, formatter.get());
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BUCKET_STATS) {
    if (bucket_name.empty() && !bucket_id.empty()) {
      rgw_bucket bucket;
      if (!rgw_find_bucket_by_id(dpp(), driver->ctx(), driver, marker, bucket_id, &bucket)) {
        cerr << "failure: no such bucket id" << std::endl;
        return -ENOENT;
      }
      bucket_op.set_tenant(bucket.tenant);
      bucket_op.set_bucket_name(bucket.name);
    }
    bucket_op.set_fetch_stats(true);

    int r = RGWBucketAdminOp::info(driver, bucket_op, stream_flusher, null_yield, dpp());
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return posix_errortrans(-r);
    }
  }

  if (opt_cmd == OPT::BUCKET_LINK) {
    bucket_op.set_bucket_id(bucket_id);
    bucket_op.set_new_bucket_name(new_bucket_name);
    string err;
    int r = RGWBucketAdminOp::link(driver, bucket_op, dpp(), null_yield, &err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << err << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT::BUCKET_UNLINK) {
    int r = RGWBucketAdminOp::unlink(driver, bucket_op, dpp(), null_yield);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }

  if (opt_cmd == OPT::BUCKET_SHARD_OBJECTS) {
    const auto prefix = opt_prefix ? *opt_prefix : "obj"s;
    if (!num_shards_specified) {
      cerr << "ERROR: num-shards must be specified."
	   << std::endl;
      return EINVAL;
    }

    if (specified_shard_id) {
      if (shard_id >= num_shards) {
	cerr << "ERROR: shard-id must be less than num-shards."
	     << std::endl;
	return EINVAL;
      }
      std::string obj;
      uint64_t ctr = 0;
      int shard;
      do {
	obj = fmt::format("{}{:0>20}", prefix, ctr);
	shard = RGWSI_BucketIndex_RADOS::bucket_shard_index(obj, num_shards);
	++ctr;
      } while (shard != shard_id);

      formatter->open_object_section("shard_obj");
      encode_json("obj", obj, formatter.get());
      formatter->close_section();
      formatter->flush(cout);
    } else {
      std::vector<std::string> objs(num_shards);
      for (uint64_t ctr = 0, shardsleft = num_shards; shardsleft > 0; ++ctr) {
	auto key = fmt::format("{}{:0>20}", prefix, ctr);
	auto shard = RGWSI_BucketIndex_RADOS::bucket_shard_index(key, num_shards);
	if (objs[shard].empty()) {
	  objs[shard] = std::move(key);
	  --shardsleft;
	}
      }

      formatter->open_object_section("shard_objs");
      encode_json("objs", objs, formatter.get());
      formatter->close_section();
      formatter->flush(cout);
    }
  }

  if (opt_cmd == OPT::BUCKET_OBJECT_SHARD) {
    if (!num_shards_specified || object.empty()) {
      cerr << "ERROR: num-shards and object must be specified."
	   << std::endl;
      return EINVAL;
    }
    auto shard = RGWSI_BucketIndex_RADOS::bucket_shard_index(object, num_shards);
    formatter->open_object_section("obj_shard");
    encode_json("shard", shard, formatter.get());
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BUCKET_RESYNC_ENCRYPTED_MULTIPART) {
    // repair logic for replication of encrypted multipart uploads:
    // https://tracker.ceph.com/issues/46062
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    auto rados_driver = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!rados_driver) {
      cerr << "ERROR: this command can only work when the cluster "
          "has a RADOS backing store." << std::endl;
      return EPERM;
    }

    // fail if recovery wouldn't generate replication log entries
    if (!rados_driver->svc()->zone->need_to_log_data() && !yes_i_really_mean_it) {
      cerr << "This command is only necessary for replicated buckets." << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EPERM;
    }

    formatter->open_object_section("modified");
    encode_json("bucket", bucket->get_name(), formatter.get());
    encode_json("bucket_id", bucket->get_bucket_id(), formatter.get());

    ret = rados_driver->getRados()->bucket_resync_encrypted_multipart(
        dpp(), null_yield, rados_driver, bucket->get_info(),
        marker, stream_flusher);
    if (ret < 0) {
      return -ret;
    }
    formatter->close_section();
    formatter->flush(cout);
    return 0;
  }

  if (opt_cmd == OPT::BUCKET_CHOWN) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }

    bucket_op.set_bucket_name(bucket_name);
    bucket_op.set_new_bucket_name(new_bucket_name);
    string err;

    int r = RGWBucketAdminOp::chown(driver, bucket_op, marker, dpp(), null_yield, &err);
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
    int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->log_list_init(dpp(), date, &h);
    if (r == -ENOENT) {
      // no logs.
    } else {
      if (r < 0) {
        cerr << "log list: error " << r << std::endl;
        return -r;
      }
      while (true) {
        string name;
        int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->log_list_next(h, &name);
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

      int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->log_show_init(dpp(), oid, &h);
      if (r < 0) {
	cerr << "error opening log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }

      formatter->reset();
      formatter->open_object_section("log");

      struct rgw_log_entry entry;

      // peek at first entry to get bucket metadata
      r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->log_show_next(dpp(), h, &entry);
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
	r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->log_show_next(dpp(), h, &entry);
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
      int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->log_remove(dpp(), oid);
      if (r < 0) {
	cerr << "error removing log " << oid << ": " << cpp_strerror(-r) << std::endl;
	return -r;
      }
    }
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
      int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
	cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }
    ret = RGWUsage::show(dpp(), driver, user.get(), bucket.get(), start_epoch,
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
      int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
      if (ret < 0) {
	cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
	return -ret;
      }
    }
    ret = RGWUsage::trim(dpp(), driver, user.get(), bucket.get(), start_epoch, end_epoch, null_yield);
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

    ret = RGWUsage::clear(dpp(), driver, null_yield);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    RGWOLHInfo olh;
    rgw_obj obj(bucket->get_key(), object);
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_olh(dpp(), bucket->get_info(), obj, &olh, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed reading olh: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("olh", olh, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::OLH_READLOG) {
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<uint64_t, vector<rgw_bucket_olh_log_entry> > log;
    bool is_truncated;

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);

    RGWObjState *state;

    ret = obj->get_obj_state(dpp(), &state, null_yield);
    if (ret < 0) {
      return -ret;
    }

    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bucket_index_read_olh_log(dpp(), bucket->get_info(), *state, obj->get_obj(), 0, &log, &is_truncated, null_yield);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket->get_key(), object);
    if (!object_version.empty()) {
      obj.key.set_instance(object_version);
    }

    rgw_cls_bi_entry entry;
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_get(dpp(), bucket->get_info(), obj, bi_index_type, &entry, null_yield);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
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

    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_put(dpp(), bucket->get_key(), obj, entry, null_yield);
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

    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      ldpp_dout(dpp(), 0) << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
	dendl;
      return -ret;
    }

    std::list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    const auto& index = bucket->get_info().layout.current_index;
    const int max_shards = rgw::num_shards(index);
    if (max_entries < 0) {
      max_entries = 1000;
    }

    ldpp_dout(dpp(), 20) << "INFO: " << __func__ << ": max_entries=" << max_entries <<
      ", index=" << index << ", max_shards=" << max_shards << dendl;

    formatter->open_array_section("entries");

    int i = (specified_shard_id ? shard_id : 0);
    for (; i < max_shards; i++) {
      ldpp_dout(dpp(), 20) << "INFO: " << __func__ << ": starting shard=" << i << dendl;
      marker.clear();

      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(driver)->getRados());
      int ret = bs.init(dpp(), bucket->get_info(), index, i, null_yield);
      if (ret < 0) {
	ldpp_dout(dpp(), 0) << "ERROR: bs.init(bucket=" << bucket << ", shard=" << i <<
	  "): " << cpp_strerror(-ret) << dendl;
        return -ret;
      }

      do {
        entries.clear();
	// if object is specified, we use that as a filter to only retrieve some entries
        ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_list(bs, object, marker, max_entries, &entries, &is_truncated, null_yield);
        if (ret < 0) {
          ldpp_dout(dpp(), 0) << "ERROR: bi_list(): " << cpp_strerror(-ret) << dendl;
          return -ret;
        }
	ldpp_dout(dpp(), 20) << "INFO: " << __func__ <<
	  ": bi_list() returned without error; entries.size()=" <<
	  entries.size() << ", is_truncated=" << is_truncated <<
	  ", marker=" << marker << dendl;

	for (const auto& entry : entries) {
          encode_json("entry", entry, formatter.get());
          marker = entry.idx;
        }
        formatter->flush(cout);
      } while (is_truncated);

      formatter->flush(cout);

      if (specified_shard_id) {
        break;
      }
    }
    ldpp_dout(dpp(), 20) << "INFO: " << __func__ << ": done" << dendl;

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BI_PURGE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Bucket> cur_bucket;
    ret = init_bucket(tenant, bucket_name, string(), &cur_bucket);
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

    const auto& index = bucket->get_info().layout.current_index;
    if (index.layout.type == rgw::BucketIndexType::Indexless) {
      cerr << "ERROR: indexless bucket has no index to purge" << std::endl;
      return EINVAL;
    }

    const int max_shards = rgw::num_shards(index);
    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(driver)->getRados());
      int ret = bs.init(dpp(), bucket->get_info(), index, i, null_yield);
      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << i << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_remove(dpp(), bs);
      if (ret < 0) {
        cerr << "ERROR: failed to remove bucket index object: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
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

    RGWDataAccess data_access(driver);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj_key key(object, object_version);
    ret = rgw_remove_object(dpp(), driver, bucket.get(), key, null_yield);

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

    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);
    bool need_rewrite = true;
    if (min_rewrite_stripe_size > 0) {
      ret = check_min_obj_stripe_size(driver, obj.get(), min_rewrite_stripe_size, &need_rewrite);
      if (ret < 0) {
        ldpp_dout(dpp(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << ret << dendl;
      }
    }
    if (need_rewrite) {
      RGWRados* store = static_cast<rgw::sal::RadosStore*>(driver)->getRados();
      ret = store->rewrite_obj(bucket->get_info(), obj->get_obj(), dpp(), null_yield);
      if (ret < 0) {
        cerr << "ERROR: object rewrite returned: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ldpp_dout(dpp(), 20) << "skipped object" << dendl;
    }
  } // OPT::OBJECT_REWRITE

  if (opt_cmd == OPT::OBJECT_REINDEX) {
    if (bucket_name.empty()) {
      cerr << "ERROR: --bucket not specified." << std::endl;
      return EINVAL;
    }
    if (object.empty() && objects_file.empty()) {
      cerr << "ERROR: neither --object nor --objects-file specified." << std::endl;
      return EINVAL;
    } else if (!object.empty() && !objects_file.empty()) {
      cerr << "ERROR: both --object and --objects-file specified and only one is allowed." << std::endl;
      return EINVAL;
    } else if (!objects_file.empty() && !object_version.empty()) {
      cerr << "ERROR: cannot specify --object_version when --objects-file specified." << std::endl;
      return EINVAL;
    }

    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
	"." << std::endl;
      return -ret;
    }

    rgw::sal::RadosStore* rados_store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!rados_store) {
      cerr <<
	"ERROR: this command can only work when the cluster has a RADOS backing store." <<
	std::endl;
      return EPERM;
    }
    RGWRados* store = rados_store->getRados();

    auto process = [&](const std::string& p_object, const std::string& p_object_version) -> int {
      std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(p_object);
      obj->set_instance(p_object_version);
      ret = store->reindex_obj(driver, bucket->get_info(), obj->get_obj(), dpp(), null_yield);
      if (ret < 0) {
	return ret;
      }
      return 0;
    };

    if (!object.empty()) {
      ret = process(object, object_version);
      if (ret < 0) {
	return -ret;
      }
    } else {
      std::ifstream file;
      file.open(objects_file);
      if (!file.is_open()) {
	std::cerr << "ERROR: unable to open objects-file \"" <<
	  objects_file << "\"." << std::endl;
	return ENOENT;
      }

      std::string obj_name;
      while (std::getline(file, obj_name)) {
	std::string version;
	auto pos = obj_name.find('\t');
	if (pos != std::string::npos) {
	  version = obj_name.substr(1 + pos);
	  obj_name = obj_name.substr(0, pos);
	}

	ret = process(obj_name, version);
	if (ret < 0) {
	  std::cerr << "ERROR: while processing \"" << obj_name <<
	    "\", received " << cpp_strerror(-ret) << "." << std::endl;
	  if (!yes_i_really_mean_it) {
	    std::cerr <<
	      "NOTE: with *caution* you can use --yes-i-really-mean-it to push through errors and continue processing." <<
	      std::endl;
	    return -ret;
	  }
	}
      } // while
    }
  } // OPT::OBJECT_REINDEX

  if (opt_cmd == OPT::OBJECTS_EXPIRE) {
    if (!static_cast<rgw::sal::RadosStore*>(driver)->getRados()->process_expire_objects(dpp(), null_yield)) {
      cerr << "ERROR: process_expire_objects() processing returned error." << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::OBJECTS_EXPIRE_STALE_LIST) {
    ret = RGWBucketAdminOp::fix_obj_expiry(driver, bucket_op, stream_flusher, dpp(), null_yield, true);
    if (ret < 0) {
      cerr << "ERROR: listing returned " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::OBJECTS_EXPIRE_STALE_RM) {
    ret = RGWBucketAdminOp::fix_obj_expiry(driver, bucket_op, stream_flusher, dpp(), null_yield, false);
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

    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
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

      const auto& current_index = bucket->get_info().layout.current_index;
      int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->cls_bucket_list_ordered(
	dpp(), bucket->get_info(), current_index, RGW_NO_SHARD,
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
            r = check_min_obj_stripe_size(driver, obj.get(), min_rewrite_stripe_size, &need_rewrite);
            if (r < 0) {
              ldpp_dout(dpp(), 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
            }
          }
          if (!need_rewrite) {
            formatter->dump_string("status", "Skipped");
          } else {
            RGWRados* store = static_cast<rgw::sal::RadosStore*>(driver)->getRados();
            r = store->rewrite_obj(bucket->get_info(), obj->get_obj(), dpp(), null_yield);
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
    int ret = check_reshard_bucket_params(driver,
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

    auto zone_svc = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone;
    if (!zone_svc->can_reshard()) {
      const auto& zonegroup = zone_svc->get_zonegroup();
      std::cerr << "The zonegroup '" << zonegroup.get_name() << "' does not "
          "have the resharding feature enabled." << std::endl;
      return ENOTSUP;
    }

    if (!RGWBucketReshard::should_zone_reshard_now(bucket->get_info(), zone_svc) &&
        !yes_i_really_mean_it) {
      std::cerr << "Bucket '" << bucket->get_name() << "' already has too many "
          "log generations (" << bucket->get_info().layout.logs.size() << ") "
          "from previous reshards that peer zones haven't finished syncing. "
          "Resharding is not recommended until the old generations sync, but "
          "you can force a reshard with --yes-i-really-mean-it." << std::endl;
      return EINVAL;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(driver),
			bucket->get_info(), bucket->get_attrs(),
			nullptr /* no callback */);

#define DEFAULT_RESHARD_MAX_ENTRIES 1000
    if (max_entries < 1) {
      max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
    }

    ReshardFaultInjector fault;
    if (inject_error_at) {
      const int code = -inject_error_code.value_or(EIO);
      fault.inject(*inject_error_at, InjectError{code, dpp()});
    } else if (inject_abort_at) {
      fault.inject(*inject_abort_at, InjectAbort{});
    } else if (inject_delay_at) {
      fault.inject(*inject_delay_at, InjectDelay{inject_delay, dpp()});
    }
    ret = br.execute(num_shards, fault, max_entries, dpp(), null_yield,
                     verbose, &cout, formatter.get());
    return -ret;
  }

  if (opt_cmd == OPT::RESHARD_ADD) {
    int ret = check_reshard_bucket_params(driver,
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

    int num_source_shards = rgw::current_num_shards(bucket->get_info().layout);

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), dpp());
    cls_rgw_reshard_entry entry;
    entry.time = real_clock::now();
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    entry.bucket_id = bucket->get_info().bucket.bucket_id;
    entry.old_num_shards = num_source_shards;
    entry.new_num_shards = num_shards;

    return reshard.add(dpp(), entry, null_yield);
  }

  if (opt_cmd == OPT::RESHARD_LIST) {
    int ret;
    int count = 0;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int num_logshards =
      driver->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), dpp());

    formatter->open_array_section("reshard");
    for (int i = 0; i < num_logshards; i++) {
      bool is_truncated = true;
      std::string marker;
      do {
	std::list<cls_rgw_reshard_entry> entries;
        ret = reshard.list(dpp(), i, marker, max_entries - count, entries, &is_truncated);
        if (ret < 0) {
          cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
          return ret;
        }
        for (const auto& entry : entries) {
          encode_json("entry", entry, formatter.get());
        }
	if (is_truncated) {
	  entries.crbegin()->get_key(&marker); // last entry's key becomes marker
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

    ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(driver),
			bucket->get_info(), bucket->get_attrs(),
			nullptr /* no callback */);
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
    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), true, &cout);

    int ret = reshard.process_all_logshards(dpp(), null_yield);
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
    ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
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

    bool resharding_underway = true;

    if (bucket_initable) {
      // we did not encounter an error, so let's work with the bucket
	RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(driver),
			    bucket->get_info(), bucket->get_attrs(),
			    nullptr /* no callback */);
      int ret = br.cancel(dpp(), null_yield);
      if (ret < 0) {
        if (ret == -EBUSY) {
          cerr << "There is ongoing resharding, please retry after " <<
            driver->ctx()->_conf.get_val<uint64_t>("rgw_reshard_bucket_lock_duration") <<
            " seconds." << std::endl;
	  return -ret;
	} else if (ret == -EINVAL) {
	  resharding_underway = false;
	  // we can continue and try to unschedule
        } else {
          cerr << "Error cancelling bucket \"" << bucket_name <<
            "\" resharding: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
        }
      }
    }

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), dpp());

    cls_rgw_reshard_entry entry;
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;

    ret = reshard.remove(dpp(), entry, null_yield);
    if (ret == -ENOENT) {
      if (!resharding_underway) {
	cerr << "Error, bucket \"" << bucket_name <<
	  "\" is neither undergoing resharding nor scheduled to undergo "
	  "resharding." << std::endl;
	return EINVAL;
      } else {
	// we cancelled underway resharding above, so we're good
	return 0;
      }
    } else if (ret < 0) {
      cerr << "Error in updating reshard log with bucket \"" <<
        bucket_name << "\": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } // OPT_RESHARD_CANCEL

  if (opt_cmd == OPT::OBJECT_UNLINK) {
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<rgw_obj_index_key> oid_list;
    rgw_obj_key key(object, object_version);
    rgw_obj_index_key index_key;
    key.get_index_key(&index_key);
    oid_list.push_back(index_key);

    // note: under rados this removes directly from rados index objects
    ret = bucket->remove_objs_from_index(dpp(), oid_list);
    if (ret < 0) {
      cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::OBJECT_STAT) {
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);

    ret = obj->get_obj_attrs(null_yield, dpp());
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
      } else if (iter->first == RGW_ATTR_TORRENT) {
        // contains bencoded binary data which shouldn't be output directly
        // TODO: decode torrent info for display as json?
        formatter->dump_string("torrent", "<contains binary data>");
        handled = true;
      } else if (iter->first == RGW_ATTR_PG_VER) {
        handled = decode_dump<uint64_t>("pg_ver", bl, formatter.get());
      } else if (iter->first == RGW_ATTR_SOURCE_ZONE) {
        handled = decode_dump<uint32_t>("source_zone", bl, formatter.get());
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
  } // OPT::OBJECT_STAT

  if (opt_cmd == OPT::OBJECT_MANIFEST) {
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
	std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(object);
    obj->set_instance(object_version);

    ret = obj->get_obj_attrs(null_yield, dpp());
    if (ret < 0) {
      cerr << "ERROR: failed to retrieve object metadata, returned error: " <<
	cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    formatter->open_object_section("outer");  // name not displayed since top level
    formatter->dump_unsigned("size", obj->get_obj_size());

    auto attr_iter = obj->get_attrs().find(RGW_ATTR_MANIFEST);
    if (attr_iter == obj->get_attrs().end()) {
      cerr << "ERROR: unable to find object manifest" << std::endl;
      return ENOENT;
    }

    RGWObjManifest m;
    try {
      auto part_iter = attr_iter->second.cbegin();
      decode(m, part_iter);
    } catch (buffer::error& err) {
      cerr << "ERROR: unable to decode manifest" << std::endl;
      return EIO;
    }

    rgw::sal::RadosStore* store =
      dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr << "ERROR: this command (currently) only works with "
	"RADOS back-ends" << std::endl;
      return EINVAL;
    }

    RGWRados* rados = store->getRados();

    rgw_obj head_obj = obj->get_obj();
    rgw_raw_obj raw_head_obj;
    store->get_raw_obj(m.get_head_placement_rule(), head_obj, &raw_head_obj);
    
    formatter->open_array_section("objects");
    unsigned index = 0;
    for (auto p = m.obj_begin(dpp()); p != m.obj_end(dpp()); ++p, ++index) {
      rgw_raw_obj raw_obj =  p.get_location().get_raw_obj(rados);

      if (index == 0 && raw_obj != raw_head_obj) {
	// we have a head object without data, so let's include it
	formatter->open_object_section("object"); // name not displayed since in array

	formatter->dump_int("index", -1);
	formatter->dump_unsigned("offset", 0);
	formatter->dump_unsigned("size", 0);
	
	formatter->open_object_section("raw_obj");
	raw_head_obj.dump(formatter.get());
	formatter->close_section(); // raw_obj

	formatter->close_section(); // object
      }

      formatter->open_object_section("object"); // name not displayed since in array

      formatter->dump_unsigned("index", index);
      formatter->dump_unsigned("part_id", p.get_cur_part_id());
      formatter->dump_unsigned("stripe_id", p.get_cur_stripe());
      formatter->dump_unsigned("offset", p.get_ofs());
      formatter->dump_unsigned("size", p.get_stripe_size());

      formatter->open_object_section("raw_obj");
      raw_obj.dump(formatter.get());
      formatter->close_section(); // raw_obj

      formatter->close_section(); // object
    }
    formatter->close_section(); // objects array

    formatter->close_section(); // outer
    formatter->flush(cout);
  } // OPT::OBJECT_MANIFEST

  if (opt_cmd == OPT::BUCKET_CHECK) {
    if (check_head_obj_locator) {
      if (bucket_name.empty()) {
        cerr << "ERROR: need to specify bucket name" << std::endl;
        return EINVAL;
      }
      do_check_object_locator(tenant, bucket_name, fix, remove_bad, formatter.get());
    } else {
      RGWBucketAdminOp::check_index(driver, bucket_op, stream_flusher, null_yield, dpp());
    }
  }

  if (opt_cmd == OPT::BUCKET_CHECK_OLH) {
    rgw::sal::RadosStore* store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr <<
	      "WARNING: this command is only relevant when the cluster has a RADOS backing store." <<
	      std::endl;
      return 0;
    }
    RGWBucketAdminOp::check_index_olh(store, bucket_op, stream_flusher, dpp());
  }

  if (opt_cmd == OPT::BUCKET_CHECK_UNLINKED) {
    rgw::sal::RadosStore* store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr <<
	      "WARNING: this command is only relevant when the cluster has a RADOS backing store." <<
	      std::endl;
      return 0;
    }
    RGWBucketAdminOp::check_index_unlinked(store, bucket_op, stream_flusher, dpp());
  }

  if (opt_cmd == OPT::BUCKET_RM) {
    if (!inconsistent_index) {
      RGWBucketAdminOp::remove_bucket(driver, bucket_op, null_yield, dpp(), bypass_gc, true);
    } else {
      if (!yes_i_really_mean_it) {
	cerr << "using --inconsistent_index can corrupt the bucket index " << std::endl
	<< "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
	return 1;
      }
      RGWBucketAdminOp::remove_bucket(driver, bucket_op, null_yield, dpp(), bypass_gc, false);
    }
  }

  if (opt_cmd == OPT::GC_LIST) {
    int index = 0;
    bool truncated;
    bool processing_queue = false;
    formatter->open_array_section("entries");

    do {
      list<cls_rgw_gc_obj_info> result;
      int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->list_gc_objs(&index, marker, 1000, !include_all, result, &truncated, processing_queue);
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
    rgw::sal::RadosStore* rados_store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!rados_store) {
      cerr <<
	"WARNING: this command can only work when the cluster has a RADOS backing store." <<
	std::endl;
      return 0;
    }
    RGWRados* store = rados_store->getRados();

    int ret = store->process_gc(!include_all, null_yield);
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::LC_LIST) {
    formatter->open_array_section("lifecycle_list");
    vector<std::unique_ptr<rgw::sal::Lifecycle::LCEntry>> bucket_lc_map;
    string marker;
    int index{0};
#define MAX_LC_LIST_ENTRIES 100
    if (max_entries < 0) {
      max_entries = MAX_LC_LIST_ENTRIES;
    }
    do {
      int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->list_lc_progress(marker, max_entries,
						    bucket_lc_map, index);
      if (ret < 0) {
        cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret)
	     << std::endl;
        return 1;
      }
      for (const auto& entry : bucket_lc_map) {
        formatter->open_object_section("bucket_lc_info");
        formatter->dump_string("bucket", entry->get_bucket());
	formatter->dump_string("shard", entry->get_oid());
	char exp_buf[100];
	time_t t{time_t(entry->get_start_time())};
	if (std::strftime(
	      exp_buf, sizeof(exp_buf),
	      "%a, %d %b %Y %T %Z", std::gmtime(&t))) {
	  formatter->dump_string("started", exp_buf);
	}
        string lc_status = LC_STATUS[entry->get_status()];
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
    ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
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
        int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
	if (ret < 0) {
	  cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret)
	       << std::endl;
	  return ret;
	}
    }

    int ret =
      static_cast<rgw::sal::RadosStore*>(driver)->getRados()->process_lc(bucket);
    if (ret < 0) {
      cerr << "ERROR: lc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT::LC_RESHARD_FIX) {
    ret = RGWBucketAdminOp::fix_lc_shards(driver, bucket_op, stream_flusher, dpp(), null_yield);
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

    RGWOrphanSearch search(static_cast<rgw::sal::RadosStore*>(driver), max_concurrent_ios, orphan_stale_secs);

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

    RGWOrphanSearch search(static_cast<rgw::sal::RadosStore*>(driver), max_concurrent_ios, orphan_stale_secs);

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

    RGWOrphanStore orphan_store(static_cast<rgw::sal::RadosStore*>(driver));
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
    check_bad_user_bucket_mapping(driver, *user.get(), fix, null_yield, dpp());
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
      ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->user->reset_bucket_stats(dpp(), user->get_id(), null_yield);
      if (ret < 0) {
	cerr << "ERROR: could not reset user stats: " << cpp_strerror(-ret) <<
	  std::endl;
	return -ret;
      }
    }

    if (sync_stats) {
      if (!bucket_name.empty()) {
        int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
        if (ret < 0) {
          cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
        ret = bucket->sync_user_stats(dpp(), null_yield, nullptr);
        if (ret < 0) {
          cerr << "ERROR: could not sync bucket stats: " <<
	    cpp_strerror(-ret) << std::endl;
          return -ret;
        }
      } else {
        int ret = rgw_user_sync_all_stats(dpp(), driver, user.get(), null_yield);
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
    int ret = user->read_stats(dpp(), null_yield, &stats, &last_stats_sync, &last_stats_update);
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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->get(metadata_key, formatter.get(), null_yield, dpp());
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
    ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->put(metadata_key, bl, null_yield, dpp(), RGWMDLogSyncType::APPLY_ALWAYS, false);
    if (ret < 0) {
      cerr << "ERROR: can't put key: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::METADATA_RM) {
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->remove(metadata_key, null_yield, dpp());
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
    int ret = driver->meta_list_keys_init(dpp(), metadata_key, marker, &handle);
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
      ret = driver->meta_list_keys_next(dpp(), handle, left, keys, &truncated);
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
        encode_json("marker", driver->meta_get_marker(handle), formatter.get());
      }
      formatter->close_section();
    }
    formatter->flush(cout);

    driver->meta_list_keys_complete(handle);
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
      // use realm's current period
      RGWRealm realm;
      int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                realm_id, realm_name, realm);
      if (ret < 0 ) {
        cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      period_id = realm.current_period;
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_log(period_id);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      void *handle;
      list<cls_log_entry> entries;

      meta_log->init_list_entries(i, {}, {}, marker, &handle);
      bool truncated;
      do {
	int ret = meta_log->list_entries(dpp(), handle, 1000, entries, NULL, &truncated, null_yield);
        if (ret < 0) {
          cerr << "ERROR: meta_log->list_entries(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        for (list<cls_log_entry>::iterator iter = entries.begin(); iter != entries.end(); ++iter) {
          cls_log_entry& entry = *iter;
          static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->dump_log_entry(entry, formatter.get());
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
      // use realm's current period
      RGWRealm realm;
      int ret = rgw::read_realm(dpp(), null_yield, cfgstore.get(),
                                realm_id, realm_name, realm);
      if (ret < 0 ) {
        cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      period_id = realm.current_period;
      std::cerr << "No --period given, using current period="
          << period_id << std::endl;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_log(period_id);

    formatter->open_array_section("entries");

    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      RGWMetadataLogInfo info;
      meta_log->get_info(dpp(), i, &info, null_yield);

      ::encode_json("info", info, formatter.get());

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::MDLOG_AUTOTRIM) {
    // need a full history for purging old mdlog periods
    static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->init_oldest_log_period(null_yield, dpp());

    RGWCoroutinesManager crs(driver->ctx(), driver->get_cr_registry());
    RGWHTTPManager http(driver->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf()->rgw_md_log_max_shards;
    auto mltcr = create_admin_meta_log_trim_cr(
      dpp(), static_cast<rgw::sal::RadosStore*>(driver), &http, num_shards);
    if (!mltcr) {
      cerr << "Cluster misconfigured! Unable to trim." << std::endl;
      return -EIO;
    }
    ret = crs.run(dpp(), mltcr);
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
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_log(period_id);

    // trim until -ENODATA
    do {
      ret = meta_log->trim(dpp(), shard_id, {}, {}, {}, marker, null_yield);
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
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

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
    formatter->dump_string("current_time",
			   to_iso_8601(ceph::real_clock::now(),
				       iso_8601_format::YMDhms));
    formatter->close_section();

    formatter->flush(cout);

  }

  if (opt_cmd == OPT::METADATA_SYNC_INIT) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

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
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

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
    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, source_zone, nullptr);

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
      formatter->dump_string("current_time",
			     to_iso_8601(ceph::real_clock::now(),
					 iso_8601_format::YMDhms));
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
      formatter->dump_string("current_time",
			     to_iso_8601(ceph::real_clock::now(),
					 iso_8601_format::YMDhms));
      formatter->close_section();

      formatter->flush(cout);
    }
  }

  if (opt_cmd == OPT::DATA_SYNC_INIT) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, source_zone, nullptr);

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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->sync_modules->get_manager()->create_instance(dpp(), g_ceph_context, static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone().tier_type,
        static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_params().tier_config, &sync_module);
    if (ret < 0) {
      ldpp_dout(dpp(), -1) << "ERROR: failed to init sync module instance, ret=" << ret << dendl;
      return ret;
    }

    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, source_zone, nullptr, sync_module);

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
    int ret = init_bucket_for_sync(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto opt_sb = opt_source_bucket;
    if (opt_sb && opt_sb->bucket_id.empty()) {
      string sbid;
      std::unique_ptr<rgw::sal::Bucket> sbuck;
      int ret = init_bucket_for_sync(opt_sb->tenant, opt_sb->name, sbid, &sbuck);
      if (ret < 0) {
        return -ret;
      }
      opt_sb = sbuck->get_key();
    }

    auto sync = RGWBucketPipeSyncStatusManager::construct(
      dpp(), static_cast<rgw::sal::RadosStore*>(driver), source_zone, opt_sb,
      bucket->get_key(), extra_info ? &std::cout : nullptr);

    if (!sync) {
      cerr << "ERROR: sync.init() returned error=" << sync.error() << std::endl;
      return -sync.error();
    }
    ret = (*sync)->init_sync_status(dpp());
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    if (!static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->bucket_imports_data(bucket->get_key(), null_yield, dpp())) {
      std::cout << "Sync is disabled for bucket " << bucket_name << std::endl;
      return 0;
    }

    RGWBucketSyncPolicyHandlerRef handler;
    ret = driver->get_sync_policy_handler(dpp(), std::nullopt, bucket->get_key(), &handler, null_yield);
    if (ret < 0) {
      std::cerr << "ERROR: failed to get policy handler for bucket ("
          << bucket << "): r=" << ret << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto timeout_at = ceph::coarse_mono_clock::now() + opt_timeout_sec;
    ret = rgw_bucket_sync_checkpoint(dpp(), static_cast<rgw::sal::RadosStore*>(driver), *handler, bucket->get_info(),
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
    ret = RGWBucketAdminOp::sync_bucket(driver, bucket_op, dpp(), null_yield, &err_msg);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    bucket_sync_info(driver, bucket->get_info(), std::cout);
  }

  if (opt_cmd == OPT::BUCKET_SYNC_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    bucket_sync_status(driver, bucket->get_info(), source_zone, opt_source_bucket, std::cout);
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
    int ret = init_bucket_for_sync(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto sync = RGWBucketPipeSyncStatusManager::construct(
      dpp(), static_cast<rgw::sal::RadosStore*>(driver), source_zone,
      opt_source_bucket, bucket->get_key(), nullptr);

    if (!sync) {
      cerr << "ERROR: sync.init() returned error=" << sync.error() << std::endl;
      return -sync.error();
    }

    auto sync_status = (*sync)->read_sync_status(dpp());
    if (!sync_status) {
      cerr << "ERROR: sync.read_sync_status() returned error="
	   << sync_status.error() << std::endl;
      return -sync_status.error();
    }

    encode_json("sync_status", *sync_status, formatter.get());
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
    int ret = init_bucket_for_sync(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto sync = RGWBucketPipeSyncStatusManager::construct(
      dpp(), static_cast<rgw::sal::RadosStore*>(driver), source_zone,
      opt_source_bucket, bucket->get_key(), extra_info ? &std::cout : nullptr);

    if (!sync) {
      cerr << "ERROR: sync.init() returned error=" << sync.error() << std::endl;
      return -sync.error();
    }

    ret = (*sync)->run(dpp());
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_array_section("entries");
    bool truncated;
    int count = 0;
    if (max_entries < 0)
      max_entries = 1000;

    const auto& logs = bucket->get_info().layout.logs;
    auto log_layout = std::reference_wrapper{logs.back()};
    if (gen) {
      auto i = std::find_if(logs.begin(), logs.end(), rgw::matches_gen(*gen));
      if (i == logs.end()) {
        cerr << "ERROR: no log layout with gen=" << *gen << std::endl;
        return ENOENT;
      }
      log_layout = *i;
    }

    do {
      list<rgw_bi_log_entry> entries;
      ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->bilog_rados->log_list(dpp(), bucket->get_info(), log_layout, shard_id, marker, max_entries - count, entries, &truncated);
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
        ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->timelog.list(dpp(), oid, {}, {}, max_entries - count, entries, marker, &marker, &truncated,
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
      std::cerr << "start-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!end_marker.empty()) {
      std::cerr << "end_marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (marker.empty()) {
      marker = "9"; // trims everything
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
    CHECK_TRUE(require_non_empty_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_status), "ERROR: --status is not specified (options: forbidden, allowed, enabled)", EINVAL);

    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    CHECK_TRUE(require_non_empty_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);

    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    CHECK_TRUE(require_non_empty_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opt_flow_id), "ERROR: --flow-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_flow_type),
                           "ERROR: --flow-type not specified (options: symmetrical, directional)", EINVAL);
    CHECK_TRUE((symmetrical_flow_opt(*opt_flow_type) ||
                            directional_flow_opt(*opt_flow_type)),
                           "ERROR: --flow-type invalid (options: symmetrical, directional)", EINVAL);

    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    CHECK_TRUE(require_non_empty_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opt_flow_id), "ERROR: --flow-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opt_flow_type),
                           "ERROR: --flow-type not specified (options: symmetrical, directional)", EINVAL);
    CHECK_TRUE((symmetrical_flow_opt(*opt_flow_type) ||
                            directional_flow_opt(*opt_flow_type)),
                           "ERROR: --flow-type invalid (options: symmetrical, directional)", EINVAL);

    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    CHECK_TRUE(require_non_empty_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opt_pipe_id), "ERROR: --pipe-id not specified", EINVAL);
    if (opt_cmd == OPT::SYNC_GROUP_PIPE_CREATE) {
      CHECK_TRUE(require_non_empty_opt(opt_source_zone_ids), "ERROR: --source-zones not provided or is empty; should be list of zones or '*'", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opt_dest_zone_ids), "ERROR: --dest-zones not provided or is empty; should be list of zones or '*'", EINVAL);
    }

    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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

    if (opt_source_zone_ids) {
      pipe->source.add_zones(*opt_source_zone_ids);
    }
    pipe->source.set_bucket(opt_source_tenant,
                            opt_source_bucket_name,
                            opt_source_bucket_id);
    if (opt_dest_zone_ids) {
      pipe->dest.add_zones(*opt_dest_zone_ids);
    }
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
    CHECK_TRUE(require_non_empty_opt(opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opt_pipe_id), "ERROR: --pipe-id not specified", EINVAL);

    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    SyncPolicyContext sync_policy_ctx(cfgstore.get(), opt_bucket);
    ret = sync_policy_ctx.init(zonegroup_id, zonegroup_name);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    if (!gen) {
      gen = 0;
    }
    ret = bilog_trim(dpp(), static_cast<rgw::sal::RadosStore*>(driver),
		     bucket->get_info(), *gen,
		     shard_id, start_marker, end_marker);
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
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    map<int, string> markers;
    const auto& logs = bucket->get_info().layout.logs;
    auto log_layout = std::reference_wrapper{logs.back()};
    if (gen) {
      auto i = std::find_if(logs.begin(), logs.end(), rgw::matches_gen(*gen));
      if (i == logs.end()) {
        cerr << "ERROR: no log layout with gen=" << *gen << std::endl;
        return ENOENT;
      }
      log_layout = *i;
    }

    ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->bilog_rados->get_log_status(dpp(), bucket->get_info(), log_layout, shard_id,
						    &markers, null_yield);
    if (ret < 0) {
      cerr << "ERROR: get_bi_log_status(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    formatter->open_object_section("entries");
    encode_json("markers", markers, formatter.get());
    formatter->dump_string("current_time",
			   to_iso_8601(ceph::real_clock::now(),
				       iso_8601_format::YMDhms));
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::BILOG_AUTOTRIM) {
    RGWCoroutinesManager crs(driver->ctx(), driver->get_cr_registry());
    RGWHTTPManager http(driver->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    rgw::BucketTrimConfig config;
    configure_bucket_trim(driver->ctx(), config);

    rgw::BucketTrimManager trim(static_cast<rgw::sal::RadosStore*>(driver), config);
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

    auto datalog_svc = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    RGWDataChangesLog::LogMarker log_marker;

    do {
      std::vector<rgw_data_change_log_entry> entries;
      if (specified_shard_id) {
        ret = datalog_svc->list_entries(dpp(), shard_id, max_entries - count,
					entries, marker,
					&marker, &truncated,
					null_yield);
      } else {
        ret = datalog_svc->list_entries(dpp(), max_entries - count, entries,
					log_marker, &truncated, null_yield);
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
      static_cast<rgw::sal::RadosStore*>(driver)->svc()->
	datalog_rados->get_info(dpp(), i, &info, null_yield);

      ::encode_json("info", info, formatter.get());

      if (specified_shard_id)
        break;
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::DATALOG_AUTOTRIM) {
    RGWCoroutinesManager crs(driver->ctx(), driver->get_cr_registry());
    RGWHTTPManager http(driver->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf()->rgw_data_log_num_shards;
    std::vector<std::string> markers(num_shards);
    ret = crs.run(dpp(), create_admin_data_log_trim_cr(dpp(), static_cast<rgw::sal::RadosStore*>(driver), &http, num_shards, markers));
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

    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    ret = datalog->trim_entries(dpp(), shard_id, marker, null_yield);

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
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    ret = datalog->change_format(dpp(), *opt_log_type, null_yield);
    if (ret < 0) {
      cerr << "ERROR: change_format(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::DATALOG_PRUNE) {
    auto datalog = static_cast<rgw::sal::RadosStore*>(driver)->svc()->datalog_rados;
    std::optional<uint64_t> through;
    ret = datalog->trim_generations(dpp(), through, null_yield);

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
      set_bucket_quota(driver, opt_cmd, tenant, bucket_name,
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
      return set_bucket_ratelimit(driver, opt_cmd, tenant, bucket_name,
                           max_read_ops, max_write_ops,
                           max_read_bytes, max_write_bytes,
                           have_max_read_ops, have_max_write_ops,
                           have_max_read_bytes, have_max_write_bytes);
    } else if (!rgw::sal::User::empty(user)) {
      if (ratelimit_scope == "user") {
        return set_user_ratelimit(opt_cmd, user, max_read_ops, max_write_ops,
                         max_read_bytes, max_write_bytes,
                         have_max_read_ops, have_max_write_ops,
                         have_max_read_bytes, have_max_write_bytes);
      } else {
        cerr << "ERROR: invalid ratelimit scope specification. Please specify either --ratelimit-scope=bucket, or --ratelimit-scope=user" << std::endl;
        return EINVAL;
      }
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
      return show_bucket_ratelimit(driver, tenant, bucket_name, formatter.get());
    } else if (!rgw::sal::User::empty(user)) {
      if (ratelimit_scope == "user") {
        return show_user_ratelimit(user, formatter.get());
      } else {
        cerr << "ERROR: invalid ratelimit scope specification. Please specify either --ratelimit-scope=bucket, or --ratelimit-scope=user" << std::endl;
        return EINVAL;
      }
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
    string oid = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.get_mfa_oid(user->get_id());

    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(user->get_id()),
					     mtime, &objv_tracker,
					     null_yield, dpp(),
					     MDLOG_STATUS_WRITE,
					     [&] {
      return static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.create_mfa(dpp(), user->get_id(), config, &objv_tracker, mtime, null_yield);
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

    int ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(user->get_id()),
					     mtime, &objv_tracker,
					     null_yield, dpp(),
					     MDLOG_STATUS_WRITE,
					     [&] {
      return static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.remove_mfa(dpp(), user->get_id(), totp_serial, &objv_tracker, mtime, null_yield);
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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.get_mfa(dpp(), user->get_id(), totp_serial, &result, null_yield);
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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.list_mfa(dpp(), user->get_id(), &result, null_yield);
    if (ret < 0 && ret != -ENOENT) {
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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.check_mfa(dpp(), user->get_id(), totp_serial, totp_pin.front(), null_yield);
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
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.get_mfa(dpp(), user->get_id(), totp_serial, &config, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    ceph::real_time now;

    ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.otp_get_current_time(dpp(), user->get_id(), &now, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to fetch current time from osd: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    time_t time_ofs;

    ret = scan_totp(driver->ctx(), now, config, totp_pin, &time_ofs);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "failed to resync, TOTP values not found in range" << std::endl;
      } else {
        cerr << "ERROR: failed to scan for TOTP values: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    // time offset is a small number and unlikely to overflow
    // coverity[store_truncates_time_t:SUPPRESS]
    config.time_ofs = time_ofs;

    /* now update the backend */
    real_time mtime = real_clock::now();

    ret = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(user->get_id()),
				         mtime, &objv_tracker,
				         null_yield, dpp(),
				         MDLOG_STATUS_WRITE,
				         [&] {
      return static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->mfa.create_mfa(dpp(), user->get_id(), config, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA update failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

 }

 if (opt_cmd == OPT::RESHARD_STALE_INSTANCES_LIST) {
   if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->can_reshard() && !yes_i_really_mean_it) {
     cerr << "Resharding disabled in a multisite env, stale instances unlikely from resharding" << std::endl;
     cerr << "These instances may not be safe to delete." << std::endl;
     cerr << "Use --yes-i-really-mean-it to force displaying these instances." << std::endl;
     return EINVAL;
   }

   ret = RGWBucketAdminOp::list_stale_instances(driver, bucket_op, stream_flusher, dpp(), null_yield);
   if (ret < 0) {
     cerr << "ERROR: listing stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

 if (opt_cmd == OPT::RESHARD_STALE_INSTANCES_DELETE) {
   if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->can_reshard()) {
     cerr << "Resharding disabled in a multisite env. Stale instances are not safe to be deleted." << std::endl;
     return EINVAL;
   }

   ret = RGWBucketAdminOp::clear_stale_instances(driver, bucket_op, stream_flusher, dpp(), null_yield);
   if (ret < 0) {
     cerr << "ERROR: deleting stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

  if (opt_cmd == OPT::PUBSUB_NOTIFICATION_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(driver, tenant);

    rgw_pubsub_bucket_topics result;
    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    const RGWPubSub::Bucket b(ps, bucket.get());
    ret = b.get_topics(dpp(), result, null_yield);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("result", result, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_LIST) {
    RGWPubSub ps(driver, tenant);

    rgw_pubsub_topics result;
    int ret = ps.get_topics(dpp(), result, null_yield);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    if (!rgw::sal::User::empty(user)) {
      for (auto it = result.topics.cbegin(); it != result.topics.cend();) {
        const auto& topic = it->second;
        if (user->get_id() != topic.user) {
          result.topics.erase(it++);
        } else {
          ++it;
        }
      }
    }
    encode_json("result", result, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_GET) {
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(driver, tenant);

    rgw_pubsub_topic topic;
    ret = ps.get_topic(dpp(), topic_name, topic, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not get topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("topic", topic, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_NOTIFICATION_GET) {
    if (notification_id.empty()) {
      cerr << "ERROR: notification-id was not provided (via --notification-id)" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }

    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWPubSub ps(driver, tenant);

    rgw_pubsub_bucket_topics bucket_topics;
    const RGWPubSub::Bucket b(ps, bucket.get());
    ret = b.get_topics(dpp(), bucket_topics, null_yield);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not get bucket notifications: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_pubsub_topic_filter bucket_topic;
    ret = b.get_notification_by_id(dpp(), notification_id, bucket_topic, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not get notification: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("notification", bucket_topic, formatter.get());
    formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_RM) {
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    ret = rgw::notify::remove_persistent_topic(
        dpp(), static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_notif_pool_ctx(), topic_name, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not remove persistent topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWPubSub ps(driver, tenant);

    ret = ps.remove_topic(dpp(), topic_name, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not remove topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::PUBSUB_NOTIFICATION_RM) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket name was not provided (via --bucket)" << std::endl;
      return EINVAL;
    }

    int ret = init_bucket(tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWPubSub ps(driver, tenant);

    rgw_pubsub_bucket_topics bucket_topics;
    const RGWPubSub::Bucket b(ps, bucket.get());
    ret = b.get_topics(dpp(), bucket_topics, null_yield);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not get bucket notifications: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_pubsub_topic_filter bucket_topic;
    if(notification_id.empty()) {
      ret = b.remove_notifications(dpp(), null_yield);
    } else {
      ret = b.remove_notification_by_id(dpp(), notification_id, null_yield);
    }
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_STATS) {
    if (topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    rgw::notify::rgw_topic_stats stats;
    ret = rgw::notify::get_persistent_queue_stats_by_topic_name(
        dpp(), static_cast<rgw::sal::RadosStore *>(driver)->getRados()->get_notif_pool_ctx(), topic_name,
        stats, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not get persistent queue: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("", stats, formatter.get());
    formatter->flush(cout);
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
      cerr << "ERROR: invalid script context: " << *str_script_ctx << ". must be one of: " << LUA_CONTEXT_LIST << std::endl;
      return EINVAL;
    }
    if (script_ctx == rgw::lua::context::background && !tenant.empty()) {
      cerr << "ERROR: cannot specify tenant in background context" << std::endl;
      return EINVAL;
    }
    auto lua_manager = driver->get_lua_manager("");
    rc = rgw::lua::write_script(dpp(), lua_manager.get(), tenant, null_yield, script_ctx, script);
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
      cerr << "ERROR: invalid script context: " << *str_script_ctx << ". must be one of: " << LUA_CONTEXT_LIST << std::endl;
      return EINVAL;
    }
    auto lua_manager = driver->get_lua_manager("");
    std::string script;
    const auto rc = rgw::lua::read_script(dpp(), lua_manager.get(), tenant, null_yield, script_ctx, script);
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
      cerr << "ERROR: invalid script context: " << *str_script_ctx << ". must be one of: " << LUA_CONTEXT_LIST << std::endl;
      return EINVAL;
    }
    auto lua_manager = driver->get_lua_manager("");
    const auto rc = rgw::lua::delete_script(dpp(), lua_manager.get(), tenant, null_yield, script_ctx);
    if (rc < 0) {
      cerr << "ERROR: failed to remove script. error: " << rc << std::endl;
      return -rc;
    }
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_ADD) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    if (!script_package) {
      cerr << "ERROR: Lua package name was not provided (via --package)" << std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::add_package(dpp(), driver, null_yield, *script_package, bool(allow_compilation));
    if (rc < 0) {
      cerr << "ERROR: failed to add Lua package: " << script_package << " .error: " << rc << std::endl;
      return -rc;
    }
#else
    cerr << "ERROR: adding Lua packages is not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_RM) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    if (!script_package) {
      cerr << "ERROR: Lua package name was not provided (via --package)" << std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::remove_package(dpp(), driver, null_yield, *script_package);
    if (rc == -ENOENT) {
      cerr << "WARNING: package " << script_package << " did not exists or already removed" << std::endl;
      return 0;
    }
    if (rc < 0) {
      cerr << "ERROR: failed to remove Lua package: " << script_package << " .error: " << rc << std::endl;
      return -rc;
    }
#else
    cerr << "ERROR: removing Lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_LIST) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    rgw::lua::packages_t packages;
    const auto rc = rgw::lua::list_packages(dpp(), driver, null_yield, packages);
    if (rc == -ENOENT) {
      std::cout << "no Lua packages in allowlist" << std::endl;
    } else if (rc < 0) {
      cerr << "ERROR: failed to read Lua packages allowlist. error: " << rc << std::endl;
      return rc;
    } else {
      for (const auto& package : packages) {
          std::cout << package << std::endl;
      }
    }
#else
    cerr << "ERROR: listing Lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (opt_cmd == OPT::SCRIPT_PACKAGE_RELOAD) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    const auto rc = rgw::lua::reload_packages(dpp(), driver, null_yield);
    if (rc < 0) {
      cerr << "ERROR: failed to reload Lua packages. error: " << rc << std::endl;
      return rc;
    }
#else
    cerr << "ERROR: reloading Lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }
  return 0;
}

