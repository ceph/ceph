// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Copyright (C) 2025 IBM
 */

#include <cerrno>
#include <string>
#include <sstream>
#include <optional>
#include <iostream>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "rgw_cors_s3.h"

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
#include "common/JSONFormatter.h"
#include "common/XMLFormatter.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/fault_injector.h"

#include "common/async/blocked_completion.h"

#include "include/util.h"

#ifdef WITH_RADOSGW_RADOS
#include "cls/rgw/cls_rgw_types.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "cls/2pc_queue/cls_2pc_queue_client.h"
#endif

#include "include/utime.h"
#include "include/str_list.h"

#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/orphan.h"
#include "radosgw-admin/sync_checkpoint.h"
#include "radosgw-admin/log.h"
#endif
#include "radosgw-admin/account.h"
#include "radosgw-admin/bucket.h"
#include "radosgw-admin/user.h"
#include "radosgw-admin/usage.h"
#include "radosgw-admin/quota_ratelimit.h"
#include "radosgw-admin/script.h"
#include "radosgw-admin/pubsub.h"
#include "radosgw-admin/realm.h"
#include "radosgw-admin/zonegroup.h"
#include "radosgw-admin/zone.h"
#include "radosgw-admin/period.h"

#include "radosgw-admin/bucket_logging.h"
#include "radosgw-admin/object.h"
#include "radosgw-admin/bi.h"
#include "radosgw-admin/olh.h"
#include "radosgw-admin/dedup.h"
#include "radosgw-admin/gc.h"
#include "radosgw-admin/lc.h"
#include "radosgw-admin/bucket_sync.h"
#ifdef WITH_RADOSGW_RADOS
#include "radosgw-admin/bucket_resync_encrypted_multipart.h"
#endif
#include "radosgw-admin/bilog.h"
#include "radosgw-admin/role.h"
#include "radosgw-admin/reshard.h"
#include "radosgw-admin/cors.h"

#include "radosgw-admin/metadata.h"
#include "radosgw-admin/sync.h"
#include "radosgw-admin/mfa.h"
#include "radosgw-admin/datalog.h"
#include "radosgw-admin/restore.h"
#include "radosgw-admin/radosgw-admin.h"

#include "rgw/async_utils.h"

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
#ifdef WITH_RADOSGW_RADOS
#include "rgw_sync.h"
#endif
#include "rgw_trim_bilog.h"
#include "rgw_trim_datalog.h"
#include "rgw_trim_mdlog.h"
#ifdef WITH_RADOSGW_RADOS
#include "rgw_data_sync.h"
#endif
#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"
#include "rgw_role.h"
#include "rgw_reshard.h"
#include "rgw_http_client_curl.h"
#include "rgw_zone.h"
#include "rgw_pubsub.h"
#include "rgw_bucket_sync.h"
#include "rgw_lua.h"
#include "rgw_sal.h"
#include "rgw_sal_config.h"
#include "rgw_data_access.h"
#include "rgw_account.h"
#include "rgw_bucket_logging.h"
#include "rgw_dedup_cluster.h"
#include "rgw_dedup_filter.h"
#include "services/svc_sync_modules.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_mdlog.h"
#include "services/svc_user.h"
#include "services/svc_zone.h"

#include "driver/rados/rgw_bucket.h"
#ifdef WITH_RADOSGW_RADOS
#include "driver/rados/rgw_sal_rados.h"
#endif
#include "driver/rados/rgw_bl_rados.h"

#include <iomanip>

#define dout_context g_ceph_context

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

using namespace std;
using rgw::run_coro;

inline int posix_errortrans(int r)
{
 return ERR_NO_SUCH_BUCKET == r ? ENOENT : r;
}

static const std::string LUA_CONTEXT_LIST("prerequest, postauth, postrequest, background, getdata, putdata");

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
  cout << "  user policy attach               attach a managed policy\n";
  cout << "  user policy detach               detach a managed policy\n";
  cout << "  user policy list attached        list attached managed policies\n";
  cout << "  caps add                         add user capabilities\n";
  cout << "  caps rm                          remove user capabilities\n";
  cout << "  dedup stats                      Display dedup statistics from the last run\n";
  cout << "  dedup estimate                   Runs dedup in estimate mode (no changes will be made)\n";
  cout << "  dedup exec                       Execute dedup (duplicated tail objects will be deleted); must include --yes-i-really-mean-it to activate\n";
  cout << "  dedup abort                      Abort dedup\n";
  cout << "  dedup pause                      Pause dedup\n";
  cout << "  dedup resume                     Resume paused dedup\n";
  cout << "  dedup throttle                   Throttle dedup execution\n";
  cout << "  subuser create                   create a new subuser\n" ;
  cout << "  subuser modify                   modify subuser\n";
  cout << "  subuser rm                       remove subuser\n";
  cout << "  key create                       create access key\n";
  cout << "  key rm                           remove access key\n";
  cout << "  account create                   create a new account\n";
  cout << "  account modify                   modify an existing account\n";
  cout << "  account get                      get account info\n";
  cout << "  account stats                    dump account storage stats\n";
  cout << "  account rm                       remove an account\n";
  cout << "  account list                     list all account ids\n";
  cout << "  bucket list                      list buckets (specify --allow-unordered for faster, unsorted listing)\n";
  cout << "  bucket limit check               show bucket sharding stats\n";
  cout << "  bucket link                      link bucket to specified user\n";
  cout << "  bucket unlink                    unlink bucket from specified user\n";
  cout << "  bucket stats                     returns bucket statistics\n";
  cout << "  bucket suspend                   suspend a bucket\n";
  cout << "  bucket unsuspend                 unsuspend a bucket\n";
  cout << "  bucket rm                        remove bucket\n";
  cout << "  bucket check                     check bucket index by verifying size and object count stats\n";
  cout << "  bucket check olh                 check for olh index entries and objects that are pending removal\n";
  cout << "  bucket check unlinked            check for object versions that are not visible in a bucket listing \n";
  cout << "  bucket chown                     link bucket to specified user and update its object ACLs\n";
  cout << "  bucket reshard                   reshard bucket\n";
  cout << "  bucket set-min-shards            set the minimum number of shards that dynamic resharding will consider for a bucket\n";
  cout << "  bucket rewrite                   rewrite all objects in the specified bucket\n";
  cout << "  bucket sync checkpoint           poll a bucket's sync status until it catches up to its remote\n";
  cout << "  bucket sync disable              disable bucket sync\n";
  cout << "  bucket sync enable               enable bucket sync\n";
  cout << "  bucket radoslist                 list rados objects backing bucket's objects\n";
  cout << "  bucket logging flush             flush pending log records object of source bucket to the log bucket\n";
  cout << "  bucket logging info              get info on bucket logging configuration on source bucket or list of sources in log bucket\n";
  cout << "  bucket logging list              list the log objects pending commit for the source bucket\n";
  cout << "  bi get                           retrieve bucket index object entries\n";
  cout << "  bi put                           store bucket index object entries\n";
  cout << "  bi list                          list raw bucket index entries\n";
  cout << "  bi purge                         purge bucket index entries\n";
  cout << "  object rm                        remove object; include --yes-i-really-mean-it to force removal from bucket index\n";
  cout << "  object put                       put object\n";
  cout << "  object stat                      stat an object for its metadata\n";
  cout << "  object manifest                  display the manifest of an object, producing a list of RADOS objects containing the data\n";
  cout << "  object unlink                    unlink object from bucket index\n";
  cout << "  object rewrite                   rewrite the specified object\n";
  cout << "  object reindex                   reindex the object(s) indicated by --bucket and either --object or --objects-file\n";
  cout << "  objects expire                   run expired objects cleanup\n";
  cout << "  objects expire-stale list        list stale expired objects (caused by reshard)\n";
  cout << "  objects expire-stale rm          remove stale expired objects\n";
  cout << "  period delete                    remove a period\n";
  cout << "  period get                       get period info\n";
  cout << "  period get-current               get current period info\n";
  cout << "  period pull                      pull a period\n";
  cout << "  period push                      push a period\n";
  cout << "  period list                      list all periods\n";
  cout << "  period update                    update the staging period\n";
  cout << "  period commit                    commit the staging period\n";
  cout << "  quota set                        set quota params for a user/bucket/account\n";
  cout << "  quota enable                     enable quota for a user/bucket/account\n";
  cout << "  quota disable                    disable quota for a user/bucket/account\n";
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
  cout << "  realm default rm                 clear the current default realm\n";
  cout << "  realm pull                       pull a realm and its current period\n";
  cout << "  zonegroup add                    add a zone to a zonegroup\n";
  cout << "  zonegroup create                 create a new zone group info\n";
  cout << "  zonegroup default                set default zone group\n";
  cout << "  zonegroup delete                 delete a zone group info\n";
  cout << "  zonegroup get                    show zone group info\n";
  cout << "  zonegroup modify                 modify an existing zonegroup\n";
  cout << "  zonegroup set                    set zone group info (requires infile)\n";
  cout << "  zonegroup remove                 remove a zone from a zonegroup\n";
  cout << "  zonegroup rename                 rename a zone group\n";
  cout << "  zonegroup list                   list all zone groups set on this cluster\n";
  cout << "  zonegroup placement list         list zonegroup's placement targets\n";
  cout << "  zonegroup placement get          get a placement target of a specific zonegroup\n";
  cout << "  zonegroup placement add          add a placement target id to a zonegroup\n";
  cout << "  zonegroup placement modify       modify a placement target of a specific zonegroup\n";
  cout << "  zonegroup placement rm           remove a placement target from a zonegroup\n";
  cout << "  zonegroup placement default      set a zonegroup's default placement target\n";
  cout << "  zone create                      create a new zone\n";
  cout << "  zone delete                      remove a zone\n";
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
  cout << "  datalog type                     change datalog type to --log_type=fifo\n";
  cout << "  datalog semaphore list           List recovery semaphores\n";
  cout << "  datalog semaphore reset          Reset recovery semaphore (use marker)\n";
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
  cout << "  role policy attach               attach a managed policy\n";
  cout << "  role policy detach               detach a managed policy\n";
  cout << "  role policy list attached        list attached managed policies\n";
  cout << "  role update                      update max_session_duration of a role\n";
  cout << "  reshard add                      schedule a resharding of a bucket\n";
  cout << "  reshard list                     list all bucket resharding or scheduled to be resharded\n";
  cout << "  reshard status                   read bucket resharding status\n";
  cout << "  reshard process                  process of scheduled reshard jobs\n";
  cout << "  reshard cancel                   cancel resharding a bucket\n";
  cout << "  reshard stale-instances list     list stale-instances from bucket resharding\n";
  cout << "  reshard stale-instances delete   cleanup stale-instances from bucket resharding\n";
  cout << "  reshardlog list                  list bucket resharding log\n";
  cout << "  reshardlog purge                 trim bucket resharding log\n";
  cout << "  sync error list                  list sync error\n";
  cout << "  sync error trim                  trim sync error\n";
#ifdef WITH_RADOSGW_RADOS
  cout << "  mfa create                       create a new MFA TOTP token\n";
  cout << "  mfa list                         list MFA TOTP tokens\n";
  cout << "  mfa get                          show MFA TOTP token\n";
  cout << "  mfa remove                       delete MFA TOTP token\n";
  cout << "  mfa check                        check MFA TOTP token\n";
  cout << "  mfa resync                       re-sync MFA TOTP token\n";
#endif
  cout << "  topic list                       list bucket notifications topics\n";
  cout << "  topic get                        get a bucket notifications topic\n";
  cout << "  topic rm                         remove a bucket notifications topic\n";
  cout << "  topic stats                      get a bucket notifications persistent topic stats (i.e. reservations, entries & size)\n";
  cout << "  topic dump                       dump (in JSON format) all pending bucket notifications of a persistent topic\n";
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
  cout << "  restore status                   shows restoration status of object in a bucket\n";
  cout << "  restore list                     list restore status of each object in the bucket\n";
  cout << "                                   can be filtered with help of --restore-status which shows objects with specified status\n";
  cout << "options:\n";
  cout << "   --tenant=<tenant>                 tenant name\n";
  cout << "   --user_ns=<namespace>             namespace of user (oidc in case of users authenticated with oidc provider)\n";
  cout << "   --uid=<id>                        user id\n";
  cout << "   --new-uid=<id>                    new user id\n";
  cout << "   --subuser=<name>                  subuser name\n";
  cout << "   --account-name=<name>             account name\n";
  cout << "   --account-id=<id>                 account id\n";
  cout << "   --max-users                       max number of users for an account\n";
  cout << "   --max-roles                       max number of roles for an account\n";
  cout << "   --max-groups                      max number of groups for an account\n";
  cout << "   --max-access-keys                 max number of keys per user for an account\n";
  cout << "   --access-key=<key>                S3 access key\n";
  cout << "   --email=<email>                   user's email address\n";
  cout << "   --secret/--secret-key=<key>       specify secret key\n";
  cout << "   --gen-access-key                  generate random access key (for S3)\n";
  cout << "   --gen-secret                      generate random secret key\n";
  cout << "   --generate-key                    create user with or without credentials\n";
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
  cout << "   --count=<count>                   optional for:\n";
  cout << "                                       datalog semaphore reset\n";
  cout << "   --shard-id=<shard-id>             optional for:\n";
  cout << "                                       mdlog list\n";
  cout << "                                       data sync status\n";
  cout << "                                       sync error trim\n";
  cout << "                                       gc list\n";
  cout << "                                       gc process\n";
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
  cout << "   --format=<format>                 specify output format for certain operations: xml, json (default: json)\n";
  cout << "   --pretty-format                   enable pretty formatting for json/xml output\n";
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
  cout << "\nDedup throttle options:\n";
  cout << "   --max-bucket-index-ops        specify max bucket-index requests per second allowed for an RGW during dedup, 0 means unlimited\n";
  cout << "   --max-metadata-ops            specify max metadata requests per second allowed for an RGW during dedup, 0 means unlimited\n";
  cout << "   --stat                        display dedup throttle setting\n";
  cout << "\nDedup filter options:\n";
  cout << "   --allow-bucket-list=<file>    file with bucket names to allow in dedup (mutually exclusive with --deny-bucket-list)\n";
  cout << "   --deny-bucket-list=<file>     file with bucket names to deny in dedup (mutually exclusive with --allow-bucket-list)\n";
  cout << "   --allow-storage-class-list=<file> file with storage class names to allow in dedup (mutually exclusive with --deny-storage-class-list)\n";
  cout << "   --deny-storage-class-list=<file>  file with storage class names to deny in dedup (mutually exclusive with --allow-storage-class-list)\n";
  cout << "\nQuota options:\n";
  cout << "   --max-objects                 specify max objects (negative value to disable)\n";
  cout << "   --max-size                    specify max size (in B/K/M/G/T, negative value to disable)\n";
  cout << "   --quota-scope                 scope of quota (bucket, user, account)\n";
  cout << "\nRate limiting options:\n";
  cout << "   --max-read-ops                specify max requests per accumulation interval for READ ops per RGW (GET and HEAD request methods), 0 means unlimited\n";
  cout << "   --max-read-bytes              specify max bytes per accumulation interval for READ ops per RGW (GET and HEAD request methods), 0 means unlimited\n";
  cout << "   --max-write-ops               specify max requests per accumulation interval for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited\n";
  cout << "   --max-write-bytes             specify max bytes per accumulation interval for WRITE ops per RGW (Not GET or HEAD request methods), 0 means unlimited\n";
  cout << "   --max-list-ops                specify max requests per accumulation interval for bucket listing requests per RGW, 0 means unlimited\n";
  cout << "   --max-delete-ops              specify max requests per accumulation interval for DELETE ops per RGW (DELETE request methods), 0 means unlimited\n";
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
  cout << "   --description                 Role description\n";
  cout << "   --policy-arn                  ARN of a managed policy\n";
#ifdef WITH_RADOSGW_RADOS
  cout << "\nMFA options:\n";
#endif
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
  cout << "\nBucket list objects options:\n";
  cout << "   --max-entries                 max number of entries listed (default 1000)\n";
  cout << "   --marker                      object name marker to specify where listing begins (default: start from beginning)\n";
  cout << "                                 requires ordered listing (do not use with --allow-unordered)\n";
  cout << "   --object-version              for versioned buckets: specify the version/instance ID to start from\n";
  cout << "                                 use together with --marker to paginate through versioned buckets\n";
  cout << "                                 example: --marker=obj1 --object-version=abc123def456\n";
  cout << "   --show-restore-stats          if the flag is in present it will show restores stats in the bucket stats command\n";
  cout << "\n";
  generic_client_usage();
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
  { "user policy attach", OPT::USER_POLICY_ATTACH },
  { "user policy detach", OPT::USER_POLICY_DETACH },
  { "user policy list attached", OPT::USER_POLICY_LIST_ATTACHED },
  { "subuser create", OPT::SUBUSER_CREATE },
  { "subuser modify", OPT::SUBUSER_MODIFY },
  { "subuser rm", OPT::SUBUSER_RM },
  { "key create", OPT::KEY_CREATE },
  { "key rm", OPT::KEY_RM },
  { "buckets list", OPT::BUCKETS_LIST },
  { "bucket list", OPT::BUCKETS_LIST },
  { "bucket limit check", OPT::BUCKET_LIMIT_CHECK },
#ifdef WITH_RADOSGW_RADOS
  { "bucket link", OPT::BUCKET_LINK },
  { "bucket unlink", OPT::BUCKET_UNLINK },
#endif
  { "bucket layout", OPT::BUCKET_LAYOUT },
  { "bucket stats", OPT::BUCKET_STATS },
  { "bucket suspend", OPT::BUCKET_SUSPEND },
  { "bucket unsuspend", OPT::BUCKET_UNSUSPEND },
#ifdef WITH_RADOSGW_RADOS
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
#endif
  { "bucket rm", OPT::BUCKET_RM },
#ifdef WITH_RADOSGW_RADOS
  { "bucket rewrite", OPT::BUCKET_REWRITE },
  { "bucket reshard", OPT::BUCKET_RESHARD },
#endif
  { "bucket set-min-shards", OPT::BUCKET_SET_MIN_SHARDS },
  { "bucket chown", OPT::BUCKET_CHOWN },
#ifdef WITH_RADOSGW_RADOS
  { "bucket radoslist", OPT::BUCKET_RADOS_LIST },
  { "bucket rados list", OPT::BUCKET_RADOS_LIST },
#endif
  { "bucket shard objects", OPT::BUCKET_SHARD_OBJECTS },
  { "bucket shard object", OPT::BUCKET_SHARD_OBJECTS },
  { "bucket object shard", OPT::BUCKET_OBJECT_SHARD },
#ifdef WITH_RADOSGW_RADOS
  { "bucket resync encrypted multipart", OPT::BUCKET_RESYNC_ENCRYPTED_MULTIPART },
#endif
  { "bucket logging flush", OPT::BUCKET_LOGGING_FLUSH },
  { "bucket logging info", OPT::BUCKET_LOGGING_INFO },
  { "bucket logging list", OPT::BUCKET_LOGGING_LIST },
  { "policy", OPT::POLICY },
#ifdef WITH_RADOSGW_RADOS
  { "log list", OPT::LOG_LIST },
  { "log show", OPT::LOG_SHOW },
  { "log rm", OPT::LOG_RM },
#endif
  { "usage show", OPT::USAGE_SHOW },
  { "usage trim", OPT::USAGE_TRIM },
  { "usage clear", OPT::USAGE_CLEAR },
  { "object put", OPT::OBJECT_PUT },
  { "object rm", OPT::OBJECT_RM },
  { "object unlink", OPT::OBJECT_UNLINK },
  { "object stat", OPT::OBJECT_STAT },
#ifdef WITH_RADOSGW_RADOS
  { "object manifest", OPT::OBJECT_MANIFEST },
  { "object rewrite", OPT::OBJECT_REWRITE },
  { "object reindex", OPT::OBJECT_REINDEX },
#endif  
  { "objects expire", OPT::OBJECTS_EXPIRE },
  { "objects expire-stale list", OPT::OBJECTS_EXPIRE_STALE_LIST },
  { "objects expire-stale rm", OPT::OBJECTS_EXPIRE_STALE_RM },
#ifdef WITH_RADOSGW_RADOS
  { "bi get", OPT::BI_GET },
  { "bi put", OPT::BI_PUT },
  { "bi list", OPT::BI_LIST },
  { "bi purge", OPT::BI_PURGE },
  { "olh get", OPT::OLH_GET },
  { "olh readlog", OPT::OLH_READLOG },
#endif
  { "quota set", OPT::QUOTA_SET },
  { "quota enable", OPT::QUOTA_ENABLE },
  { "quota disable", OPT::QUOTA_DISABLE },
  { "ratelimit get", OPT::RATELIMIT_GET },
  { "ratelimit set", OPT::RATELIMIT_SET },
  { "ratelimit enable", OPT::RATELIMIT_ENABLE },
  { "ratelimit disable", OPT::RATELIMIT_DISABLE },
#ifdef WITH_RADOSGW_RADOS
  { "dedup stats", OPT::DEDUP_STATS },
  { "dedup estimate", OPT::DEDUP_ESTIMATE },
  { "dedup abort", OPT::DEDUP_ABORT },
  { "dedup restart", OPT::DEDUP_EXEC },
  { "dedup exec", OPT::DEDUP_EXEC },
  { "dedup pause", OPT::DEDUP_PAUSE },
  { "dedup resume", OPT::DEDUP_RESUME },
  { "dedup throttle", OPT::DEDUP_THROTTLE },
  { "gc list", OPT::GC_LIST },
  { "gc process", OPT::GC_PROCESS },
#endif
  { "lc list", OPT::LC_LIST },
  { "lc get", OPT::LC_GET },
#ifdef WITH_RADOSGW_RADOS
  { "lc process", OPT::LC_PROCESS },
#endif
  { "lc reshard fix", OPT::LC_RESHARD_FIX },
#ifdef WITH_RADOSGW_RADOS
  { "orphans find", OPT::ORPHANS_FIND },
  { "orphans finish", OPT::ORPHANS_FINISH },
  { "orphans list jobs", OPT::ORPHANS_LIST_JOBS },
  { "orphans list-jobs", OPT::ORPHANS_LIST_JOBS },
#endif
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
#ifdef WITH_RADOSGW_RADOS
  { "zone placement add", OPT::ZONE_PLACEMENT_ADD },
#endif
  { "zone placement modify", OPT::ZONE_PLACEMENT_MODIFY },
  { "zone placement rm", OPT::ZONE_PLACEMENT_RM },
  { "zone placement list", OPT::ZONE_PLACEMENT_LIST },
  { "zone placement get", OPT::ZONE_PLACEMENT_GET },
  { "caps add", OPT::CAPS_ADD },
  { "caps rm", OPT::CAPS_RM },
#ifdef WITH_RADOSGW_RADOS
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
  { "mdlog status", OPT::MDLOG_STATUS },
  { "sync error list", OPT::SYNC_ERROR_LIST },
  { "sync error trim", OPT::SYNC_ERROR_TRIM },
#endif
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
#ifdef WITH_RADOSGW_RADOS
  { "bilog trim", OPT::BILOG_TRIM },
  { "bilog status", OPT::BILOG_STATUS },
  { "bilog autotrim", OPT::BILOG_AUTOTRIM },
  { "data sync status", OPT::DATA_SYNC_STATUS },
  { "data sync init", OPT::DATA_SYNC_INIT },
  { "data sync run", OPT::DATA_SYNC_RUN },
#endif
  { "datalog list", OPT::DATALOG_LIST },
  { "datalog status", OPT::DATALOG_STATUS },
  { "datalog autotrim", OPT::DATALOG_AUTOTRIM },
  { "datalog trim", OPT::DATALOG_TRIM },
  { "datalog type", OPT::DATALOG_TYPE },
  { "datalog prune", OPT::DATALOG_PRUNE },
  { "datalog semaphore list", OPT::DATALOG_SEMAPHORE_LIST },
  { "datalog semaphore reset", OPT::DATALOG_SEMAPHORE_RESET },
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
  { "realm default rm", OPT::REALM_DEFAULT_RM },
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
#ifdef WITH_RADOSGW_RADOS
  { "sync status", OPT::SYNC_STATUS },
#endif
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
  { "role policy attach", OPT::ROLE_POLICY_ATTACH },
  { "role policy detach", OPT::ROLE_POLICY_DETACH },
  { "role policy list attached", OPT::ROLE_POLICY_LIST_ATTACHED },
  { "role update", OPT::ROLE_UPDATE },
#ifdef WITH_RADOSGW_RADOS
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
  { "reshardlog list", OPT::RESHARDLOG_LIST},
  { "reshardlog purge", OPT::RESHARDLOG_PURGE},
#endif
  { "topic list", OPT::PUBSUB_TOPIC_LIST },
  { "topic get", OPT::PUBSUB_TOPIC_GET },
  { "topic rm", OPT::PUBSUB_TOPIC_RM },
  { "notification list", OPT::PUBSUB_NOTIFICATION_LIST },
  { "notification get", OPT::PUBSUB_NOTIFICATION_GET },
  { "notification rm", OPT::PUBSUB_NOTIFICATION_RM },
#ifdef WITH_RADOSGW_RADOS
  { "topic stats", OPT::PUBSUB_TOPIC_STATS },
  { "topic dump", OPT::PUBSUB_TOPIC_DUMP },
#endif
  { "script put", OPT::SCRIPT_PUT },
  { "script get", OPT::SCRIPT_GET },
  { "script rm", OPT::SCRIPT_RM },
  { "script-package add", OPT::SCRIPT_PACKAGE_ADD },
  { "script-package rm", OPT::SCRIPT_PACKAGE_RM },
  { "script-package list", OPT::SCRIPT_PACKAGE_LIST },
  { "script-package reload", OPT::SCRIPT_PACKAGE_RELOAD },
  { "account create", OPT::ACCOUNT_CREATE },
  { "account modify", OPT::ACCOUNT_MODIFY },
  { "account get", OPT::ACCOUNT_GET },
  { "account stats", OPT::ACCOUNT_STATS },
  { "account rm", OPT::ACCOUNT_RM },
  { "account list", OPT::ACCOUNT_LIST },
  { "restore status", OPT::RESTORE_STATUS },
  { "restore list", OPT::RESTORE_LIST },
  { "global-cors get", OPT::GLOBAL_CORS_GET},
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
  if (type_str == "resharddeleted")
    return BIIndexType::ReshardDeleted;

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

class StoreDestructor {
  rgw::sal::Driver* driver;
  ceph::async::io_context_pool* pool;
public:
  explicit StoreDestructor(rgw::sal::Driver* _s,
			   ceph::async::io_context_pool* pool)
    : driver(_s), pool(pool) {}
  ~StoreDestructor() {
    driver->shutdown();
    pool->finish();
    DriverManager::close_storage(driver);
    rgw_http_client_cleanup();
  }
};

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

  constexpr auto READ_CHUNK=8196;
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

  // alternative defaults for radosgw-admin
  map<std::string,std::string> defaults = {
    { "rgw_thread_pool_size", "8" },
  };

  auto cct = rgw_global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
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
  string account_name;
  rgw_account_id account_id;
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
  std::string description;
  std::string policy_arn;
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
  enum generate_key_enum {
    OPTION_SET_FALSE = 0,
    OPTION_SET_TRUE  = 1,
    OPTION_NOT_SET   = 2,
  };

  generate_key_enum generate_key = OPTION_NOT_SET;
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
  int throttle_stat = false;
  int delete_child_objects = false;
  int fix = false;
  int remove_bad = false;
  int check_head_obj_locator = false;
  std::optional<int> max_buckets;
  std::optional<int> max_users;
  std::optional<int> max_roles;
  std::optional<int> max_groups;
  std::optional<int> max_access_keys;
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
  int account_root = false;
  bool account_root_specified = false;
  int shard_id = -1;
  bool specified_shard_id = false;
  std::optional<std::uint64_t> count;
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
#ifdef WITH_RADOSGW_RADOS
  int placement_inline_data = true;
  bool placement_inline_data_specified = false;
  bool format_arg_passed = false;
#endif

  int64_t max_objects = -1;
  int64_t max_size = -1;
  int64_t max_read_ops = 0;
  int64_t max_write_ops = 0;
  int64_t max_list_ops = 0;
  int64_t max_delete_ops = 0;
  int64_t max_read_bytes = 0;
  int64_t max_write_bytes = 0;
#ifdef WITH_RADOSGW_RADOS
  uint32_t max_bucket_index_ops = 0;
  uint32_t max_metadata_ops = 0;
#endif
  bool have_max_objects = false;
  bool have_max_size = false;
  bool have_max_write_ops = false;
  bool have_max_read_ops = false;
  bool have_max_list_ops = false;
  bool have_max_delete_ops = false;
  bool have_max_write_bytes = false;
  bool have_max_read_bytes = false;
#ifdef WITH_RADOSGW_RADOS
  bool have_max_bucket_index_ops = false;
  bool have_max_metadata_ops = false;
  std::string allow_bucket_list_file;
  std::string deny_bucket_list_file;
  std::string allow_storage_class_list_file;
  std::string deny_storage_class_list_file;
#endif
  int include_all = false;
  int allow_unordered = false;

  int sync_stats = false;
  int reset_stats = false;
  int bypass_gc = false;
  int warnings_only = false;
  int inconsistent_index = false;

  int verbose = false;

  int extra_info = false;

#ifdef WITH_RADOSGW_RADOS
  uint64_t min_rewrite_size = 4 * 1024 * 1024;
  uint64_t max_rewrite_size = ULLONG_MAX;
  uint64_t min_rewrite_stripe_size = 0;
#endif

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
#ifdef WITH_RADOSGW_RADOS
  uint64_t orphan_stale_secs = (24 * 3600);
#endif
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

  std::optional<string> index_pool;
  std::optional<string> data_pool;
  std::optional<string> data_extra_pool;
#ifdef WITH_RADOSGW_RADOS
  rgw::BucketIndexType placement_index_type = rgw::BucketIndexType::Normal;
  bool index_type_specified = false;
#endif

  std::optional<std::string> compression_type;

  string totp_serial;
  string totp_seed;
  string totp_seed_type = "hex";
  vector<string> totp_pin;
#ifdef WITH_RADOSGW_RADOS
  int totp_seconds = 0;
  int totp_window = 0;
  int trim_delay_ms = 0;
#endif

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
  std::optional<std::string> restore_status_filter;
  int show_restore_stats = false;

  // global CORS settings
  std::optional<std::string> gcors_allow_origins;
  std::optional<std::string> gcors_allow_methods;
  std::optional<std::string> gcors_allow_headers;
  std::optional<std::string> gcors_expose_headers;

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
    } else if (ceph_argparse_witharg(args, i, &val, "--account-name", (char*)NULL)) {
      account_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--account-id", (char*)NULL)) {
      account_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-users", (char*)NULL)) {
      max_users = ceph::parse<int>(val);
      if (!max_users) {
        cerr << "ERROR: failed to parse --max-users" << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-roles", (char*)NULL)) {
      max_roles = ceph::parse<int>(val);
      if (!max_roles) {
        cerr << "ERROR: failed to parse --max-roles" << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-groups", (char*)NULL)) {
      max_groups = ceph::parse<int>(val);
      if (!max_groups) {
        cerr << "ERROR: failed to parse --max-groups" << std::endl;
        return EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-access-keys", (char*)NULL)) {
      max_access_keys = ceph::parse<int>(val);
      if (!max_access_keys) {
        cerr << "ERROR: failed to parse --max-access-keys" << std::endl;
        return EINVAL;
      }
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
    } else if (ceph_argparse_witharg(args, i, &val, "--generate-key", (char*)NULL)) {
      key_type_str = val;
      if (key_type_str.compare("true") == 0) {
	generate_key = OPTION_SET_TRUE;
      } else if(key_type_str.compare("false") == 0) {
	generate_key = OPTION_SET_FALSE;
      } else {
        cerr << "wrong value for --generate-key: " << key_type_str << " please specify either true or false" << std::endl;
        exit(1);
      }
      // do nothing
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
    } else if (ceph_argparse_binary_flag(args, i, &account_root, NULL, "--account-root", (char*)NULL)) {
      account_root_specified = true;
    } else if (ceph_argparse_binary_flag(args, i, &verbose, NULL, "--verbose", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &staging, NULL, "--staging", (char*)NULL)) {
      // do nothing
    } else if (ceph_argparse_binary_flag(args, i, &commit, NULL, "--commit", (char*)NULL)) {
      // do nothing
#ifdef WITH_RADOSGW_RADOS
    } else if (ceph_argparse_witharg(args, i, &val, "--min-rewrite-size", (char*)NULL)) {
      min_rewrite_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--max-rewrite-size", (char*)NULL)) {
      max_rewrite_size = (uint64_t)atoll(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--min-rewrite-stripe-size", (char*)NULL)) {
      min_rewrite_stripe_size = (uint64_t)atoll(val.c_str());
#endif
    } else if (ceph_argparse_witharg(args, i, &val, "--max-buckets", (char*)NULL)) {
      max_buckets = ceph::parse<int>(val);
      if (!max_buckets) {
        cerr << "ERROR: failed to parse max buckets" << std::endl;
        return EINVAL;
      }
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
    } else if (ceph_argparse_witharg(args, i, &val, "--max-list-ops", (char*)NULL)) {
      max_list_ops = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max list requests: " << err << std::endl;
        return EINVAL;
      }
      have_max_list_ops = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-delete-ops", (char*)NULL)) {
      max_delete_ops = (int64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse max delete requests: " << err << std::endl;
        return EINVAL;
      }
      have_max_delete_ops = true;
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
    } else if (ceph_argparse_witharg(args, i, &val, "--max-bucket-index-ops", (char*)NULL)) {
#ifdef WITH_RADOSGW_RADOS
      max_bucket_index_ops = (int64_t)strict_strtoll(val.c_str(), 10, &err);
#endif
      if (!err.empty()) {
	cerr << "ERROR: failed to parse max bucket index ops: " << err << std::endl;
	return EINVAL;
      }
#ifdef WITH_RADOSGW_RADOS
      have_max_bucket_index_ops = true;
#endif
    } else if (ceph_argparse_witharg(args, i, &val, "--max-metadata-ops", (char*)NULL)) {
#ifdef WITH_RADOSGW_RADOS
      max_metadata_ops = (int64_t)strict_strtoll(val.c_str(), 10, &err);
#endif
      if (!err.empty()) {
	cerr << "ERROR: failed to parse max metadata ops: " << err << std::endl;
	return EINVAL;
      }
#ifdef WITH_RADOSGW_RADOS
      have_max_metadata_ops = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--allow-bucket-list", (char*)NULL)) {
      allow_bucket_list_file = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--deny-bucket-list", (char*)NULL)) {
      deny_bucket_list_file = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--allow-storage-class-list", (char*)NULL)) {
      allow_storage_class_list_file = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--deny-storage-class-list", (char*)NULL)) {
      deny_storage_class_list_file = val;
#endif
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
#ifdef WITH_RADOSGW_RADOS
    } else if (ceph_argparse_witharg(args, i, &val, "--orphan-stale-secs", (char*)NULL)) {
      orphan_stale_secs = (uint64_t)strict_strtoll(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse orphan stale secs: " << err << std::endl;
        return EINVAL;
      }
#endif
    } else if (ceph_argparse_witharg(args, i, &val, "--shard-id", (char*)NULL)) {
      shard_id = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse shard id: " << err << std::endl;
        return EINVAL;
      }
      specified_shard_id = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--count", (char*)NULL)) {
      count = strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cerr << "ERROR: failed to parse count: " << err << std::endl;
        return EINVAL;
      }
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
#ifdef WITH_RADOSGW_RADOS
      format_arg_passed = true;
#endif
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
    } else if (ceph_argparse_binary_flag(args, i, &throttle_stat, NULL, "--stat", (char*)NULL)) {
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
#ifdef WITH_RADOSGW_RADOS
    } else if (ceph_argparse_binary_flag(args, i, &placement_inline_data, NULL, "--placement-inline-data", (char*)NULL)) {
      placement_inline_data_specified = true;
#endif
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
#ifdef WITH_RADOSGW_RADOS
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
#endif
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
    } else if (ceph_argparse_witharg(args, i, &val, "--policy-arn", (char*)NULL)) {
      policy_arn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-session-duration", (char*)NULL)) {
      max_session_duration = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--description", (char*)NULL)) {
      description = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-serial", (char*)NULL)) {
      totp_serial = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-pin", (char*)NULL)) {
      totp_pin.push_back(val);
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-seed", (char*)NULL)) {
      totp_seed = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-seed-type", (char*)NULL)) {
      totp_seed_type = val;
#ifdef WITH_RADOSGW_RADOS
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-seconds", (char*)NULL)) {
      totp_seconds = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--totp-window", (char*)NULL)) {
      totp_window = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--trim-delay-ms", (char*)NULL)) {
      trim_delay_ms = atoi(val.c_str());
#endif
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
    } else if (ceph_argparse_witharg(args, i, &val, "--restore-status", (char*)NULL)) {
      restore_status_filter = val;
    } else if (ceph_argparse_binary_flag(args, i, &show_restore_stats, NULL, "--show-restore-stats", (char*)NULL)){
      // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--allow-origin", (char*)NULL)) {
      gcors_allow_origins = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--allow-methods", (char*)NULL)) {
      gcors_allow_methods = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--allow-headers", (char*)NULL)) {
      gcors_allow_headers = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--expose-headers", (char*)NULL)) {
      gcors_expose_headers = val;
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
  std::unique_ptr<rgw::SiteConfig> site;

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
#ifdef WITH_RADOSGW_RADOS
        case OPT::METADATA_GET:
        case OPT::METADATA_PUT:
        case OPT::METADATA_RM:
        case OPT::METADATA_LIST:
          metadata_key = extra_args[0];
          break;
#endif
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
#ifdef WITH_RADOSGW_RADOS
			 OPT::ZONE_PLACEMENT_ADD, 
#endif
			 OPT::ZONE_PLACEMENT_RM,
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
			 OPT::REALM_DEFAULT, OPT::REALM_DEFAULT_RM, OPT::REALM_PULL};

    std::set<OPT> readonly_ops_list = {
                         OPT::USER_INFO,
			 OPT::USER_STATS,
			 OPT::USER_LIST,
			 OPT::USER_POLICY_LIST_ATTACHED,
			 OPT::ACCOUNT_GET,
			 OPT::ACCOUNT_STATS,
			 OPT::ACCOUNT_LIST,
			 OPT::BUCKETS_LIST,
			 OPT::BUCKET_LIMIT_CHECK,
			 OPT::BUCKET_LAYOUT,
			 OPT::BUCKET_STATS,
#ifdef WITH_RADOSGW_RADOS
			 OPT::BUCKET_SYNC_CHECKPOINT,
			 OPT::BUCKET_SYNC_INFO,
			 OPT::BUCKET_SYNC_STATUS,
			 OPT::BUCKET_SYNC_MARKERS,
			 OPT::BUCKET_SHARD_OBJECTS,
			 OPT::BUCKET_OBJECT_SHARD,
			 OPT::LOG_LIST,
			 OPT::LOG_SHOW,
#endif
			 OPT::USAGE_SHOW,
			 OPT::OBJECT_STAT,
#ifdef WITH_RADOSGW_RADOS
			 OPT::OBJECT_MANIFEST,
			 OPT::BI_GET,
			 OPT::BI_LIST,
			 OPT::OLH_GET,
			 OPT::OLH_READLOG,
			 OPT::DEDUP_STATS,
			 OPT::DEDUP_ESTIMATE,
			 OPT::DEDUP_ABORT,     // TBD - not READ-ONLY
			 OPT::DEDUP_EXEC,   // TBD - not READ-ONLY
			 OPT::DEDUP_PAUSE,
			 OPT::DEDUP_RESUME,
			 OPT::DEDUP_THROTTLE,
			 OPT::GC_LIST,
#endif
			 OPT::LC_LIST,
#ifdef WITH_RADOSGW_RADOS
			 OPT::ORPHANS_LIST_JOBS,
#endif
			 OPT::ZONEGROUP_GET,
			 OPT::ZONEGROUP_LIST,
			 OPT::ZONEGROUP_PLACEMENT_LIST,
			 OPT::ZONEGROUP_PLACEMENT_GET,
			 OPT::ZONE_GET,
			 OPT::ZONE_LIST,
			 OPT::ZONE_PLACEMENT_LIST,
			 OPT::ZONE_PLACEMENT_GET,
#ifdef WITH_RADOSGW_RADOS
			 OPT::METADATA_GET,
			 OPT::METADATA_LIST,
			 OPT::METADATA_SYNC_STATUS,
			 OPT::MDLOG_LIST,
			 OPT::MDLOG_STATUS,
			 OPT::SYNC_ERROR_LIST,
#endif
			 OPT::SYNC_GROUP_GET,
			 OPT::SYNC_POLICY_GET,
			 OPT::BILOG_LIST,
#ifdef WITH_RADOSGW_RADOS
			 OPT::BILOG_STATUS,
			 OPT::DATA_SYNC_STATUS,
#endif
			 OPT::DATALOG_LIST,
			 OPT::DATALOG_SEMAPHORE_LIST,
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
#ifdef WITH_RADOSGW_RADOS
			 OPT::SYNC_STATUS,
#endif
			 OPT::ROLE_GET,
			 OPT::ROLE_LIST,
			 OPT::ROLE_POLICY_LIST,
			 OPT::ROLE_POLICY_GET,
			 OPT::ROLE_POLICY_LIST_ATTACHED,
#ifdef WITH_RADOSGW_RADOS
			 OPT::RESHARD_LIST,
			 OPT::RESHARD_STATUS,
#endif			 
			 OPT::PUBSUB_TOPIC_LIST,
       OPT::PUBSUB_NOTIFICATION_LIST,
			 OPT::PUBSUB_TOPIC_GET,
       OPT::PUBSUB_NOTIFICATION_GET,
#ifdef WITH_RADOSGW_RADOS
       OPT::PUBSUB_TOPIC_STATS  ,
       OPT::PUBSUB_TOPIC_DUMP  ,
#endif
			 OPT::SCRIPT_GET,
       OPT::RESTORE_STATUS,
       OPT::RESTORE_LIST,
    };

    std::set<OPT> gc_ops_list = {
#ifdef WITH_RADOSGW_RADOS
			 OPT::GC_LIST,
			 OPT::GC_PROCESS,
#endif
			 OPT::OBJECT_RM,
			 OPT::BUCKET_RM,  // --purge-objects
			 OPT::USER_RM,    // --purge-data
			 OPT::OBJECTS_EXPIRE,
			 OPT::OBJECTS_EXPIRE_STALE_RM,
#ifdef WITH_RADOSGW_RADOS
			 OPT::LC_PROCESS,
       OPT::BUCKET_SYNC_RUN,
       OPT::DATA_SYNC_RUN,
       OPT::BUCKET_REWRITE,
       OPT::OBJECT_REWRITE
#endif
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

    if (raw_storage_op) {
      site = rgw::SiteConfig::make_fake();
      driver = DriverManager::get_raw_storage(dpp(), g_ceph_context,
					      cfg, context_pool, *site, cfgstore.get());
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
                                        false,
                                        false,
					false, // No background tasks!
                                        null_yield,
					cfgstore.get(),
					need_cache && g_conf()->rgw_cache_enabled,
					need_gc, true /* admin */);
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
                          && opt_cmd != OPT::ROLE_POLICY_ATTACH
                          && opt_cmd != OPT::ROLE_POLICY_DETACH
                          && opt_cmd != OPT::ROLE_POLICY_LIST_ATTACHED
                          && opt_cmd != OPT::ROLE_UPDATE
#ifdef WITH_RADOSGW_RADOS
                          && opt_cmd != OPT::RESHARD_ADD
                          && opt_cmd != OPT::RESHARD_CANCEL
                          && opt_cmd != OPT::RESHARD_STATUS
#endif			  
                          && opt_cmd != OPT::PUBSUB_TOPIC_LIST
                          && opt_cmd != OPT::PUBSUB_NOTIFICATION_LIST
                          && opt_cmd != OPT::PUBSUB_TOPIC_GET
                          && opt_cmd != OPT::PUBSUB_NOTIFICATION_GET
                          && opt_cmd != OPT::PUBSUB_TOPIC_RM
                          && opt_cmd != OPT::PUBSUB_NOTIFICATION_RM
#ifdef WITH_RADOSGW_RADOS
                          && opt_cmd != OPT::PUBSUB_TOPIC_STATS
                          && opt_cmd != OPT::PUBSUB_TOPIC_DUMP
#endif
			  && opt_cmd != OPT::SCRIPT_PUT
			  && opt_cmd != OPT::SCRIPT_GET
			  && opt_cmd != OPT::SCRIPT_RM
                          && opt_cmd != OPT::ACCOUNT_CREATE
                          && opt_cmd != OPT::ACCOUNT_MODIFY
                          && opt_cmd != OPT::ACCOUNT_GET
                          && opt_cmd != OPT::ACCOUNT_STATS
                          && opt_cmd != OPT::ACCOUNT_RM
                          && opt_cmd != OPT::ACCOUNT_LIST) {
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
    if ((!access_key.empty()) && (gen_access_key || generate_key == OPTION_SET_TRUE)) {
        cerr << "ERROR: key parameter conflict, --access-key & --gen-access-key/generate-key" << std::endl;
        return EINVAL;
    }
    if ((!secret_key.empty()) && (gen_secret_key || generate_key == OPTION_SET_TRUE)) {
        cerr << "ERROR: key parameter conflict, --secret & --gen-secret/generate-key" << std::endl;
        return EINVAL;
    }
    if (generate_key == OPTION_SET_FALSE) {
      if ((!access_key.empty()) || gen_access_key || (!secret_key.empty()) || gen_secret_key) {
        cerr << "ERROR: key parameter conflict, if --generate-key is not set so no other key parameters can be set" << std::endl;
        return EINVAL;
      }
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

  StoreDestructor store_destructor(driver, &context_pool);

  auto fill_period_options = [&](rgw_admin_period_options& o) {
    o.command = opt_cmd;
    o.realm_id = &realm_id;
    o.realm_name = &realm_name;
    o.period_id = &period_id;
    o.period_epoch = &period_epoch;
    o.url = &url;
    o.access_key = &access_key;
    o.secret_key = &secret_key;
    o.remote = &remote;
    o.quota_scope = &quota_scope;
    o.ratelimit_scope = &ratelimit_scope;
    o.opt_region = &opt_region;
    o.commit = commit;
    o.staging = staging;
    o.yes_i_really_mean_it = yes_i_really_mean_it;
    o.have_max_read_ops = have_max_read_ops;
    o.have_max_write_ops = have_max_write_ops;
    o.have_max_list_ops = have_max_list_ops;
    o.have_max_delete_ops = have_max_delete_ops;
    o.have_max_read_bytes = have_max_read_bytes;
    o.have_max_write_bytes = have_max_write_bytes;
    o.have_max_size = have_max_size;
    o.have_max_objects = have_max_objects;
    o.max_read_ops = max_read_ops;
    o.max_write_ops = max_write_ops;
    o.max_list_ops = max_list_ops;
    o.max_delete_ops = max_delete_ops;
    o.max_read_bytes = max_read_bytes;
    o.max_write_bytes = max_write_bytes;
    o.max_size = max_size;
    o.max_objects = max_objects;
  };

  auto fill_realm_options = [&](rgw_admin_realm_options& o) {
    o.command = opt_cmd;
    o.realm_id = &realm_id;
    o.realm_name = &realm_name;
    o.realm_new_name = &realm_new_name;
    o.period_id = &period_id;
    o.period_epoch = &period_epoch;
    o.url = &url;
    o.access_key = &access_key;
    o.secret_key = &secret_key;
    o.remote = &remote;
    o.infile = &infile;
    o.opt_region = &opt_region;
    o.set_default = set_default;
    o.yes_i_really_mean_it = yes_i_really_mean_it;
  };

  auto fill_zonegroup_options = [&](rgw_admin_zonegroup_options& o) {
    o.command = opt_cmd;
    o.zonegroup_id = &zonegroup_id;
    o.zonegroup_name = &zonegroup_name;
    o.zonegroup_new_name = &zonegroup_new_name;
    o.zone_id = &zone_id;
    o.zone_name = &zone_name;
    o.realm_id = &realm_id;
    o.realm_name = &realm_name;
    o.placement_id = &placement_id;
    o.infile = &infile;
    o.tags = &tags;
    o.tags_add = &tags_add;
    o.tags_rm = &tags_rm;
    o.sync_from = &sync_from;
    o.sync_from_rm = &sync_from_rm;
    o.endpoints = &endpoints;
    o.master_zone = &master_zone;
    o.api_name = &api_name;
    o.tier_type = &tier_type;
    o.tier_type_specified = tier_type_specified;
    o.tier_config_add = &tier_config_add;
    o.tier_config_rm = &tier_config_rm;
    o.redirect_zone = &redirect_zone;
    o.opt_storage_class = &opt_storage_class;
    o.bucket_index_max_shards = &bucket_index_max_shards;
    o.sync_from_all_specified = sync_from_all_specified;
    o.redirect_zone_set = redirect_zone_set;
    o.sync_from_all = sync_from_all;
    o.set_default = set_default;
    o.read_only = read_only;
    o.is_master = is_master;
    o.is_master_set = is_master_set;
    o.is_read_only_set = is_read_only_set;
    o.yes_i_really_mean_it = yes_i_really_mean_it;
#ifdef WITH_RADOSGW_RADOS
    o.enable_features = &enable_features;
    o.disable_features = &disable_features;
#endif
  };

  auto fill_zone_options = [&](rgw_admin_zone_options& o) {
    o.command = opt_cmd;
    o.zonegroup_id = &zonegroup_id;
    o.zonegroup_name = &zonegroup_name;
    o.zone_id = &zone_id;
    o.zone_name = &zone_name;
    o.zone_new_name = &zone_new_name;
    o.realm_id = &realm_id;
    o.realm_name = &realm_name;
    o.placement_id = &placement_id;
    o.url = &url;
    o.access_key = &access_key;
    o.secret_key = &secret_key;
    o.infile = &infile;
    o.sync_from = &sync_from;
    o.sync_from_rm = &sync_from_rm;
    o.endpoints = &endpoints;
    o.master_zone = &master_zone;
    o.format = &format;
    o.api_name = &api_name;
    o.tier_type = &tier_type;
    o.tier_type_specified = tier_type_specified;
    o.tier_config_add = &tier_config_add;
    o.tier_config_rm = &tier_config_rm;
    o.index_pool = &index_pool;
    o.data_pool = &data_pool;
    o.data_extra_pool = &data_extra_pool;
    o.compression_type = &compression_type;
    o.bucket_index_max_shards = &bucket_index_max_shards;
    o.redirect_zone = &redirect_zone;
    o.redirect_zone_set = redirect_zone_set;
    o.placement_inline_data = placement_inline_data;
    o.placement_inline_data_specified = placement_inline_data_specified;
    o.opt_storage_class = &opt_storage_class;
    o.opt_region = &opt_region;
    o.sync_from_all_specified = sync_from_all_specified;
    o.set_default = set_default;
    o.read_only = read_only;
    o.is_master = is_master;
    o.is_master_set = is_master_set;
    o.is_read_only_set = is_read_only_set;
    o.sync_from_all = sync_from_all;
    o.yes_i_really_mean_it = yes_i_really_mean_it;
    o.num_shards_specified = num_shards_specified;
    o.num_shards = num_shards;
#ifdef WITH_RADOSGW_RADOS
    o.placement_index_type = &placement_index_type;
    o.index_type_specified = index_type_specified;
    o.enable_features = &enable_features;
    o.disable_features = &disable_features;
#endif
  };

  auto dispatch_raw_storage = [&]() -> int {
    switch (opt_cmd) {
    case OPT::PERIOD_DELETE:
    case OPT::PERIOD_GET:
    case OPT::PERIOD_GET_CURRENT:
    case OPT::PERIOD_PULL:
    case OPT::PERIOD_LIST:
    case OPT::PERIOD_UPDATE:
    case OPT::GLOBAL_QUOTA_GET:
    case OPT::GLOBAL_QUOTA_SET:
    case OPT::GLOBAL_QUOTA_ENABLE:
    case OPT::GLOBAL_QUOTA_DISABLE:
    case OPT::GLOBAL_RATELIMIT_GET:
    case OPT::GLOBAL_RATELIMIT_SET:
    case OPT::GLOBAL_RATELIMIT_ENABLE:
    case OPT::GLOBAL_RATELIMIT_DISABLE: {
      rgw_admin_period_options o;
      fill_period_options(o);
      return rgw_admin_period(dpp(), driver, cfgstore.get(), *site,
                              formatter.get(), o);
    }
    case OPT::REALM_CREATE:
    case OPT::REALM_DELETE:
    case OPT::REALM_GET:
    case OPT::REALM_GET_DEFAULT:
    case OPT::REALM_LIST:
    case OPT::REALM_LIST_PERIODS:
    case OPT::REALM_RENAME:
    case OPT::REALM_SET:
    case OPT::REALM_DEFAULT:
    case OPT::REALM_DEFAULT_RM:
    case OPT::REALM_PULL: {
      rgw_admin_realm_options o;
      fill_realm_options(o);
      return rgw_admin_realm(dpp(), driver, cfgstore.get(), *site,
                             formatter.get(), o);
    }
    case OPT::ZONEGROUP_ADD:
    case OPT::ZONEGROUP_CREATE:
    case OPT::ZONEGROUP_DEFAULT:
    case OPT::ZONEGROUP_DELETE:
    case OPT::ZONEGROUP_GET:
    case OPT::ZONEGROUP_MODIFY:
    case OPT::ZONEGROUP_SET:
    case OPT::ZONEGROUP_LIST:
    case OPT::ZONEGROUP_REMOVE:
    case OPT::ZONEGROUP_RENAME:
    case OPT::ZONEGROUP_PLACEMENT_ADD:
    case OPT::ZONEGROUP_PLACEMENT_MODIFY:
    case OPT::ZONEGROUP_PLACEMENT_RM:
    case OPT::ZONEGROUP_PLACEMENT_LIST:
    case OPT::ZONEGROUP_PLACEMENT_GET:
    case OPT::ZONEGROUP_PLACEMENT_DEFAULT: {
      rgw_admin_zonegroup_options o;
      fill_zonegroup_options(o);
      return rgw_admin_zonegroup(dpp(), driver, cfgstore.get(), *site,
                                 formatter.get(), o);
    }
    case OPT::ZONE_CREATE:
    case OPT::ZONE_DELETE:
    case OPT::ZONE_GET:
    case OPT::ZONE_MODIFY:
    case OPT::ZONE_SET:
    case OPT::ZONE_LIST:
    case OPT::ZONE_RENAME:
    case OPT::ZONE_DEFAULT:
#ifdef WITH_RADOSGW_RADOS
    case OPT::ZONE_PLACEMENT_ADD:
#endif
    case OPT::ZONE_PLACEMENT_MODIFY:
    case OPT::ZONE_PLACEMENT_RM:
    case OPT::ZONE_PLACEMENT_LIST:
    case OPT::ZONE_PLACEMENT_GET: {
      rgw_admin_zone_options o;
      fill_zone_options(o);
      return rgw_admin_zone(dpp(), driver, cfgstore.get(), *site,
                            formatter.get(), o);
    }
    default:
      cerr << "internal error: unhandled raw storage command" << std::endl;
      return EINVAL;
    }
  };

  if (raw_storage_op) {
    return dispatch_raw_storage();
  }

  resolve_zone_id_opt(opt_effective_zone_name, opt_effective_zone_id);
  resolve_zone_id_opt(opt_source_zone_name, opt_source_zone_id);
  resolve_zone_id_opt(opt_dest_zone_name, opt_dest_zone_id);
  resolve_zone_ids_opt(opt_zone_names, opt_zone_ids);
  resolve_zone_ids_opt(opt_source_zone_names, opt_source_zone_ids);
  resolve_zone_ids_opt(opt_dest_zone_names, opt_dest_zone_ids);

  bool non_master_cmd = (!driver->is_meta_master() && !yes_i_really_mean_it);
  std::set<OPT> non_master_ops_list = {OPT::ACCOUNT_CREATE,
                                        OPT::ACCOUNT_MODIFY, OPT::ACCOUNT_RM,
                                        OPT::USER_CREATE, OPT::USER_RM,
                                        OPT::USER_MODIFY, OPT::USER_ENABLE,
                                        OPT::USER_SUSPEND, OPT::SUBUSER_CREATE,
                                        OPT::SUBUSER_MODIFY, OPT::SUBUSER_RM,
#ifdef WITH_RADOSGW_RADOS
                                        OPT::BUCKET_LINK, OPT::BUCKET_UNLINK,
#endif
                                        OPT::BUCKET_CHOWN,
                                        OPT::BUCKET_SUSPEND,
                                        OPT::BUCKET_UNSUSPEND,
#ifdef WITH_RADOSGW_RADOS
                                        OPT::METADATA_PUT,
                                        OPT::METADATA_RM,
				       	                OPT::MFA_CREATE,
                                        OPT::MFA_REMOVE, OPT::MFA_RESYNC,
#endif
                                        OPT::CAPS_ADD, OPT::CAPS_RM,
                                        OPT::ROLE_CREATE, OPT::ROLE_DELETE,
                                        OPT::ROLE_POLICY_PUT, OPT::ROLE_POLICY_DELETE,
                                        OPT::ROLE_POLICY_ATTACH, OPT::ROLE_POLICY_DETACH,
                                        OPT::USER_POLICY_ATTACH, OPT::USER_POLICY_DETACH,
                                        OPT::RATELIMIT_SET, OPT::RATELIMIT_ENABLE, OPT::RATELIMIT_DISABLE,
                                        OPT::QUOTA_SET, OPT::QUOTA_ENABLE, OPT::QUOTA_DISABLE};

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

  if (max_buckets)
    user_op.set_max_buckets(*max_buckets);

  if (admin_specified)
     user_op.set_admin(admin);

  if (system_specified)
    user_op.set_system(system);

  if (account_root_specified)
    user_op.set_account_root(account_root);

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
  user_op.path = path;

  user_op.account_id = account_id;
  bucket_op.account_id = account_id;

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
  case OPT::USER_CREATE:
  case OPT::USER_RM:
  case OPT::USER_RENAME:
  case OPT::USER_ENABLE:
  case OPT::USER_SUSPEND:
  case OPT::USER_MODIFY:
  case OPT::SUBUSER_CREATE:
  case OPT::SUBUSER_MODIFY:
  case OPT::SUBUSER_RM:
  case OPT::KEY_CREATE:
  case OPT::KEY_RM:
  case OPT::CAPS_ADD:
  case OPT::CAPS_RM: {
    rgw_admin_user_mutate_options mutate_opts;
    mutate_opts.command = opt_cmd;
    mutate_opts.access_key = &access_key;
    mutate_opts.subuser = &subuser;
    mutate_opts.yes_i_really_mean_it = yes_i_really_mean_it;
    mutate_opts.generate_key = static_cast<int>(generate_key);
    ret = rgw_admin_user_mutate(dpp(), driver, formatter.get(), ruser,
                                user_op, user, mutate_opts, err_msg);
    if (ret != 0) {
      return ret;
    }
    output_user_info = false;
    break;
  }
  case OPT::PERIOD_PUSH:
  case OPT::PERIOD_UPDATE:
  case OPT::PERIOD_COMMIT: {
    rgw_admin_period_options o;
    fill_period_options(o);
    return rgw_admin_period(dpp(), driver, cfgstore.get(), *site,
                            formatter.get(), o);
  }

  case OPT::ROLE_CREATE:
  case OPT::ROLE_DELETE:
  case OPT::ROLE_GET:
  case OPT::ROLE_TRUST_POLICY_MODIFY:
  case OPT::ROLE_LIST:
  case OPT::ROLE_POLICY_PUT:
  case OPT::ROLE_POLICY_LIST:
  case OPT::ROLE_POLICY_GET:
  case OPT::ROLE_POLICY_DELETE:
  case OPT::ROLE_POLICY_ATTACH:
  case OPT::ROLE_POLICY_DETACH:
  case OPT::ROLE_POLICY_LIST_ATTACHED:
  case OPT::ROLE_UPDATE: {
    rgw_admin_role_options ropts;
    ropts.command = opt_cmd;
    ropts.role_name = &role_name;
    ropts.tenant = &tenant;
    ropts.account_id = &account_id;
    ropts.path = &path;
    ropts.assume_role_doc = &assume_role_doc;
    ropts.perm_policy_doc = &perm_policy_doc;
    ropts.policy_name = &policy_name;
    ropts.policy_arn = &policy_arn;
    ropts.description = &description;
    ropts.path_prefix = &path_prefix;
    ropts.max_session_duration = &max_session_duration;
    ropts.marker = &marker;
    ropts.infile = &infile;
    ropts.max_entries = max_entries;
    ropts.max_entries_specified = max_entries_specified;
    return rgw_admin_role(dpp(), driver, formatter.get(), ropts);
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


  if (opt_cmd == OPT::BUCKET_LOGGING_FLUSH ||
      opt_cmd == OPT::BUCKET_LOGGING_INFO
#ifdef WITH_RADOSGW_RADOS
      || opt_cmd == OPT::BUCKET_LOGGING_LIST
#endif
      ) {
    rgw_admin_bucket_logging_options blog_opts;
    blog_opts.command = opt_cmd;
    blog_opts.tenant = &tenant;
    blog_opts.bucket_name = &bucket_name;
    blog_opts.bucket_id = &bucket_id;
    ret = rgw_admin_bucket_logging(dpp(), driver, formatter.get(), bucket, blog_opts);
    if (ret != 0) {
      return ret;
    }
  }

#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::OLH_GET || opt_cmd == OPT::OLH_READLOG) {
    rgw_admin_olh_options olh_opts;
    olh_opts.command = opt_cmd;
    olh_opts.tenant = &tenant;
    olh_opts.bucket_name = &bucket_name;
    olh_opts.bucket_id = &bucket_id;
    olh_opts.object = &object;
    ret = rgw_admin_olh(dpp(), driver, formatter.get(), bucket, olh_opts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::BI_GET || opt_cmd == OPT::BI_PUT ||
      opt_cmd == OPT::BI_LIST || opt_cmd == OPT::BI_PURGE) {
    rgw_admin_bi_options bi_opts;
    bi_opts.command = opt_cmd;
    bi_opts.tenant = &tenant;
    bi_opts.bucket_name = &bucket_name;
    bi_opts.bucket_id = &bucket_id;
    bi_opts.object = &object;
    bi_opts.object_version = &object_version;
    bi_opts.infile = &infile;
    bi_opts.marker = &marker;
    bi_opts.max_entries = max_entries;
    bi_opts.shard_id = shard_id;
    bi_opts.bi_index_type = bi_index_type;
    bi_opts.max_entries_specified = max_entries_specified;
    bi_opts.specified_shard_id = specified_shard_id;
    bi_opts.yes_i_really_mean_it = yes_i_really_mean_it;
    ret = rgw_admin_bi(dpp(), driver, formatter.get(), bucket, bi_opts);
    if (ret != 0) {
      return ret;
    }
  }
#endif

  if (opt_cmd == OPT::OBJECT_PUT ||
      opt_cmd == OPT::OBJECT_RM ||
#ifdef WITH_RADOSGW_RADOS
      opt_cmd == OPT::OBJECT_REWRITE ||
      opt_cmd == OPT::OBJECT_REINDEX ||
#endif
      opt_cmd == OPT::OBJECTS_EXPIRE ||
      opt_cmd == OPT::OBJECTS_EXPIRE_STALE_LIST ||
      opt_cmd == OPT::OBJECTS_EXPIRE_STALE_RM ||
      opt_cmd == OPT::OBJECT_UNLINK ||
      opt_cmd == OPT::OBJECT_STAT ||
#ifdef WITH_RADOSGW_RADOS
      opt_cmd == OPT::OBJECT_MANIFEST
#endif
      ) {
    rgw_admin_object_options oopts;
    oopts.command = opt_cmd;
    oopts.tenant = &tenant;
    oopts.bucket_name = &bucket_name;
    oopts.bucket_id = &bucket_id;
    oopts.object = &object;
    oopts.object_version = &object_version;
    oopts.infile = &infile;
    oopts.objects_file = &objects_file;
    oopts.end_date = &end_date;
    oopts.start_date = &start_date;
    oopts.marker = &marker;
    oopts.max_entries = max_entries;
    oopts.shard_id = shard_id;
    oopts.max_entries_specified = max_entries_specified;
    oopts.specified_shard_id = specified_shard_id;
    oopts.min_rewrite_size = min_rewrite_size;
    oopts.max_rewrite_size = max_rewrite_size;
    oopts.min_rewrite_stripe_size = min_rewrite_stripe_size;
    oopts.yes_i_really_mean_it = yes_i_really_mean_it;
    oopts.fix = fix;
    oopts.remove_bad = remove_bad;
    ret = rgw_admin_object(dpp(), driver, *site, formatter.get(), stream_flusher,
                           bucket_op, bucket, oopts);
    if (ret != 0) {
      return ret;
    }
  }

#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::RESHARD_ADD ||
      opt_cmd == OPT::RESHARD_LIST ||
      opt_cmd == OPT::RESHARD_STATUS ||
      opt_cmd == OPT::RESHARD_PROCESS ||
      opt_cmd == OPT::RESHARD_CANCEL ||
      opt_cmd == OPT::RESHARD_STALE_INSTANCES_LIST ||
      opt_cmd == OPT::RESHARD_STALE_INSTANCES_DELETE ||
      opt_cmd == OPT::RESHARDLOG_LIST ||
      opt_cmd == OPT::RESHARDLOG_PURGE) {
    rgw_admin_reshard_options ropts;
    ropts.command = opt_cmd;
    ropts.tenant = &tenant;
    ropts.bucket_name = &bucket_name;
    ropts.bucket_id = &bucket_id;
    ropts.marker = &marker;
    ropts.max_entries = max_entries;
    ropts.num_shards = num_shards;
    ropts.shard_id = shard_id;
    ropts.num_shards_specified = num_shards_specified;
    ropts.max_entries_specified = max_entries_specified;
    ropts.specified_shard_id = specified_shard_id;
    ropts.yes_i_really_mean_it = yes_i_really_mean_it;
    ret = rgw_admin_reshard(dpp(), driver, formatter.get(), stream_flusher,
                            bucket_op, bucket, ropts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::DEDUP_STATS ||
      opt_cmd == OPT::DEDUP_ESTIMATE ||
      opt_cmd == OPT::DEDUP_ABORT ||
      opt_cmd == OPT::DEDUP_PAUSE ||
      opt_cmd == OPT::DEDUP_RESUME ||
      opt_cmd == OPT::DEDUP_THROTTLE ||
      opt_cmd == OPT::DEDUP_EXEC) {
    rgw_admin_dedup_options dopts;
    dopts.command = opt_cmd;
    dopts.allow_bucket_list_file = &allow_bucket_list_file;
    dopts.deny_bucket_list_file = &deny_bucket_list_file;
    dopts.allow_storage_class_list_file = &allow_storage_class_list_file;
    dopts.deny_storage_class_list_file = &deny_storage_class_list_file;
    dopts.yes_i_really_mean_it = yes_i_really_mean_it;
    dopts.throttle_stat = throttle_stat;
    dopts.have_max_bucket_index_ops = have_max_bucket_index_ops;
    dopts.have_max_metadata_ops = have_max_metadata_ops;
    dopts.max_bucket_index_ops = max_bucket_index_ops;
    dopts.max_metadata_ops = max_metadata_ops;
    ret = rgw_admin_dedup(dpp(), driver, formatter.get(), dopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::GC_LIST || opt_cmd == OPT::GC_PROCESS) {
    rgw_admin_gc_options gopts;
    gopts.command = opt_cmd;
    gopts.marker = &marker;
    gopts.shard_id = shard_id;
    gopts.specified_shard_id = specified_shard_id;
    gopts.include_all = include_all;
    ret = rgw_admin_gc(dpp(), driver, formatter.get(), gopts);
    if (ret != 0) {
      return ret;
    }
  }
#endif

  if (opt_cmd == OPT::LC_LIST || opt_cmd == OPT::LC_GET ||
#ifdef WITH_RADOSGW_RADOS
      opt_cmd == OPT::LC_PROCESS ||
#endif
      opt_cmd == OPT::LC_RESHARD_FIX) {
    rgw_admin_lc_options lopts;
    lopts.command = opt_cmd;
    lopts.tenant = &tenant;
    lopts.bucket_name = &bucket_name;
    lopts.bucket_id = &bucket_id;
    lopts.max_entries = max_entries;
    ret = rgw_admin_lc(dpp(), driver, formatter.get(), stream_flusher,
                       bucket_op, bucket, lopts);
    if (ret != 0) {
      return ret;
    }
  }

#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::BUCKET_SYNC_INIT ||
      opt_cmd == OPT::BUCKET_SYNC_CHECKPOINT ||
      opt_cmd == OPT::BUCKET_SYNC_DISABLE ||
      opt_cmd == OPT::BUCKET_SYNC_ENABLE ||
      opt_cmd == OPT::BUCKET_SYNC_INFO ||
      opt_cmd == OPT::BUCKET_SYNC_STATUS ||
      opt_cmd == OPT::BUCKET_SYNC_MARKERS ||
      opt_cmd == OPT::BUCKET_SYNC_RUN) {
    rgw_admin_bucket_sync_options bsopts;
    bsopts.command = opt_cmd;
    bsopts.tenant = &tenant;
    bsopts.bucket_name = &bucket_name;
    bsopts.bucket_id = &bucket_id;
    bsopts.source_zone = &source_zone;
    bsopts.opt_source_bucket = &opt_source_bucket;
    bsopts.bucket_op = &bucket_op;
    bsopts.opt_retry_delay_ms = opt_retry_delay_ms;
    bsopts.opt_timeout_sec = opt_timeout_sec;
    bsopts.extra_info = extra_info;
    bsopts.format_arg_passed = format_arg_passed;
    ret = rgw_admin_bucket_sync(dpp(), driver, formatter.get(), bucket_op,
                                bucket, bsopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::BILOG_LIST ||
      opt_cmd == OPT::BILOG_TRIM ||
      opt_cmd == OPT::BILOG_STATUS ||
      opt_cmd == OPT::BILOG_AUTOTRIM) {
    rgw_admin_bilog_options blopts;
    blopts.command = opt_cmd;
    blopts.tenant = &tenant;
    blopts.bucket_name = &bucket_name;
    blopts.bucket_id = &bucket_id;
    blopts.marker = &marker;
    blopts.start_marker = &start_marker;
    blopts.end_marker = &end_marker;
    blopts.gen = &gen;
    blopts.max_entries = max_entries;
    blopts.shard_id = shard_id;
    blopts.yes_i_really_mean_it = yes_i_really_mean_it;
    ret = rgw_admin_bilog(dpp(), driver, formatter.get(), bucket, blopts);
    if (ret != 0) {
      return ret;
    }
  }
#endif

  if (opt_cmd == OPT::GLOBAL_CORS_GET) {
    rgw_admin_cors_options copts;
    copts.command = opt_cmd;
    ret = rgw_admin_cors(formatter.get(), copts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::BUCKET_LIMIT_CHECK ||
      opt_cmd == OPT::BUCKETS_LIST ||
#ifdef WITH_RADOSGW_RADOS
      opt_cmd == OPT::BUCKET_RADOS_LIST ||
      opt_cmd == OPT::BUCKET_LINK ||
      opt_cmd == OPT::BUCKET_UNLINK ||
      opt_cmd == OPT::BUCKET_REWRITE ||
      opt_cmd == OPT::BUCKET_RESHARD ||
#endif
      opt_cmd == OPT::BUCKET_LAYOUT ||
      opt_cmd == OPT::BUCKET_STATS ||
      opt_cmd == OPT::BUCKET_SHARD_OBJECTS ||
      opt_cmd == OPT::BUCKET_OBJECT_SHARD ||
      opt_cmd == OPT::BUCKET_CHOWN ||
      opt_cmd == OPT::BUCKET_SET_MIN_SHARDS ||
#ifdef WITH_RADOSGW_RADOS
      opt_cmd == OPT::BUCKET_CHECK ||
      opt_cmd == OPT::BUCKET_CHECK_OLH ||
      opt_cmd == OPT::BUCKET_CHECK_UNLINKED ||
#endif
      opt_cmd == OPT::BUCKET_RM ||
      opt_cmd == OPT::BUCKET_SUSPEND ||
      opt_cmd == OPT::BUCKET_UNSUSPEND ||
      opt_cmd == OPT::POLICY
      ) {
    rgw_admin_bucket_options bopts;
    bopts.command = opt_cmd;
    bopts.tenant = &tenant;
    bopts.bucket_name = &bucket_name;
    bopts.bucket_id = &bucket_id;
    bopts.object = &object;
    bopts.object_version = &object_version;
    bopts.marker = &marker;
    bopts.source_zone = &source_zone;
    bopts.metadata_key = &metadata_key;
    bopts.err = &err;
    bopts.new_bucket_name = &new_bucket_name;
    bopts.account_id = &account_id;
    bopts.format = &format;
    bopts.start_date = &start_date;
    bopts.end_date = &end_date;
    bopts.opt_prefix = &opt_prefix;
    bopts.opt_source_bucket = &opt_source_bucket;
    bopts.inject_error_at = &inject_error_at;
    bopts.inject_error_code = &inject_error_code;
    bopts.inject_abort_at = &inject_abort_at;
    bopts.inject_delay_at = &inject_delay_at;
    bopts.inject_delay = &inject_delay;
    bopts.rgw_obj_fs = &rgw_obj_fs;
    bopts.ret = &ret;
    bopts.max_entries = max_entries;
    bopts.max_concurrent_ios = max_concurrent_ios;
    bopts.orphan_stale_secs = orphan_stale_secs;
    bopts.num_shards = num_shards;
    bopts.shard_id = shard_id;
    bopts.min_age = min_age;
    bopts.min_rewrite_size = min_rewrite_size;
    bopts.max_rewrite_size = max_rewrite_size;
    bopts.min_rewrite_stripe_size = min_rewrite_stripe_size;
    bopts.opt_retry_delay_ms = opt_retry_delay_ms;
    bopts.opt_timeout_sec = opt_timeout_sec;
    bopts.max_entries_specified = max_entries_specified;
    bopts.warnings_only = warnings_only;
    bopts.allow_unordered = allow_unordered;
    bopts.show_restore_stats = show_restore_stats;
    bopts.yes_i_really_mean_it = yes_i_really_mean_it;
    bopts.bypass_gc = bypass_gc;
    bopts.inconsistent_index = inconsistent_index;
    bopts.num_shards_specified = num_shards_specified;
    bopts.specified_shard_id = specified_shard_id;
    bopts.fix = fix;
    bopts.dump_keys = dump_keys;
    bopts.hide_progress = hide_progress;
    bopts.extra_info = extra_info;
    bopts.verbose = verbose;
    bopts.format_arg_passed = format_arg_passed;
    bopts.check_head_obj_locator = check_head_obj_locator;
    bopts.remove_bad = remove_bad;
    ret = rgw_admin_bucket(dpp(), driver, *site, formatter.get(), stream_flusher,
                           user, user_op, bucket_op, bucket, bopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::USER_POLICY_ATTACH ||
      opt_cmd == OPT::USER_POLICY_DETACH ||
      opt_cmd == OPT::USER_POLICY_LIST_ATTACHED) {
    rgw_admin_user_policy_options upopts;
    upopts.command = opt_cmd;
    upopts.policy_arn = &policy_arn;
    ret = rgw_admin_user_policy(dpp(), driver, formatter.get(), user, upopts);
    if (ret != 0) {
      return ret;
    }
  }

#ifdef WITH_RADOSGW_RADOS
#endif


#ifdef WITH_RADOSGW_RADOS
#endif


#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::BUCKET_RESYNC_ENCRYPTED_MULTIPART) {
    rgw_admin_bucket_resync_encrypted_multipart_options remopts;
    remopts.command = opt_cmd;
    remopts.tenant = &tenant;
    remopts.bucket_name = &bucket_name;
    remopts.bucket_id = &bucket_id;
    remopts.marker = &marker;
    remopts.yes_i_really_mean_it = yes_i_really_mean_it;
    ret = rgw_admin_bucket_resync_encrypted_multipart(
        dpp(), driver, formatter.get(), stream_flusher, bucket, remopts);
    if (ret != 0) {
      return ret;
    }
  }
#endif



#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::LOG_LIST ||
      opt_cmd == OPT::LOG_SHOW ||
      opt_cmd == OPT::LOG_RM) {
    rgw_admin_log_options log_opts;
    log_opts.command = opt_cmd;
    log_opts.date = &date;
    log_opts.object = &object;
    log_opts.bucket_name = &bucket_name;
    log_opts.bucket_id = &bucket_id;
    log_opts.show_log_entries = show_log_entries;
    log_opts.show_log_sum = show_log_sum;
    log_opts.skip_zero_entries = skip_zero_entries;
    int ret = rgw_admin_log(dpp(), driver, formatter.get(), log_opts);
    if (ret != 0) {
      return ret;
    }
  }
#endif

  if (opt_cmd == OPT::USAGE_SHOW ||
      opt_cmd == OPT::USAGE_TRIM ||
      opt_cmd == OPT::USAGE_CLEAR) {
    rgw_admin_usage_options usage_opts;
    usage_opts.command = opt_cmd;
    usage_opts.tenant = &tenant;
    usage_opts.bucket_name = &bucket_name;
    usage_opts.bucket_id = &bucket_id;
    usage_opts.start_date = &start_date;
    usage_opts.end_date = &end_date;
    usage_opts.categories = &categories;
    usage_opts.show_log_entries = show_log_entries;
    usage_opts.show_log_sum = show_log_sum;
    usage_opts.yes_i_really_mean_it = yes_i_really_mean_it;
    ret = rgw_admin_usage(dpp(), driver, formatter.get(), stream_flusher,
                          user, bucket, usage_opts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::ACCOUNT_CREATE ||
      opt_cmd == OPT::ACCOUNT_MODIFY ||
      opt_cmd == OPT::ACCOUNT_GET ||
      opt_cmd == OPT::ACCOUNT_STATS ||
      opt_cmd == OPT::ACCOUNT_RM ||
      opt_cmd == OPT::ACCOUNT_LIST) {
    rgw_admin_account_options account_opts;
    account_opts.command = opt_cmd;
    account_opts.tenant = tenant;
    account_opts.account_id = account_id;
    account_opts.account_name = account_name;
    account_opts.user_email = user_email;
    account_opts.marker = marker;
    account_opts.max_users = &max_users;
    account_opts.max_roles = &max_roles;
    account_opts.max_groups = &max_groups;
    account_opts.max_access_keys = &max_access_keys;
    account_opts.max_buckets = &max_buckets;
    account_opts.purge_data = static_cast<bool>(purge_data);
    account_opts.sync_stats = sync_stats;
    account_opts.reset_stats = reset_stats;
    account_opts.max_entries = max_entries;
    account_opts.max_entries_specified = max_entries_specified;
    ret = rgw_admin_account(dpp(), driver, stream_flusher, account_opts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::QUOTA_SET || opt_cmd == OPT::QUOTA_ENABLE ||
      opt_cmd == OPT::QUOTA_DISABLE || opt_cmd == OPT::RATELIMIT_SET ||
      opt_cmd == OPT::RATELIMIT_ENABLE || opt_cmd == OPT::RATELIMIT_DISABLE ||
      opt_cmd == OPT::RATELIMIT_GET) {
    rgw_admin_quota_ratelimit_options qopts;
    qopts.command = opt_cmd;
    qopts.tenant = &tenant;
    qopts.bucket_name = &bucket_name;
    qopts.account_id = &account_id;
    qopts.account_name = &account_name;
    qopts.quota_scope = &quota_scope;
    qopts.ratelimit_scope = &ratelimit_scope;
    qopts.max_size = max_size;
    qopts.max_objects = max_objects;
    qopts.max_read_ops = max_read_ops;
    qopts.max_write_ops = max_write_ops;
    qopts.max_list_ops = max_list_ops;
    qopts.max_delete_ops = max_delete_ops;
    qopts.max_read_bytes = max_read_bytes;
    qopts.max_write_bytes = max_write_bytes;
    qopts.have_max_size = have_max_size;
    qopts.have_max_objects = have_max_objects;
    qopts.have_max_read_ops = have_max_read_ops;
    qopts.have_max_write_ops = have_max_write_ops;
    qopts.have_max_list_ops = have_max_list_ops;
    qopts.have_max_delete_ops = have_max_delete_ops;
    qopts.have_max_read_bytes = have_max_read_bytes;
    qopts.have_max_write_bytes = have_max_write_bytes;
    ret = rgw_admin_quota_ratelimit(dpp(), driver, stream_flusher, formatter.get(),
                                    ruser, user_op, user, qopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::SCRIPT_PUT || opt_cmd == OPT::SCRIPT_GET ||
      opt_cmd == OPT::SCRIPT_RM ||
#ifdef WITH_RADOSGW_LUA_PACKAGES
      opt_cmd == OPT::SCRIPT_PACKAGE_ADD ||
      opt_cmd == OPT::SCRIPT_PACKAGE_RM ||
      opt_cmd == OPT::SCRIPT_PACKAGE_LIST ||
      opt_cmd == OPT::SCRIPT_PACKAGE_RELOAD ||
#endif
      false) {
    rgw_admin_script_options sopts;
    sopts.command = opt_cmd;
    sopts.tenant = &tenant;
    sopts.infile = &infile;
    sopts.script_package = &script_package;
    sopts.str_script_ctx = &str_script_ctx;
    sopts.allow_compilation = allow_compilation;
    ret = rgw_admin_script(dpp(), driver, sopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::PUBSUB_NOTIFICATION_LIST ||
      opt_cmd == OPT::PUBSUB_TOPIC_LIST ||
      opt_cmd == OPT::PUBSUB_TOPIC_GET ||
      opt_cmd == OPT::PUBSUB_NOTIFICATION_GET ||
      opt_cmd == OPT::PUBSUB_TOPIC_RM ||
      opt_cmd == OPT::PUBSUB_NOTIFICATION_RM
#ifdef WITH_RADOSGW_RADOS
      || opt_cmd == OPT::PUBSUB_TOPIC_STATS
      || opt_cmd == OPT::PUBSUB_TOPIC_DUMP
#endif
      ) {
    rgw_admin_pubsub_options pubsub_opts;
    pubsub_opts.command = opt_cmd;
    pubsub_opts.tenant = tenant;
    pubsub_opts.account_id = account_id;
    pubsub_opts.bucket_name = bucket_name;
    pubsub_opts.bucket_id = bucket_id;
    pubsub_opts.topic_name = topic_name;
    pubsub_opts.notification_id = notification_id;
    pubsub_opts.marker = marker;
    pubsub_opts.max_entries = max_entries;
    pubsub_opts.max_entries_specified = max_entries_specified;
    ret = rgw_admin_pubsub(dpp(), driver, *site, user.get(), formatter.get(),
                           pubsub_opts);
    if (ret != 0) {
      return ret;
    }
  }







#ifdef WITH_RADOSGW_RADOS
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
#endif

  if (opt_cmd == OPT::USER_CHECK ||
      opt_cmd == OPT::USER_STATS ||
      opt_cmd == OPT::USER_LIST) {
    rgw_admin_user_query_options query_opts;
    query_opts.command = opt_cmd;
    query_opts.tenant = &tenant;
    query_opts.bucket_name = &bucket_name;
    query_opts.bucket_id = &bucket_id;
    query_opts.account_id = &account_id;
    query_opts.account_name = &account_name;
    query_opts.path_prefix = &path_prefix;
    query_opts.marker = &marker;
    query_opts.max_entries = max_entries;
    query_opts.max_entries_specified = max_entries_specified;
    query_opts.account_root = account_root;
    query_opts.sync_stats = sync_stats;
    query_opts.reset_stats = reset_stats;
    query_opts.fix = fix;
    ret = rgw_admin_user_query(dpp(), driver, formatter.get(), stream_flusher,
                               user, bucket, query_opts);
    if (ret != 0) {
      return ret;
    }
  }

#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::METADATA_GET ||
      opt_cmd == OPT::METADATA_PUT ||
      opt_cmd == OPT::METADATA_RM ||
      opt_cmd == OPT::METADATA_LIST) {
    rgw_admin_metadata_options mopts;
    mopts.command = opt_cmd;
    mopts.metadata_key = &metadata_key;
    mopts.marker = &marker;
    mopts.infile = &infile;
    mopts.max_entries = max_entries;
    mopts.max_entries_specified = max_entries_specified;
    ret = rgw_admin_metadata(dpp(), driver, formatter.get(), mopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::MDLOG_LIST ||
      opt_cmd == OPT::MDLOG_AUTOTRIM ||
      opt_cmd == OPT::MDLOG_TRIM ||
      opt_cmd == OPT::MDLOG_STATUS ||
      opt_cmd == OPT::SYNC_INFO ||
      opt_cmd == OPT::SYNC_STATUS ||
      opt_cmd == OPT::METADATA_SYNC_STATUS ||
      opt_cmd == OPT::METADATA_SYNC_INIT ||
      opt_cmd == OPT::METADATA_SYNC_RUN ||
      opt_cmd == OPT::DATA_SYNC_STATUS ||
      opt_cmd == OPT::DATA_SYNC_INIT ||
      opt_cmd == OPT::DATA_SYNC_RUN ||
      opt_cmd == OPT::SYNC_ERROR_LIST ||
      opt_cmd == OPT::SYNC_ERROR_TRIM ||
      opt_cmd == OPT::SYNC_GROUP_CREATE ||
      opt_cmd == OPT::SYNC_GROUP_MODIFY ||
      opt_cmd == OPT::SYNC_GROUP_GET ||
      opt_cmd == OPT::SYNC_GROUP_REMOVE ||
      opt_cmd == OPT::SYNC_GROUP_FLOW_CREATE ||
      opt_cmd == OPT::SYNC_GROUP_FLOW_REMOVE ||
      opt_cmd == OPT::SYNC_GROUP_PIPE_CREATE ||
      opt_cmd == OPT::SYNC_GROUP_PIPE_MODIFY ||
      opt_cmd == OPT::SYNC_GROUP_PIPE_REMOVE ||
      opt_cmd == OPT::SYNC_POLICY_GET) {
    rgw_admin_sync_options sopts;
    sopts.command = opt_cmd;
    sopts.source_zone = &source_zone;
    sopts.marker = &marker;
    sopts.start_marker = &start_marker;
    sopts.end_marker = &end_marker;
    sopts.start_date = &start_date;
    sopts.end_date = &end_date;
    sopts.period_id = &period_id;
    sopts.realm_id = &realm_id;
    sopts.realm_name = &realm_name;
    sopts.zonegroup_id = &zonegroup_id;
    sopts.zonegroup_name = &zonegroup_name;
    sopts.opt_effective_zone_id = &opt_effective_zone_id;
    sopts.opt_bucket = &opt_bucket;
    sopts.opt_bucket_name = &opt_bucket_name;
    sopts.opt_source_zone_id = &opt_source_zone_id;
    sopts.opt_dest_zone_id = &opt_dest_zone_id;
    sopts.opt_source_zone_name = &opt_source_zone_name;
    sopts.opt_dest_zone_name = &opt_dest_zone_name;
    sopts.opt_zone_ids = &opt_zone_ids;
    sopts.opt_source_zone_ids = &opt_source_zone_ids;
    sopts.opt_dest_zone_ids = &opt_dest_zone_ids;
    sopts.opt_source_bucket = &opt_source_bucket;
    sopts.opt_dest_bucket = &opt_dest_bucket;
    sopts.opt_source_tenant = &opt_source_tenant;
    sopts.opt_dest_tenant = &opt_dest_tenant;
    sopts.opt_source_bucket_name = &opt_source_bucket_name;
    sopts.opt_dest_bucket_name = &opt_dest_bucket_name;
    sopts.opt_source_bucket_id = &opt_source_bucket_id;
    sopts.opt_dest_bucket_id = &opt_dest_bucket_id;
    sopts.opt_pipe_id = &opt_pipe_id;
    sopts.opt_group_id = &opt_group_id;
    sopts.opt_flow_id = &opt_flow_id;
    sopts.opt_flow_type = &opt_flow_type;
    sopts.opt_status = &opt_status;
    sopts.opt_prefix = &opt_prefix;
    sopts.opt_prefix_rm = &opt_prefix_rm;
    sopts.opt_dest_owner = &opt_dest_owner;
    sopts.opt_storage_class = &opt_storage_class;
    sopts.opt_priority = &opt_priority;
    sopts.opt_mode = &opt_mode;
    sopts.tags_add = &tags_add;
    sopts.tags_rm = &tags_rm;
    sopts.user = &user;
    sopts.max_entries = max_entries;
    sopts.shard_id = shard_id;
    sopts.trim_delay_ms = trim_delay_ms;
    sopts.max_entries_specified = max_entries_specified;
    sopts.specified_shard_id = specified_shard_id;
    ret = rgw_admin_sync(dpp(), driver, cfgstore.get(), *site, formatter.get(),
                         zone_formatter.get(), sopts);
    if (ret != 0) {
      return ret;
    }
  }

  if (opt_cmd == OPT::MFA_CREATE ||
      opt_cmd == OPT::MFA_REMOVE ||
      opt_cmd == OPT::MFA_GET ||
      opt_cmd == OPT::MFA_LIST ||
      opt_cmd == OPT::MFA_CHECK ||
      opt_cmd == OPT::MFA_RESYNC) {
    rgw_admin_mfa_options mfa_opts;
    mfa_opts.command = opt_cmd;
    mfa_opts.totp_serial = &totp_serial;
    mfa_opts.totp_seed = &totp_seed;
    mfa_opts.totp_seed_type = &totp_seed_type;
    mfa_opts.totp_pin = &totp_pin;
    mfa_opts.objv_tracker = &objv_tracker;
    mfa_opts.totp_seconds = totp_seconds;
    mfa_opts.totp_window = totp_window;
    ret = rgw_admin_mfa(dpp(), driver, formatter.get(), ruser, user_op, user, mfa_opts);
    if (ret != 0) {
      return ret;
    }
  }
#endif

#ifdef WITH_RADOSGW_RADOS
  if (opt_cmd == OPT::DATALOG_LIST ||
      opt_cmd == OPT::DATALOG_STATUS ||
      opt_cmd == OPT::DATALOG_AUTOTRIM ||
      opt_cmd == OPT::DATALOG_TRIM ||
      opt_cmd == OPT::DATALOG_TYPE ||
      opt_cmd == OPT::DATALOG_PRUNE ||
      opt_cmd == OPT::DATALOG_SEMAPHORE_LIST ||
      opt_cmd == OPT::DATALOG_SEMAPHORE_RESET) {
    rgw_admin_datalog_options dopts;
    dopts.command = opt_cmd;
    dopts.marker = &marker;
    dopts.start_marker = &start_marker;
    dopts.end_marker = &end_marker;
    dopts.start_date = &start_date;
    dopts.end_date = &end_date;
    dopts.opt_log_type = &opt_log_type;
    dopts.count = &count;
    dopts.max_entries = max_entries;
    dopts.shard_id = shard_id;
    dopts.specified_shard_id = specified_shard_id;
    dopts.extra_info = extra_info;
    ret = rgw_admin_datalog(dpp(), driver, context_pool, formatter.get(), dopts);
    if (ret != 0) {
      return ret;
    }
  }
#endif

  if (opt_cmd == OPT::RESTORE_STATUS ||
      opt_cmd == OPT::RESTORE_LIST) {
    rgw_admin_restore_options ropts;
    ropts.command = opt_cmd;
    ropts.tenant = &tenant;
    ropts.bucket_name = &bucket_name;
    ropts.object = &object;
    ropts.restore_status_filter = &restore_status_filter;
    ret = rgw_admin_restore(dpp(), driver, stream_flusher, ropts);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}
