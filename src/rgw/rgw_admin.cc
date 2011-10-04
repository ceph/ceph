#include <errno.h>

#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "global/global_init.h"
#include "common/errno.h"

#include "common/armor.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_access.h"
#include "rgw_acl.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "auth/Crypto.h"


#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

void _usage() 
{
  cerr << "usage: radosgw-admin <cmd> [options...]" << std::endl;
  cerr << "commands:\n";
  cerr << "  user create                create a new user\n" ;
  cerr << "  user modify                modify user\n";
  cerr << "  user info                  get user info\n";
  cerr << "  user rm                    remove user\n";
  cerr << "  user suspend               suspend a user\n";
  cerr << "  user enable                reenable user after suspension\n";
  cerr << "  subuser create             create a new subuser\n" ;
  cerr << "  subuser modify             modify subuser\n";
  cerr << "  subuser rm                 remove subuser\n";
  cerr << "  key create                 create access key\n";
  cerr << "  key rm                     remove access key\n";
  cerr << "  bucket list                list buckets\n";
  cerr << "  bucket link                link bucket to specified user\n";
  cerr << "  bucket unlink              unlink bucket from specified user\n";
  cerr << "  bucket stats               returns bucket statistics\n";
  cerr << "  pool add                   add an existing pool to those which can store buckets\n";
  cerr << "  pool info                  show pool information\n";
  cerr << "  pool create                generate pool information (requires bucket)\n";
  cerr << "  policy                     read bucket/object policy\n";
  cerr << "  log list                   list log objects\n";
  cerr << "  log show                   dump a log from specific object or (bucket + date\n";
  cerr << "                             + bucket-id)\n";
  cerr << "  log rm                     remove log object\n";
  cerr << "  temp remove                remove temporary objects that were created up to\n";
  cerr << "                             specified date (and optional time)\n";
  cerr << "options:\n";
  cerr << "   --uid=<id>                user id\n";
  cerr << "   --subuser=<name>          subuser name\n";
  cerr << "   --access-key=<key>        S3 access key\n";
  cerr << "   --swift-user=<group:name> Swift user\n";
  cerr << "   --email=<email>\n";
  cerr << "   --auth_uid=<auid>         librados uid\n";
  cerr << "   --secret=<key>            S3 key\n";
  cerr << "   --swift-secret=<key>      Swift key\n";
  cerr << "   --gen-access-key          generate random access key\n";
  cerr << "   --gen-secret              generate random secret key\n";
  cerr << "   --access=<access>         Set access permissions for sub-user, should be one\n";
  cerr << "                             of read, write, readwrite, full\n";
  cerr << "   --display-name=<name>\n";
  cerr << "   --bucket=<bucket>\n";
  cerr << "   --pool=<pool>\n";
  cerr << "   --object=<object>\n";
  cerr << "   --date=<yyyy-mm-dd>\n";
  cerr << "   --time=<HH:MM:SS>\n";
  cerr << "   --bucket-id=<bucket-id>\n";
  cerr << "   --format=<format>         specify output format for certain operations: xml,\n";
  cerr << "                             json\n";
  cerr << "   --purge-data              when specified, user removal will also purge all the\n";
  cerr << "                             user data\n";
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
  OPT_SUBUSER_CREATE,
  OPT_SUBUSER_MODIFY,
  OPT_SUBUSER_RM,
  OPT_KEY_CREATE,
  OPT_KEY_RM,
  OPT_BUCKETS_LIST,
  OPT_BUCKET_LINK,
  OPT_BUCKET_UNLINK,
  OPT_BUCKET_STATS,
  OPT_POLICY,
  OPT_POOL_ADD,
  OPT_POOL_INFO,
  OPT_POOL_CREATE,
  OPT_LOG_LIST,
  OPT_LOG_SHOW,
  OPT_LOG_RM,
  OPT_TEMP_REMOVE,
};

static uint32_t str_to_perm(const char *str)
{
  if (strcasecmp(str, "read") == 0)
    return RGW_PERM_READ;
  else if (strcasecmp(str, "write") == 0)
    return RGW_PERM_WRITE;
  else if (strcasecmp(str, "readwrite") == 0)
    return RGW_PERM_READ | RGW_PERM_WRITE;
  else if (strcasecmp(str, "full") == 0)
    return RGW_PERM_FULL_CONTROL;

  usage_exit();
  return 0; // unreachable
}

struct rgw_flags_desc {
  uint32_t mask;
  const char *str;
};

static struct rgw_flags_desc rgw_perms[] = {
 { RGW_PERM_FULL_CONTROL, "full-control" },
 { RGW_PERM_READ | RGW_PERM_WRITE, "read-write" },
 { RGW_PERM_READ, "read" },
 { RGW_PERM_WRITE, "write" },
 { RGW_PERM_READ_ACP, "read-acp" },
 { RGW_PERM_WRITE_ACP, "read-acp" },
 { 0, NULL }
};

static void perm_to_str(uint32_t mask, char *buf, int len)
{
  const char *sep = "";
  int pos = 0;
  if (!mask) {
    snprintf(buf, len, "<none>");
    return;
  }
  while (mask) {
    uint32_t orig_mask = mask;
    for (int i = 0; rgw_perms[i].mask; i++) {
      struct rgw_flags_desc *desc = &rgw_perms[i];
      if ((mask & desc->mask) == desc->mask) {
        pos += snprintf(buf + pos, len - pos, "%s%s", sep, desc->str);
        if (pos == len)
          return;
        sep = ", ";
        mask &= ~desc->mask;
        if (!mask)
          return;
      }
    }
    if (mask == orig_mask) // no change
      break;
  }
}

static int get_cmd(const char *cmd, const char *prev_cmd, bool *need_more)
{
  *need_more = false;
  if (strcmp(cmd, "user") == 0 ||
      strcmp(cmd, "subuser") == 0 ||
      strcmp(cmd, "key") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "bucket") == 0 ||
      strcmp(cmd, "pool") == 0 ||
      strcmp(cmd, "log") == 0 ||
      strcmp(cmd, "temp") == 0) {
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
  } else if (strcmp(prev_cmd, "log") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_LOG_LIST;
    if (strcmp(cmd, "show") == 0)
      return OPT_LOG_SHOW;
    if (strcmp(cmd, "rm") == 0)
      return OPT_LOG_RM;
  } else if (strcmp(prev_cmd, "temp") == 0) {
    if (strcmp(cmd, "remove") == 0)
      return OPT_TEMP_REMOVE;
  } else if (strcmp(prev_cmd, "pool") == 0) {
    if (strcmp(cmd, "add") == 0)
      return OPT_POOL_ADD;
    if (strcmp(cmd, "info") == 0)
      return OPT_POOL_INFO;
    if (strcmp(cmd, "create") == 0)
      return OPT_POOL_CREATE;
  }

  return -EINVAL;
}

string escape_str(string& src, char c)
{
  int pos = 0;
  string s = src;
  string dest;

  do {
    int new_pos = src.find(c, pos);
    if (new_pos >= 0) {
      dest += src.substr(pos, new_pos - pos);
      dest += "\\";
      dest += c;
    } else {
      dest += src.substr(pos);
      return dest;
    }
    pos = new_pos + 1;
  } while (pos < (int)src.size());

  return dest;
}

static void show_user_info(RGWUserInfo& info, Formatter *formatter)
{
  map<string, RGWAccessKey>::iterator kiter;
  map<string, RGWSubUser>::iterator uiter;


  formatter->open_object_section("user_info");

  formatter->dump_string("user_id", info.user_id.c_str());
  formatter->dump_int("rados_uid", info.auid);
  formatter->dump_string("display_name", info.display_name.c_str());
  formatter->dump_string("email", info.user_email.c_str());
  formatter->dump_string("swift_user", info.swift_name.c_str());
  formatter->dump_string("swift_key", info.swift_key.c_str());
  formatter->dump_int("suspended", (int)info.suspended);

  // keys
  formatter->open_array_section("keys");
  for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    const char *sep = (k.subuser.empty() ? "" : ":");
    const char *subuser = (k.subuser.empty() ? "" : k.subuser.c_str());
    formatter->open_object_section("key");
    formatter->dump_format("user", "%s%s%s", info.user_id.c_str(), sep, subuser);
    formatter->dump_string("access_key", k.id);
    formatter->dump_string("secret_key", k.key);
    formatter->close_section();
  }
  formatter->close_section();

  // subusers
  formatter->open_array_section("subusers");
  for (uiter = info.subusers.begin(); uiter != info.subusers.end(); ++uiter) {
    RGWSubUser& u = uiter->second;
    formatter->open_object_section("user");
    formatter->dump_format("id", "%s:%s", info.user_id.c_str(), u.name.c_str());
    char buf[256];
    perm_to_str(u.perm_mask, buf, sizeof(buf));
    formatter->dump_string("permissions", buf);
    formatter->close_section();
    formatter->flush(cout);
  }
  formatter->close_section();

  formatter->close_section();
  formatter->flush(cout);
}

static int create_bucket(string bucket_str, string& user_id, string& display_name, uint64_t auid)
{
  RGWAccessControlPolicy policy, old_policy;
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  string no_oid;
  rgw_obj obj;
  RGWBucketInfo bucket_info;

  int ret;

  // defaule policy (private)
  policy.create_default(user_id, display_name);
  policy.encode(aclbl);

  ret = rgw_get_bucket_info(bucket_str, bucket_info);
  if (ret < 0)
    return ret;

  rgw_bucket& bucket = bucket_info.bucket;

  ret = rgwstore->create_bucket(user_id, bucket, attrs, false, auid);
  if (ret && ret != -EEXIST)   
    goto done;

  obj.init(bucket, no_oid);

  ret = rgwstore->set_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
  if (ret < 0) {
    cerr << "couldn't set acl on bucket" << std::endl;
  }

  ret = rgw_add_bucket(user_id, bucket);

  RGW_LOG(20) << "ret=" << ret << dendl;

  if (ret == -EEXIST)
    ret = 0;
done:
  return ret;
}

static void remove_old_indexes(RGWUserInfo& old_info, RGWUserInfo new_info)
{
  int ret;
  bool success = true;

  if (!old_info.user_id.empty() && old_info.user_id.compare(new_info.user_id) != 0) {
    ret = rgw_remove_uid_index(old_info.user_id);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not remove index for uid " << old_info.user_id << " return code: " << ret << std::endl;
      success = false;
    }
  }

  if (!old_info.user_email.empty() &&
      old_info.user_email.compare(new_info.user_email) != 0) {
    ret = rgw_remove_email_index(new_info.user_id, old_info.user_email);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not remove index for email " << old_info.user_email << " return code: " << ret << std::endl;
      success = false;
    }
  }

  if (!old_info.swift_name.empty() &&
      old_info.swift_name.compare(new_info.swift_name) != 0) {
    ret = rgw_remove_swift_name_index(new_info.user_id, old_info.swift_name);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: could not remove index for swift_name " << old_info.swift_name << " return code: " << ret << std::endl;
      success = false;
    }
  }

  /* we're not removing access keys here.. keys are removed explicitly using the key rm command and removing the old key
     index is handled there */

  if (!success)
    cerr << "ERROR: this should be fixed manually!" << std::endl;
}

class IntentLogNameFilter : public RGWAccessListFilter
{
  string prefix;
  bool filter_exact_date;
public:
  IntentLogNameFilter(const char *date, struct tm *tm) {
    prefix = date;
    filter_exact_date = !(tm->tm_hour || tm->tm_min || tm->tm_sec); /* if time was specified and is not 00:00:00
                                                                       we should look at objects from that date */
  }
  bool filter(string& name, string& key) {
    if (filter_exact_date)
      return name.compare(prefix) < 0;
    else
      return name.compare(0, prefix.size(), prefix) <= 0;
  }
};

enum IntentFlags { // bitmask
  I_DEL_OBJ = 1,
  I_DEL_POOL = 2,
};

int process_intent_log(rgw_bucket& bucket, string& oid, time_t epoch, int flags, bool purge)
{
  cout << "processing intent log " << oid << std::endl;
  uint64_t size;
  rgw_obj obj(bucket, oid);
  int r = rgwstore->obj_stat(NULL, obj, &size, NULL);
  if (r < 0) {
    cerr << "error while doing stat on " << bucket << ":" << oid
	 << " " << cpp_strerror(-r) << std::endl;
    return -r;
  }
  bufferlist bl;
  r = rgwstore->read(NULL, obj, 0, size, bl);
  if (r < 0) {
    cerr << "error while reading from " <<  bucket << ":" << oid
	 << " " << cpp_strerror(-r) << std::endl;
    return -r;
  }

  bufferlist::iterator iter = bl.begin();
  string id;
  bool complete = true;
  try {
    while (!iter.end()) {
      struct rgw_intent_log_entry entry;
      try {
        ::decode(entry, iter);
      } catch (buffer::error& err) {
        RGW_LOG(0) << "ERROR: " << __func__ << "(): caught buffer::error" << dendl;
        return -EIO;
      }
      if (entry.op_time.sec() > epoch) {
        cerr << "skipping entry for obj=" << obj << " entry.op_time=" << entry.op_time.sec() << " requested epoch=" << epoch << std::endl;
        cerr << "skipping intent log" << std::endl; // no use to continue
        complete = false;
        break;
      }
      switch (entry.intent) {
      case DEL_OBJ:
        if (!flags & I_DEL_OBJ) {
          complete = false;
          break;
        }
        r = rgwstore->delete_obj(NULL, id, entry.obj);
        if (r < 0 && r != -ENOENT) {
          cerr << "failed to remove obj: " << entry.obj << std::endl;
          complete = false;
        }
        break;
      case DEL_POOL:
        if (!flags & I_DEL_POOL) {
          complete = false;
          break;
        }
        r = rgwstore->delete_bucket(id, entry.obj.bucket, true);
        if (r < 0 && r != -ENOENT) {
          cerr << "failed to remove pool: " << entry.obj.bucket.pool << std::endl;
          complete = false;
        }
        break;
      default:
        complete = false;
      }
    }
  } catch (buffer::error& err) {
    cerr << "failed to decode intent log entry in " << bucket << ":" << oid << std::endl;
    complete = false;
  }

  if (complete) {
    rgw_obj obj(bucket, oid);
    cout << "completed intent log: " << obj << (purge ? ", purging it" : "") << std::endl;
    if (purge) {
      r = rgwstore->delete_obj(NULL, id, obj);
      if (r < 0)
        cerr << "failed to remove obj: " << obj << std::endl;
    }
  }

  return 0;
}

int bucket_stats(rgw_bucket& bucket, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  int r = rgw_get_bucket_info(bucket.name, bucket_info);
  if (r < 0)
    return r;

  map<RGWObjCategory, RGWBucketStats> stats;
  int ret = rgwstore->get_bucket_stats(bucket, stats);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }
  map<RGWObjCategory, RGWBucketStats>::iterator iter;
  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name.c_str());
  formatter->dump_string("pool", bucket.pool.c_str());
  
  formatter->dump_int("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker.c_str());
  formatter->dump_string("owner", bucket_info.owner.c_str());
  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWBucketStats& s = iter->second;
    const char *cat_name = rgw_obj_category_name(iter->first);
    formatter->open_object_section(cat_name);
    formatter->dump_int("size_kb", s.num_kb);
    formatter->dump_int("num_objects", s.num_objects);
    formatter->close_section();
    formatter->flush(cout);
  }
  formatter->close_section();
  formatter->close_section();
  return 0;
}

int main(int argc, char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::string user_id, access_key, secret_key, user_email, display_name;
  std::string bucket_name, pool_name, object, swift_user, swift_key;
  std::string date, time, subuser, access, format;
  rgw_bucket bucket;
  uint32_t perm_mask = 0;
  uint64_t auid = -1;
  RGWUserInfo info;
  RGWAccess *store;
  int opt_cmd = OPT_NO_CMD;
  bool need_more;
  bool gen_secret = false;
  bool gen_key = false;
  char secret_key_buf[SECRET_KEY_LEN + 1];
  char public_id_buf[PUBLIC_ID_LEN + 1];
  bool user_modify_op;
  int64_t bucket_id = -1;
  Formatter *formatter = NULL;
  bool purge_data = false;
  RGWBucketInfo bucket_info;
  bool pretty_format = false;

  std::string val;
  std::ostringstream errs;
  long long tmp = 0;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--uid", (char*)NULL)) {
      user_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--access-key", (char*)NULL)) {
      access_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--subuser", (char*)NULL)) {
      subuser = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--secret", (char*)NULL)) {
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
    } else if (ceph_argparse_flag(args, i, "--gen-access-key", (char*)NULL)) {
      gen_key = true;
    } else if (ceph_argparse_flag(args, i, "--gen-secret", (char*)NULL)) {
      gen_secret = true;
    } else if (ceph_argparse_withlonglong(args, i, &tmp, &errs, "-a", "--auth-uid", (char*)NULL)) {
      if (!errs.str().empty()) {
	cerr << errs.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      auid = tmp;
    } else if (ceph_argparse_witharg(args, i, &val, "--swift-user", (char*)NULL)) {
      swift_user = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--swift-secret", (char*)NULL)) {
      swift_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--date", (char*)NULL)) {
      date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--time", (char*)NULL)) {
      time = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--access", (char*)NULL)) {
      access = val;
      perm_mask = str_to_perm(access.c_str());
    } else if (ceph_argparse_withlonglong(args, i, &tmp, &errs, "--bucket-id", (char*)NULL)) {
      if (!errs.str().empty()) {
	cerr << errs.str() << std::endl;
	exit(EXIT_FAILURE);
      }
      bucket_id = tmp;
      if (bucket_id < 0) {
        cerr << "bad bucket-id: " << bucket_id << std::endl;
        return usage();
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      format = val;
    } else if (ceph_argparse_flag(args, i, "--pretty-format", (char*)NULL)) {
      pretty_format = true;
    } else if (ceph_argparse_flag(args, i, "--purge-data", (char*)NULL)) {
      purge_data = true;
    } else {
      ++i;
    }
  }

  if (args.size() == 0) {
    return usage();
  }
  else {
    const char *prev_cmd = NULL;
    for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
      opt_cmd = get_cmd(*i, prev_cmd, &need_more);
      if (opt_cmd < 0) {
	cerr << "unrecognized arg " << *i << std::endl;
	return usage();
      }
      if (!need_more)
	break;
      prev_cmd = *i;
    }
    if (opt_cmd == OPT_NO_CMD)
      return usage();
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

  if (!subuser.empty()) {
    char *suser = strdup(subuser.c_str());
    char *p = strchr(suser, ':');
    if (p) {
      *p = '\0';
      if (!user_id.empty()) {
        if (user_id != suser) {
          cerr << "bad subuser " << subuser << " for uid " << user_id << std::endl;
          return 1;
        }
      } else {
        user_id = suser;
      }
      subuser = p + 1;
    }
    free(suser);
  }

  if (opt_cmd == OPT_KEY_RM && access_key.empty()) {
    cerr << "error: access key was not specified" << std::endl;
    return usage();
  }

  user_modify_op = (opt_cmd == OPT_USER_MODIFY || opt_cmd == OPT_SUBUSER_MODIFY ||
                    opt_cmd == OPT_SUBUSER_CREATE || opt_cmd == OPT_SUBUSER_RM ||
                    opt_cmd == OPT_KEY_CREATE || opt_cmd == OPT_KEY_RM || opt_cmd == OPT_USER_RM);

  RGWStoreManager store_manager;
  store = store_manager.init("rados", g_ceph_context);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  if (opt_cmd != OPT_USER_CREATE && 
      opt_cmd != OPT_LOG_SHOW && opt_cmd != OPT_LOG_LIST && opt_cmd != OPT_LOG_RM && 
      user_id.empty()) {
    bool found = false;
    string s;
    if (!found && (!user_email.empty())) {
      s = user_email;
      if (rgw_get_user_info_by_email(s, info) >= 0) {
	found = true;
      } else {
	cerr << "could not find user by specified email" << std::endl;
      }
    }
    if (!found && (!access_key.empty())) {
      s = access_key;
      if (rgw_get_user_info_by_access_key(s, info) >= 0) {
	found = true;
      } else {
	cerr << "could not find user by specified access key" << std::endl;
      }
    }
    if (!found && (!swift_user.empty())) {
      s = swift_user;
      if (rgw_get_user_info_by_swift(s, info) >= 0) {
	found = true;
      } else
        cerr << "could not find user by specified swift username" << std::endl;
    }
    if (found)
      user_id = info.user_id.c_str();
  }


  if (user_modify_op || opt_cmd == OPT_USER_CREATE ||
      opt_cmd == OPT_USER_INFO || opt_cmd == OPT_BUCKET_UNLINK || opt_cmd == OPT_BUCKET_LINK ||
      opt_cmd == OPT_USER_SUSPEND || opt_cmd == OPT_USER_ENABLE) {
    if (user_id.empty()) {
      cerr << "user_id was not specified, aborting" << std::endl;
      return usage();
    }

    bool found = (rgw_get_user_info_by_uid(user_id, info) >= 0);

    if (opt_cmd == OPT_USER_CREATE) {
      if (found) {
        cerr << "error: user already exists" << std::endl;
        return 1;
      }
    } else if (!found) {
      cerr << "error reading user info, aborting" << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_SUBUSER_CREATE || opt_cmd == OPT_SUBUSER_MODIFY ||
      opt_cmd == OPT_SUBUSER_RM) {
    if (subuser.empty()) {
      cerr << "subuser creation was requires specifying subuser name" << std::endl;
      return 1;
    }
    map<string, RGWSubUser>::iterator iter = info.subusers.find(subuser);
    bool found = (iter != info.subusers.end());
    if (opt_cmd == OPT_SUBUSER_CREATE) {
      if (found) {
        cerr << "error: subuser already exists" << std::endl;
        return 1;
      }
    } else if (!found) {
      cerr << "error: subuser doesn't exist" << std::endl;
      return 1;
    }
  }

  bool keys_not_requested = (access_key.empty() && secret_key.empty() && !gen_secret && !gen_key &&
                             opt_cmd != OPT_KEY_CREATE);

  if (opt_cmd == OPT_USER_CREATE || (user_modify_op && !keys_not_requested)) {
    int ret;

    if (opt_cmd == OPT_USER_CREATE && display_name.empty()) {
      cerr << "display name was not specified, aborting" << std::endl;
      return 0;
    }

    if (secret_key.empty() || gen_secret) {
      ret = gen_rand_base64(secret_key_buf, sizeof(secret_key_buf));
      if (ret < 0) {
        cerr << "aborting" << std::endl;
        return 1;
      }
      secret_key = secret_key_buf;
    }
    if (access_key.empty() || gen_key) {
      RGWUserInfo duplicate_check;
      string duplicate_check_id;
      do {
	ret = gen_rand_alphanumeric_upper(public_id_buf, sizeof(public_id_buf));
	if (ret < 0) {
	  cerr << "aborting" << std::endl;
	  return 1;
	}
	access_key = public_id_buf;
	duplicate_check_id = access_key;
      } while (!rgw_get_user_info_by_access_key(duplicate_check_id, duplicate_check));
    }
  }

  map<string, RGWAccessKey>::iterator kiter;
  map<string, RGWSubUser>::iterator uiter;
  RGWUserInfo old_info = info;

  if ((!bucket_name.empty()) || bucket_id >= 0) {
    if (bucket_id >= 0) {
      int ret = rgw_get_bucket_info_id(bucket_id, bucket_info);
      if (ret < 0) {
        cerr << "could not retrieve bucket info for bucket_id=" << bucket_id << std::endl;
        return ret;
      }
      bucket = bucket_info.bucket;
      if ((!bucket_name.empty()) && bucket.name.compare(bucket_name.c_str()) != 0) {
        cerr << "bucket name does not match bucket id (expected bucket name: " << bucket.name << ")" << std::endl;
        return -EINVAL;
      }
    } else {
      string bucket_name_str = bucket_name;
      RGWBucketInfo bucket_info;
      int r = rgw_get_bucket_info(bucket_name_str, bucket_info);
      if (r < 0) {
        cerr << "could not get bucket info for bucket=" << bucket_name_str << std::endl;
        return r;
      }
      bucket = bucket_info.bucket;
      bucket_id = bucket.bucket_id;
    }
  }

  int err;
  switch (opt_cmd) {
  case OPT_USER_CREATE:
  case OPT_USER_MODIFY:
  case OPT_SUBUSER_CREATE:
  case OPT_SUBUSER_MODIFY:
  case OPT_KEY_CREATE:
    if (!user_id.empty())
      info.user_id = user_id;
    if ((!access_key.empty()) && (!secret_key.empty())) {
      RGWAccessKey k;
      k.id = access_key;
      k.key = secret_key;
      if (!subuser.empty())
        k.subuser = subuser;
      info.access_keys[access_key] = k;
   } else if ((!access_key.empty()) || (!secret_key.empty())) {
      cerr << "access key modification requires both access key and secret key" << std::endl;
      return 1;
    }
    if (!display_name.empty())
      info.display_name = display_name;
    if (!user_email.empty())
      info.user_email = user_email;
    if (auid != (uint64_t)-1)
      info.auid = auid;
    if (!swift_user.empty())
      info.swift_name = swift_user;
    if (!swift_key.empty())
      info.swift_key = swift_key;
    if (!subuser.empty()) {
      RGWSubUser u;
      u.name = subuser;
      u.perm_mask = perm_mask;

      info.subusers[subuser] = u;
    }
    if ((err = rgw_store_user_info(info)) < 0) {
      cerr << "error storing user info: " << cpp_strerror(-err) << std::endl;
      break;
    }

    remove_old_indexes(old_info, info);

    show_user_info(info, formatter);
    break;

  case OPT_SUBUSER_RM:
    uiter = info.subusers.find(subuser);
    assert (uiter != info.subusers.end());
    info.subusers.erase(uiter);
    if ((err = rgw_store_user_info(info)) < 0) {
      cerr << "error storing user info: " << cpp_strerror(-err) << std::endl;
      break;
    }
    show_user_info(info, formatter);
    break;

  case OPT_KEY_RM:
    kiter = info.access_keys.find(access_key);
    if (kiter == info.access_keys.end()) {
      cerr << "key not found" << std::endl;
    } else {
      rgw_remove_key_index(kiter->second);
      info.access_keys.erase(kiter);
      if ((err = rgw_store_user_info(info)) < 0) {
        cerr << "error storing user info: " << cpp_strerror(-err) << std::endl;
        break;
      }
    }
    show_user_info(info, formatter);
    break;

  case OPT_USER_INFO:
    show_user_info(info, formatter);
    break;
  }

  if (opt_cmd == OPT_POLICY) {
    bufferlist bl;
    rgw_obj obj(bucket, object);
    int ret = store->get_attr(NULL, obj, RGW_ATTR_ACL, bl);

    RGWAccessControlPolicy policy;
    if (ret >= 0) {
      bufferlist::iterator iter = bl.begin();
      try {
        policy.decode(iter);
      } catch (buffer::error& err) {
        RGW_LOG(0) << "ERROR: caught buffer::error, could not decode policy" << dendl;
        return -EIO;
      }
      policy.to_xml(cout);
      cout << std::endl;
    }
  }

  if (opt_cmd == OPT_BUCKETS_LIST) {
    string id;
    RGWAccessHandle handle;

    formatter->reset();
    formatter->open_array_section("buckets");
    if (!user_id.empty()) {
      RGWUserBuckets buckets;
      if (rgw_read_user_buckets(user_id, buckets, false) < 0) {
        cerr << "list buckets: could not get buckets for uid " << user_id << std::endl;
      } else {
        map<string, RGWBucketEnt>& m = buckets.get_buckets();
        map<string, RGWBucketEnt>::iterator iter;

        for (iter = m.begin(); iter != m.end(); ++iter) {
          RGWBucketEnt obj = iter->second;
	  formatter->dump_string("bucket", obj.bucket.name);
        }
      }
    } else {
      if (store->list_buckets_init(id, &handle) < 0) {
        cerr << "list buckets: no buckets found" << std::endl;
      } else {
        RGWObjEnt obj;
        while (store->list_buckets_next(id, obj, &handle) >= 0) {
          formatter->dump_string("bucket", obj.name);
        }
      }
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_BUCKET_LINK) {
    if (bucket_name.empty()) {
      cerr << "bucket name was not specified" << std::endl;
      return usage();
    }
    string uid_str(user_id);
    
    string no_oid;
    bufferlist aclbl;
    rgw_obj obj(bucket, no_oid);

    int r = rgwstore->get_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
    if (r >= 0) {
      RGWAccessControlPolicy policy;
      ACLOwner owner;
      try {
       bufferlist::iterator iter = aclbl.begin();
       ::decode(policy, iter);
       owner = policy.get_owner();
      } catch (buffer::error& err) {
	dout(10) << "couldn't decode policy" << dendl;
	return -EINVAL;
      }
      //cout << "bucket is linked to user '" << owner.get_id() << "'.. unlinking" << std::endl;
      r = rgw_remove_user_bucket_info(owner.get_id(), bucket, false);
      if (r < 0) {
	cerr << "could not unlink policy from user '" << owner.get_id() << "'" << std::endl;
	return r;
      }
    }

    r = create_bucket(bucket_name.c_str(), uid_str, info.display_name, info.auid);
    if (r < 0)
        cerr << "error linking bucket to user: r=" << r << std::endl;
    return -r;
  }

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    if (bucket_name.empty()) {
      cerr << "bucket name was not specified" << std::endl;
      return usage();
    }

    int r = rgw_remove_user_bucket_info(user_id, bucket, false);
    if (r < 0)
      cerr << "error unlinking bucket " <<  cpp_strerror(-r) << std::endl;
    return -r;
  }

  if (opt_cmd == OPT_TEMP_REMOVE) {
    if (date.empty()) {
      cerr << "date wasn't specified" << std::endl;
      return usage();
    }

    struct tm tm;

    string format = "%Y-%m-%d";
    string datetime = date;
    if (datetime.size() != 10) {
      cerr << "bad date format" << std::endl;
      return -EINVAL;
    }

    if (!time.empty()) {
      if (time.size() != 5 && time.size() != 8) {
        cerr << "bad time format" << std::endl;
        return -EINVAL;
      }
      format.append(" %H:%M:%S");
      datetime.append(time.c_str());
    }
    const char *s = strptime(datetime.c_str(), format.c_str(), &tm);
    if (s && *s) {
      cerr << "failed to parse date/time" << std::endl;
      return -EINVAL;
    }
    time_t epoch = mktime(&tm);
    rgw_bucket bucket(RGW_INTENT_LOG_POOL_NAME);
    string prefix, delim, marker;
    vector<RGWObjEnt> objs;
    map<string, bool> common_prefixes;
    string ns;
    string id;

    int max = 1000;
    bool is_truncated;
    IntentLogNameFilter filter(date.c_str(), &tm);
    do {
      int r = store->list_objects(id, bucket, max, prefix, delim, marker,
                          objs, common_prefixes, false, ns,
                          &is_truncated, &filter);
      if (r == -ENOENT)
        break;
      if (r < 0) {
        cerr << "failed to list objects" << std::endl;
      }
      vector<RGWObjEnt>::iterator iter;
      for (iter = objs.begin(); iter != objs.end(); ++iter) {
        process_intent_log(bucket, (*iter).name, epoch, I_DEL_OBJ | I_DEL_POOL, true);
      }
    } while (is_truncated);
  }

  if (opt_cmd == OPT_LOG_LIST) {
    rgw_bucket log_bucket(RGW_LOG_POOL_NAME);

    formatter->reset();
    formatter->open_array_section("logs");
    RGWAccessHandle h;
    int r = store->list_objects_raw_init(log_bucket, &h);
    if (r < 0) {
      cerr << "log list: error " << r << std::endl;
      return r;
    }
    while (true) {
      RGWObjEnt obj;
      int r = store->list_objects_raw_next(obj, &h);
      if (r == -ENOENT)
	break;
      if (r < 0) {
	cerr << "log list: error " << r << std::endl;
	return r;
      }
      formatter->dump_string("object", obj.name);
    };
    formatter->close_section();
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_LOG_SHOW || opt_cmd == OPT_LOG_RM) {
    if (object.empty() && (date.empty() || bucket_name.empty() || bucket_id < 0)) {
      cerr << "object or (at least one of date, bucket, bucket-id) were not specified" << std::endl;
      return usage();
    }

    rgw_bucket log_bucket(RGW_LOG_POOL_NAME);
    string oid;
    if (!object.empty()) {
      oid = object;
    } else {
      char buf[16];
      snprintf(buf, sizeof(buf), "%lld", (unsigned long long)bucket_id);
      oid = date;
      oid += "-";
      oid += buf;
      oid += "-";
      oid += string(bucket.name);
    }
    rgw_obj obj(log_bucket, oid);

    if (opt_cmd == OPT_LOG_SHOW) {
      uint64_t size;
      int r = store->obj_stat(NULL, obj, &size, NULL);
      if (r < 0) {
	cerr << "error while doing stat on " <<  log_bucket << ":" << oid
	     << " " << cpp_strerror(-r) << std::endl;
	return -r;
      }

      bufferlist bl;
      r = store->read(NULL, obj, 0, size, bl);
      if (r < 0) {
	cerr << "error while reading from " <<  log_bucket << ":" << oid
	     << " " << cpp_strerror(-r) << std::endl;
	return -r;
      }
      
      bufferlist::iterator iter = bl.begin();
      
      struct rgw_log_entry entry;
      
      formatter->reset();
      formatter->open_object_section("log");

      // peek at first entry to get bucket metadata
      bufferlist::iterator first_iter = iter;
      if (!first_iter.end()) {
	::decode(entry, first_iter);
	formatter->dump_int("bucket_id", entry.bucket_id);
	formatter->dump_string("bucket_owner", entry.bucket_owner);
	formatter->dump_string("bucket", entry.bucket);
      }
      formatter->open_array_section("log_entries");
      
      while (!iter.end()) {
	::decode(entry, iter);
	
	uint64_t total_time =  entry.total_time.sec() * 1000000LL * entry.total_time.usec();
	
	formatter->open_object_section("log_entry");
	formatter->dump_string("bucket", entry.bucket.c_str());
	formatter->dump_stream("time") << entry.time;
	formatter->dump_string("remote_addr", entry.remote_addr.c_str());
	if (entry.object_owner.length())
	  formatter->dump_string("object_owner", entry.object_owner.c_str());
	formatter->dump_string("user", entry.user.c_str());
	formatter->dump_string("operation", entry.op.c_str());
	formatter->dump_string("uri", entry.uri.c_str());
	formatter->dump_string("http_status", entry.http_status.c_str());
	formatter->dump_string("error_code", entry.error_code.c_str());
	formatter->dump_int("bytes_sent", entry.bytes_sent);
	formatter->dump_int("bytes_received", entry.bytes_received);
	formatter->dump_int("object_size", entry.obj_size);
	formatter->dump_int("total_time", total_time);
	formatter->dump_string("user_agent",  entry.user_agent.c_str());
	formatter->dump_string("referrer",  entry.referrer.c_str());
	formatter->close_section();
	formatter->flush(cout);
      }
      formatter->close_section();
      formatter->close_section();
      formatter->flush(cout);
    }
    if (opt_cmd == OPT_LOG_RM) {
      std::string id;
      int r = store->delete_obj(NULL, id, obj);
      if (r < 0) {
	cerr << "error removing " <<  log_bucket << ":" << oid
	     << " " << cpp_strerror(-r) << std::endl;
	return -r;
      }
    }
  }
  
  if (opt_cmd == OPT_USER_RM) {
    rgw_delete_user(info, purge_data);
  }
  
  if (opt_cmd == OPT_POOL_ADD) {
    if (pool_name.empty()) {
      cerr << "need to specify pool to add!" << std::endl;
      return usage();
    }

    rgwstore->add_bucket_placement(pool_name);
  }

  if (opt_cmd == OPT_POOL_INFO) {
     if (bucket_name.empty() && bucket_id < 0) {
       cerr << "either bucket or bucket-id needs to be specified" << std::endl;
       return usage();
     }
     formatter->reset();
     formatter->open_object_section("pool_info");
     formatter->dump_int("id", bucket_id);
     formatter->dump_string("bucket", bucket_info.bucket.name.c_str());
     formatter->dump_string("pool", bucket_info.bucket.pool.c_str());
     formatter->dump_string("owner", bucket_info.owner.c_str());
     formatter->close_section();
     formatter->flush(cout);
   }

   if (opt_cmd == OPT_BUCKET_STATS) {
     if (bucket_name.empty() && bucket_id < 0 && user_id.empty()) {
       cerr << "either bucket or bucket-id or uid needs to be specified" << std::endl;
       return usage();
     }
     formatter->reset();
     if (user_id.empty()) {
       bucket_stats(bucket, formatter);
     } else {
       RGWUserBuckets buckets;
       if (rgw_read_user_buckets(user_id, buckets, false) < 0) {
	 cerr << "could not get buckets for uid " << user_id << std::endl;
       } else {
	 formatter->open_array_section("buckets");
	 map<string, RGWBucketEnt>& m = buckets.get_buckets();
	 for (map<string, RGWBucketEnt>::iterator iter = m.begin(); iter != m.end(); ++iter) {
	   RGWBucketEnt obj = iter->second;
	   bucket_stats(obj.bucket, formatter);
	 }
	 formatter->close_section();
       }
     }
     formatter->flush(cout);
   }

   if (opt_cmd == OPT_POOL_CREATE) {
 #if 0
     if (bucket_name.empty())
       return usage();
     string no_object;
     int ret;
     bufferlist bl;
     rgw_obj obj(bucket, no_object);

     ret = rgwstore->get_attr(NULL, obj, RGW_ATTR_ACL, bl);
     if (ret < 0) {
       RGW_LOG(0) << "can't read bucket acls: " << ret << dendl;
       return ret;
     }
     RGWAccessControlPolicy policy;
     bufferlist::iterator iter = bl.begin();
     policy.decode(iter);

     RGWBucketInfo info;
     info.bucket = bucket;
     info.owner = policy.get_owner().get_id();

    ret = rgw_store_bucket_info_id(bucket.bucket_id, info);
    if (ret < 0) {
      RGW_LOG(0) << "can't store pool info: bucket_id=" << bucket.bucket_id << " ret=" << ret << dendl;
      return ret;
    }
#endif
  }

 if (opt_cmd == OPT_USER_SUSPEND || opt_cmd == OPT_USER_ENABLE) {
    string id;
    __u8 disable = (opt_cmd == OPT_USER_SUSPEND ? 1 : 0);

    if (user_id.empty()) {
      cerr << "uid was not specified" << std::endl;
      return usage();
    }
    RGWUserBuckets buckets;
    if (rgw_read_user_buckets(user_id, buckets, false) < 0) {
      cerr << "could not get buckets for uid " << user_id << std::endl;
    }
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    map<string, RGWBucketEnt>::iterator iter;

    int ret;
    info.suspended = disable;
    ret = rgw_store_user_info(info);
    if (ret < 0) {
      cerr << "ERROR: failed to store user info user=" << user_id << " ret=" << ret << std::endl;
      return 1;
    }
     
    if (disable)
      RGW_LOG(0) << "disabling user buckets" << dendl;
    else
      RGW_LOG(0) << "enabling user buckets" << dendl;

    vector<rgw_bucket> bucket_names;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      RGWBucketEnt obj = iter->second;
      bucket_names.push_back(obj.bucket);
    }
    if (disable)
      ret = rgwstore->disable_buckets(bucket_names);
    else
      ret = rgwstore->enable_buckets(bucket_names, info.auid);
    if (ret < 0) {
      cerr << "ERROR: failed to change pool" << std::endl;
      return 1;
    }
  } 

  return 0;
}
