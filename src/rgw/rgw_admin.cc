#include <errno.h>

#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#include "common/ceph_json.h"

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "include/utime.h"
#include "include/str_list.h"

#include "common/armor.h"
#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "auth/Crypto.h"

#define dout_subsys ceph_subsys_rgw

#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

static RGWRados *store = NULL;

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
  cerr << "  user check                 check user info\n";
  cerr << "  caps add                   add user capabilities\n";
  cerr << "  caps rm                    remove user capabilities\n";
  cerr << "  subuser create             create a new subuser\n" ;
  cerr << "  subuser modify             modify subuser\n";
  cerr << "  subuser rm                 remove subuser\n";
  cerr << "  key create                 create access key\n";
  cerr << "  key rm                     remove access key\n";
  cerr << "  bucket list                list buckets\n";
  cerr << "  bucket link                link bucket to specified user\n";
  cerr << "  bucket unlink              unlink bucket from specified user\n";
  cerr << "  bucket stats               returns bucket statistics\n";
  cerr << "  bucket rm                  remove bucket\n";
  cerr << "  bucket check               check bucket index\n";
  cerr << "  object rm                  remove object\n";
  cerr << "  object unlink              unlink object from bucket index\n";
  cerr << "  zone info                  show zone params info\n";
  cerr << "  pool add                   add an existing pool for data placement\n";
  cerr << "  pool rm                    remove an existing pool from data placement set\n";
  cerr << "  pools list                 list placement active set\n";
  cerr << "  policy                     read bucket/object policy\n";
  cerr << "  log list                   list log objects\n";
  cerr << "  log show                   dump a log from specific object or (bucket + date\n";
  cerr << "                             + bucket-id)\n";
  cerr << "  log rm                     remove log object\n";
  cerr << "  usage show                 show usage (by user, date range)\n";
  cerr << "  usage trim                 trim usage (by user, date range)\n";
  cerr << "  temp remove                remove temporary objects that were created up to\n";
  cerr << "                             specified date (and optional time)\n";
  cerr << "  gc list                    dump expired garbage collection objects\n";
  cerr << "  gc process                 manually process garbage\n";
  cerr << "options:\n";
  cerr << "   --uid=<id>                user id\n";
  cerr << "   --subuser=<name>          subuser name\n";
  cerr << "   --access-key=<key>        S3 access key\n";
  cerr << "   --email=<email>\n";
  cerr << "   --secret=<key>            specify secret key\n";
  cerr << "   --gen-access-key          generate random access key (for S3)\n";
  cerr << "   --gen-secret              generate random secret key\n";
  cerr << "   --key-type=<type>         key type, options are: swift, s3\n";
  cerr << "   --access=<access>         Set access permissions for sub-user, should be one\n";
  cerr << "                             of read, write, readwrite, full\n";
  cerr << "   --display-name=<name>\n";
  cerr << "   --bucket=<bucket>\n";
  cerr << "   --pool=<pool>\n";
  cerr << "   --object=<object>\n";
  cerr << "   --date=<date>\n";
  cerr << "   --start-date=<date>\n";
  cerr << "   --end-date=<date>\n";
  cerr << "   --bucket-id=<bucket-id>\n";
  cerr << "   --fix                     besides checking bucket index, will also fix it\n";
  cerr << "   --check-objects           bucket check: rebuilds bucket index according to\n";
  cerr << "                             actual objects state\n";
  cerr << "   --format=<format>         specify output format for certain operations: xml,\n";
  cerr << "                             json\n";
  cerr << "   --purge-data              when specified, user removal will also purge all the\n";
  cerr << "                             user data\n";
  cerr << "   --purge-keys              when specified, subuser removal will also purge all the\n";
  cerr << "                             subuser keys\n";
  cerr << "   --purge-objects           remove a bucket's objects before deleting it\n";
  cerr << "                             (NOTE: required to delete a non-empty bucket)\n";
  cerr << "   --show-log-entries=<flag> enable/disable dump of log entries on log show\n";
  cerr << "   --show-log-sum=<flag>     enable/disable dump of log summation on log show\n";
  cerr << "   --skip-zero-entries       log show only dumps entries that don't have zero value\n";
  cerr << "                             in one of the numeric field\n";
  cerr << "   --categories=<list>       comma separated list of categories, used in usage show\n";
  cerr << "   --caps=<caps>             list of caps (e.g., \"usage=read, write; user=read\"\n";
  cerr << "   --yes-i-really-mean-it    required for certain operations\n";
  cerr << "\n";
  cerr << "<date> := \"YYYY-MM-DD[ hh:mm:ss]\"\n";
  cerr << "\n";
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
  OPT_POLICY,
  OPT_POOL_ADD,
  OPT_POOL_RM,
  OPT_POOLS_LIST,
  OPT_LOG_LIST,
  OPT_LOG_SHOW,
  OPT_LOG_RM,
  OPT_USAGE_SHOW,
  OPT_USAGE_TRIM,
  OPT_TEMP_REMOVE,
  OPT_OBJECT_RM,
  OPT_OBJECT_UNLINK,
  OPT_GC_LIST,
  OPT_GC_PROCESS,
  OPT_ZONE_INFO,
  OPT_ZONE_SET,
  OPT_CAPS_ADD,
  OPT_CAPS_RM,
};

static int get_cmd(const char *cmd, const char *prev_cmd, bool *need_more)
{
  *need_more = false;
  if (strcmp(cmd, "bucket") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "caps") == 0 ||
      strcmp(cmd, "gc") == 0 || 
      strcmp(cmd, "key") == 0 ||
      strcmp(cmd, "log") == 0 ||
      strcmp(cmd, "object") == 0 ||
      strcmp(cmd, "pool") == 0 ||
      strcmp(cmd, "pools") == 0 ||
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
  } else if (strcmp(prev_cmd, "temp") == 0) {
    if (strcmp(cmd, "remove") == 0)
      return OPT_TEMP_REMOVE;
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
  } else if (strcmp(prev_cmd, "pools") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_POOLS_LIST;
  } else if (strcmp(prev_cmd, "object") == 0) {
    if (strcmp(cmd, "rm") == 0)
      return OPT_OBJECT_RM;
    if (strcmp(cmd, "unlink") == 0)
      return OPT_OBJECT_UNLINK;
  } else if (strcmp(prev_cmd, "zone") == 0) {
    if (strcmp(cmd, "info") == 0)
      return OPT_ZONE_INFO;
    if (strcmp(cmd, "set") == 0)
      return OPT_ZONE_SET;
  } else if (strcmp(prev_cmd, "gc") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_GC_LIST;
    if (strcmp(cmd, "process") == 0)
      return OPT_GC_PROCESS;
  }

  return -EINVAL;
}

string escape_str(string& src, char c)
{
  int pos = 0;
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
  encode_json("user_info", info, formatter);
  formatter->flush(cout);
  cout << std::endl;
}


static void dump_bucket_usage(map<RGWObjCategory, RGWBucketStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWBucketStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWBucketStats& s = iter->second;
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
  int r = store->get_bucket_info(NULL, bucket.name, bucket_info);
  if (r < 0)
    return r;

  map<RGWObjCategory, RGWBucketStats> stats;
  int ret = store->get_bucket_stats(bucket, stats);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }
  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_string("pool", bucket.pool);
  
  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  formatter->dump_string("owner", bucket_info.owner);
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

static int init_bucket(string& bucket_name, rgw_bucket& bucket)
{
  if (!bucket_name.empty()) {
    RGWBucketInfo bucket_info;
    int r = store->get_bucket_info(NULL, bucket_name, bucket_info);
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

  do {
    char buf[READ_CHUNK];

    r = read(fd, buf, READ_CHUNK);
    if (r < 0) {
      int err = -errno;
      cerr << "error while reading input" << std::endl;
      return err;
    }
    bl.append(buf, r);
  } while (r > 0);

  if (infile.size()) {
    close(fd);
  }

  return 0;
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
    t.decode_json(&p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}
int main(int argc, char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::string user_id, access_key, secret_key, user_email, display_name;
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
  int max_buckets = -1;
  map<string, bool> categories;
  string caps;
  int check_objects = false;
  std::string infile;
  RGWUserAdminOpState user_op;
  RGWBucketAdminOpState bucket_op;

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
    } else if (ceph_argparse_withlonglong(args, i, &tmp, &errs, "-a", "--auth-uid", (char*)NULL)) {
      if (!errs.str().empty()) {
	cerr << errs.str() << std::endl;
	exit(EXIT_FAILURE);
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--max-buckets", (char*)NULL)) {
      max_buckets = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--date", "--time", (char*)NULL)) {
      date = val;
      if (end_date.empty())
        end_date = date;
    } else if (ceph_argparse_witharg(args, i, &val, "--start-date", "--start-time", (char*)NULL)) {
      start_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--end-date", "--end-time", (char*)NULL)) {
      end_date = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--access", (char*)NULL)) {
      access = val;
      perm_mask = rgw_str_to_perm(access.c_str());
      set_perm = true;
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
    } else if (ceph_argparse_binary_flag(args, i, &check_objects, NULL, "--check-objects", (char*)NULL)) {
     // do nothing
    } else if (ceph_argparse_witharg(args, i, &val, "--caps", (char*)NULL)) {
      caps = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-i", "--infile", (char*)NULL)) {
      infile = val;
    } else {
      ++i;
    }
  }

  if (args.empty()) {
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

  RGWStreamFlusher f(formatter, cout);

  store = RGWStoreManager::get_storage(g_ceph_context, false);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  StoreDestructor store_destructor(store);

  /* populate user operation */

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

  if (set_perm)
    user_op.set_perm(perm_mask);

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
    user_op.set_generate_key(); // generate a new key by default
    ret = user.add(user_op, &err_msg);
    if (ret < 0) {
      cerr << "could not create user: " << err_msg << std::endl;
      return -ret;
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

    ret = user.info(info, &err_msg);
    if (ret < 0) {
      cerr << "could not fetch user info: " << err_msg << std::endl;
      return -ret;
    }

    show_user_info(info, formatter);

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

  if (opt_cmd == OPT_BUCKETS_LIST || opt_cmd == OPT_BUCKET_STATS) {
    if (opt_cmd == OPT_BUCKET_STATS)
      bucket_op.set_fetch_stats(true);

    RGWBucketAdminOp::info(store, bucket_op, f);
  }

  if (opt_cmd == OPT_BUCKET_LINK) {
   RGWBucketAdminOp::link(store, bucket_op);
  }

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    RGWBucketAdminOp::unlink(store, bucket_op);
  }

  if (opt_cmd == OPT_TEMP_REMOVE) {
    if (date.empty()) {
      cerr << "date wasn't specified" << std::endl;
      return usage();
    }
    string parsed_date, parsed_time;
    int r = parse_date(date, NULL, &parsed_date, &parsed_time);
    if (r < 0) {
      cerr << "failure parsing date: " << cpp_strerror(r) << std::endl;
      return 1;
    }
    r = store->remove_temp_objects(parsed_date, parsed_time);
    if (r < 0) {
      cerr << "failure removing temp objects: " << cpp_strerror(r) << std::endl;
      return 1;
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
      oid += string(bucket.name);
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
      formatter->dump_string("bucket_owner", entry.bucket_owner);
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
      ret = parse_date(start_date, &start_epoch);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }
    if (!end_date.empty()) {
      ret = parse_date(end_date, &end_epoch);
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
      ret = parse_date(start_date, &start_epoch);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return 1;
      }
    }

    if (!end_date.empty()) {
      ret = parse_date(end_date, &end_epoch);
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

  if (opt_cmd == OPT_OBJECT_RM) {
    int ret = init_bucket(bucket_name, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = rgw_remove_object(store, bucket, object);

    if (ret < 0) {
      cerr << "ERROR: object remove returned: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT_OBJECT_UNLINK) {
    int ret = init_bucket(bucket_name, bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    list<string> oid_list;
    oid_list.push_back(object);
    ret = store->remove_objs_from_index(bucket, oid_list);
    if (ret < 0) {
      cerr << "ERROR: remove_obj_from_index() returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  if (opt_cmd == OPT_BUCKET_CHECK) {
    RGWBucketAdminOp::check_index(store, bucket_op, f);
  }

  if (opt_cmd == OPT_BUCKET_RM) {
    RGWBucketAdminOp::remove_bucket(store, bucket_op);
  }

  if (opt_cmd == OPT_GC_LIST) {
    int ret;
    int index = 0;
    string marker;
    bool truncated;
    formatter->open_array_section("entries");

    do {
      list<cls_rgw_gc_obj_info> result;
      ret = store->list_gc_objs(&index, marker, 1000, result, &truncated);
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
	  formatter->dump_string("pool", obj.pool);
	  formatter->dump_string("oid", obj.oid);
	  formatter->dump_string("key", obj.key);
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

  if (opt_cmd == OPT_ZONE_INFO) {
    store->zone.dump(formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_ZONE_SET) {
    RGWZoneParams zone;
    zone.init_default();
    int ret = read_decode_json(infile, zone);
    if (ret < 0) {
      return 1;
    }

    ret = zone.store_info(g_ceph_context, store);
    if (ret < 0) {
      cerr << "ERROR: couldn't store zone info: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }

    zone.dump(formatter);
    formatter->flush(cout);
  }

  if (opt_cmd == OPT_USER_CHECK) {
    check_bad_user_bucket_mapping(store, user_id, fix);
  }

  return 0;
}
