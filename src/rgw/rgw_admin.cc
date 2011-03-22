#include <errno.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"

#include "common/armor.h"
#include "rgw_user.h"
#include "rgw_access.h"
#include "rgw_acl.h"
#include "rgw_log.h"
#include "auth/Crypto.h"


#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

void usage() 
{
  cerr << "usage: radosgw_admin <cmd> [options...]" << std::endl;
  cerr << "commands:\n";
  cerr << "  user create                create a new user\n" ;
  cerr << "  user modify                modify user\n";
  cerr << "  user info                  get user info\n";
  cerr << "  user rm                    remove user\n";
  cerr << "  buckets list               list buckets\n";
  cerr << "  policy                     read bucket/object policy\n";
  cerr << "  log show                   dump a log from specific bucket, date\n";
  cerr << "options:\n";
  cerr << "   --uid=<id>                S3 uid\n";
  cerr << "   --os-user=<group:name>    OpenStack user\n";
  cerr << "   --email=<email>\n";
  cerr << "   --auth_uid=<auid>         librados uid\n";
  cerr << "   --secret=<key>            S3 key\n";
  cerr << "   --display-name=<name>\n";
  cerr << "   --bucket=<bucket>\n";
  cerr << "   --object=<object>\n";
  cerr << "   --date=<yyyy-mm-dd>\n";
  generic_client_usage();
  exit(1);
}

enum {
  OPT_NO_CMD = 0,
  OPT_USER_CREATE,
  OPT_USER_INFO,
  OPT_USER_MODIFY,
  OPT_USER_RM,
  OPT_BUCKETS_LIST,
  OPT_POLICY,
  OPT_LOG_SHOW,
};

static int get_cmd(const char *cmd, const char *prev_cmd, bool *need_more)
{
  *need_more = false;
  if (strcmp(cmd, "user") == 0 ||
      strcmp(cmd, "buckets") == 0 ||
      strcmp(cmd, "log") == 0) {
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
  } else if (strcmp(prev_cmd, "buckets") == 0) {
    if (strcmp(cmd, "list") == 0)
      return OPT_BUCKETS_LIST;
  } else if (strcmp(prev_cmd, "log") == 0) {
    if (strcmp(cmd, "show") == 0)
      return OPT_LOG_SHOW;
  }

  return -EINVAL;
}

int gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  ret = get_random_bytes(buf, sizeof(buf));
  if (ret < 0) {
    // assuming no threads here, for strerror
    cerr << "cannot get random bytes: " << strerror(-ret) << std::endl;
    return -1;
  }

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
		   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    cerr << "ceph_armor failed" << std::endl;
    return -1;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size] = '\0';

  return 0;
}

static const char alphanum_table[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

int gen_rand_alphanumeric(char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    // assuming no threads here, for strerror
    cerr << "cannot get random bytes: " << strerror(-ret) << std::endl;
    return -1;
  }

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_table[pos % (sizeof(alphanum_table) - 1)];
  }
  dest[i] = '\0';

  return 0;
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

int main(int argc, char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  keyring_init(&g_conf);

  const char *user_id = 0;
  const char *secret_key = 0;
  const char *user_email = 0;
  const char *display_name = 0;
  const char *bucket = 0;
  const char *object = 0;
  const char *openstack_user = 0;
  const char *date = 0;
  uint64_t auid = 0;
  RGWUserInfo info;
  RGWAccess *store;
  const char *prev_cmd = NULL;
  int opt_cmd = OPT_NO_CMD;
  bool need_more;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("uid", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&user_id, OPT_STR);
    } else if (CONF_ARG_EQ("secret", 's')) {
      CONF_SAFE_SET_ARG_VAL(&secret_key, OPT_STR);
    } else if (CONF_ARG_EQ("email", 'e')) {
      CONF_SAFE_SET_ARG_VAL(&user_email, OPT_STR);
    } else if (CONF_ARG_EQ("display-name", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&display_name, OPT_STR);
    } else if (CONF_ARG_EQ("bucket", 'b')) {
      CONF_SAFE_SET_ARG_VAL(&bucket, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'o')) {
      CONF_SAFE_SET_ARG_VAL(&object, OPT_STR);
    } else if (CONF_ARG_EQ("auth-uid", 'a')) {
      CONF_SAFE_SET_ARG_VAL(&auid, OPT_LONGLONG);
    } else if (CONF_ARG_EQ("os-user", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&openstack_user, OPT_STR);
    } else if (CONF_ARG_EQ("date", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&date, OPT_STR);
    } else {
      if (!opt_cmd) {
        opt_cmd = get_cmd(CONF_VAL, prev_cmd, &need_more);
        if (opt_cmd < 0) {
          cerr << "unrecognized arg " << args[i] << std::endl;
          ARGS_USAGE();
        }
        if (need_more) {
          prev_cmd = CONF_VAL;
          continue;
        }
      } else {
        cerr << "unrecognized arg " << args[i] << std::endl;
        ARGS_USAGE();
      }
    }
  }

  if (opt_cmd == OPT_NO_CMD)
    ARGS_USAGE();

  store = RGWAccess::init_storage_provider("rados", &g_conf);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  string user_id_str;

  if (opt_cmd != OPT_USER_CREATE && opt_cmd != OPT_LOG_SHOW && !user_id) {
    bool found = false;
    string s;
    if (user_email) {
      s = user_email;
      if (rgw_get_uid_by_email(s, user_id_str) >= 0) {
	found = true;
      } else {
	cerr << "could not find user by specified email" << std::endl;
      }
    }
    if (!found && openstack_user) {
      s = openstack_user;
      if (rgw_get_uid_by_openstack(s, user_id_str) >= 0) {
	found = true;
      } else
        cerr << "could not find user by specified openstack username" << std::endl;
    }
    if (found)
      user_id = user_id_str.c_str();
  }


  if (opt_cmd == OPT_USER_MODIFY || opt_cmd == OPT_USER_INFO) {
    if (!user_id) {
      cerr << "user_id was not specified, aborting" << std::endl;
      return 0;
    }

    string user_id_str = user_id;

    if (rgw_get_user_info(user_id_str, info) < 0) {
      cerr << "error reading user info, aborting" << std::endl;
      exit(1);
    }
  }

  if (opt_cmd == OPT_USER_CREATE) {
    char secret_key_buf[SECRET_KEY_LEN + 1];
    char public_id_buf[PUBLIC_ID_LEN + 1];
    int ret;

    if (!display_name) {
      cerr << "display name was not specified, aborting" << std::endl;
      return 0;
    }

    if (!secret_key) {
      ret = gen_rand_base64(secret_key_buf, sizeof(secret_key_buf));
      if (ret < 0) {
        cerr << "aborting" << std::endl;
        exit(1);
      }
      secret_key = secret_key_buf;
    }
    if (!user_id) {
      RGWUserInfo duplicate_check;
      string duplicate_check_id;
      do {
	ret = gen_rand_alphanumeric(public_id_buf, sizeof(public_id_buf));
	if (ret < 0) {
	  cerr << "aborting" << std::endl;
	  exit(1);
	}
	user_id = public_id_buf;
	duplicate_check_id = user_id;
      } while (!rgw_get_user_info(duplicate_check_id, duplicate_check));
    }
  }


  int err;
  switch (opt_cmd) {
  case OPT_USER_CREATE:
  case OPT_USER_MODIFY:
    if (user_id)
      info.user_id = user_id;
    if (secret_key)
      info.secret_key = secret_key;
    if (display_name)
      info.display_name = display_name;
    if (user_email)
      info.user_email = user_email;
    if (auid)
      info.auid = auid;
    if (openstack_user)
      info.openstack_name = openstack_user;
  
    if ((err = rgw_store_user_info(info)) < 0) {
      cerr << "error storing user info: " << strerror(-err) << std::endl;
      break;
    }

    /* fall through */

  case OPT_USER_INFO:
    cout << "User ID: " << info.user_id << std::endl;
    cout << "Secret Key: " << info.secret_key << std::endl;
    cout << "Display Name: " << info.display_name << std::endl;
    cout << "OpenStack User: " << (info.openstack_name.size() ? info.openstack_name : "<undefined>")<< std::endl;
    break;
  }

  if (opt_cmd == OPT_POLICY) {
    bufferlist bl;
    if (!bucket)
      bucket = "";
    if (!object)
      object = "";
    string bucket_str(bucket);
    string object_str(object);
    int ret = store->get_attr(bucket_str, object_str,
                       RGW_ATTR_ACL, bl);

    RGWAccessControlPolicy policy;
    if (ret >= 0) {
      bufferlist::iterator iter = bl.begin();
      policy.decode(iter);
      policy.to_xml(cout);
      cout << std::endl;
    }
  }

  if (opt_cmd == OPT_BUCKETS_LIST) {
    string id;
    RGWAccessHandle handle;

    if (user_id) {
      RGWUserBuckets buckets;
      if (rgw_read_user_buckets(user_id, buckets, false) < 0) {
        cout << "could not get buckets for uid " << user_id << std::endl;
      } else {
        map<string, RGWBucketEnt>& m = buckets.get_buckets();
        map<string, RGWBucketEnt>::iterator iter;

        for (iter = m.begin(); iter != m.end(); ++iter) {
          RGWBucketEnt obj = iter->second;
          cout << obj.name << std::endl;
        }
      }
    } else {
      if (store->list_buckets_init(id, &handle) < 0) {
        cout << "list-buckets: no entries found" << std::endl;
      } else {
        RGWObjEnt obj;
        cout << "listing all buckets" << std::endl;
        while (store->list_buckets_next(id, obj, &handle) >= 0) {
          cout << obj.name << std::endl;
        }
      }
    }
  }

  if (opt_cmd == OPT_LOG_SHOW) {
    if (!date || !bucket) {
      if (!date)
        cerr << "date was not specified" << std::endl;
      if (!bucket)
        cerr << "bucket was not specified" << std::endl;
      ARGS_USAGE();
    }

    string log_bucket = RGW_LOG_BUCKET_NAME;
    string oid = date;
    oid += "-";
    oid += string(bucket);
    uint64_t size;
    int r = store->obj_stat(log_bucket, oid, &size, NULL);
    if (r < 0) {
      cerr << "error while doing stat on " <<  log_bucket << ":" << oid << " " << strerror(-r) << std::endl;
      return -r;
    }
    bufferlist bl;
    r = store->read(log_bucket, oid, 0, size, bl);
    if (r < 0) {
      cerr << "error while reading from " <<  log_bucket << ":" << oid << " " << strerror(-r) << std::endl;
      return -r;
    }

    bufferlist::iterator iter = bl.begin();

    struct rgw_log_entry entry;
    const char *delim = " ";

    while (!iter.end()) {
      ::decode(entry, iter);

      cout << (entry.owner.size() ? entry.owner : "-" ) << delim
           << entry.bucket << delim
           << entry.time << delim
           << entry.remote_addr << delim
           << entry.user << delim
           << entry.op << delim
           << "\"" << escape_str(entry.uri, '"') << "\"" << delim
           << entry.http_status << delim
           << entry.error_code << delim
           << entry.bytes_sent << delim
           << entry.obj_size << delim
           << entry.total_time.usec() << delim
           << "\"" << escape_str(entry.user_agent, '"') << "\"" << delim
           << "\"" << escape_str(entry.referrer, '"') << "\"" << std::endl;
    }
  }

  if (opt_cmd == OPT_USER_RM) {
    rgw_delete_user(info);
  }

  return 0;
}
