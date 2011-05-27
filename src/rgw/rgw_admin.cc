#include <errno.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/errno.h"

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
  cerr << "  subuser create             create a new subuser\n" ;
  cerr << "  subuser modify             modify subuser\n";
  cerr << "  subuser rm                 remove subuser\n";
  cerr << "  key create                 create access key\n";
  cerr << "  key rm                     remove access key\n";
  cerr << "  buckets list               list buckets\n";
  cerr << "  bucket unlink              unlink bucket from specified user\n";
  cerr << "  policy                     read bucket/object policy\n";
  cerr << "  log show                   dump a log from specific object or (bucket + date)\n";
  cerr << "options:\n";
  cerr << "   --uid=<id>                user id\n";
  cerr << "   --subuser=<name>          subuser name\n";
  cerr << "   --access-key=<key>        S3 access key\n";
  cerr << "   --os-user=<group:name>    OpenStack user\n";
  cerr << "   --email=<email>\n";
  cerr << "   --auth_uid=<auid>         librados uid\n";
  cerr << "   --secret=<key>            S3 key\n";
  cerr << "   --os-secret=<key>         OpenStack key\n";
  cerr << "   --gen-access-key          generate random access key\n";
  cerr << "   --gen-secret              generate random secret key\n";
  cerr << "   --access=<access>         Set access permissions for sub-user, should be one\n";
  cerr << "                             of read, write, readwrite, full\n";
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
  OPT_SUBUSER_CREATE,
  OPT_SUBUSER_MODIFY,
  OPT_SUBUSER_RM,
  OPT_KEY_CREATE,
  OPT_KEY_RM,
  OPT_BUCKETS_LIST,
  OPT_BUCKET_UNLINK,
  OPT_POLICY,
  OPT_LOG_SHOW,
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

  usage();
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
    if (strcmp(cmd, "unlink") == 0)
      return OPT_BUCKET_UNLINK;
  } else if (strcmp(prev_cmd, "log") == 0) {
    if (strcmp(cmd, "show") == 0)
      return OPT_LOG_SHOW;
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

static void show_user_info( RGWUserInfo& info)
{
  map<string, RGWAccessKey>::iterator kiter;
  map<string, RGWSubUser>::iterator uiter;

  cout << "User ID: " << info.user_id << std::endl;
  cout << "Keys:" << std::endl;
  for (kiter = info.access_keys.begin(); kiter != info.access_keys.end(); ++kiter) {
    RGWAccessKey& k = kiter->second;
    cout << " User: " << info.user_id << (k.subuser.empty() ? "" : ":") << k.subuser << std::endl;
    cout << "  Access Key: " << k.id << std::endl;
    cout << "  Secret Key: " << k.key << std::endl;
  }
  cout << "Users: " << std::endl;
  for (uiter = info.subusers.begin(); uiter != info.subusers.end(); ++uiter) {
    RGWSubUser& u = uiter->second;
    cout << " Name: " << info.user_id << ":" << u.name << std::endl;
    char buf[256];
    perm_to_str(u.perm_mask, buf, sizeof(buf));
    cout << " Permissions: " << buf << std::endl;
  }
  cout << "Display Name: " << info.display_name << std::endl;
  cout << "Email: " << info.user_email << std::endl;
  cout << "OpenStack User: " << (info.openstack_name.size() ? info.openstack_name : "<undefined>")<< std::endl;
  cout << "OpenStack Key: " << (info.openstack_key.size() ? info.openstack_key : "<undefined>")<< std::endl;
}

int main(int argc, char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(&g_ceph_context);

  const char *user_id = 0;
  const char *access_key = 0;
  const char *secret_key = 0;
  const char *user_email = 0;
  const char *display_name = 0;
  const char *bucket = 0;
  const char *object = 0;
  const char *openstack_user = 0;
  const char *openstack_key = 0;
  const char *date = 0;
  const char *subuser = 0;
  const char *access = 0;
  uint32_t perm_mask = 0;
  uint64_t auid = 0;
  RGWUserInfo info;
  RGWAccess *store;
  const char *prev_cmd = NULL;
  int opt_cmd = OPT_NO_CMD;
  bool need_more;
  bool gen_secret;
  bool gen_key;
  char secret_key_buf[SECRET_KEY_LEN + 1];
  char public_id_buf[PUBLIC_ID_LEN + 1];
  bool user_modify_op;

  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("uid", 'i')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&user_id, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("access-key", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&access_key, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("subuser", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&subuser, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("secret", 's')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&secret_key, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("email", 'e')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&user_email, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("display-name", 'n')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&display_name, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("bucket", 'b')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&bucket, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("object", 'o')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&object, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("gen-access-key", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&gen_key, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("gen-secret", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&gen_secret, OPT_BOOL);
    } else if (CEPH_ARGPARSE_EQ("auth-uid", 'a')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&auid, OPT_LONGLONG);
    } else if (CEPH_ARGPARSE_EQ("os-user", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&openstack_user, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("os-secret", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&openstack_key, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("date", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&date, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("access", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&access, OPT_STR);
      perm_mask = str_to_perm(access);
    } else {
      if (!opt_cmd) {
        opt_cmd = get_cmd(CEPH_ARGPARSE_VAL, prev_cmd, &need_more);
        if (opt_cmd < 0) {
          cerr << "unrecognized arg " << args[i] << std::endl;
          usage();
        }
        if (need_more) {
          prev_cmd = CEPH_ARGPARSE_VAL;
          continue;
        }
      } else {
        cerr << "unrecognized arg " << args[i] << std::endl;
        usage();
      }
    }
  }

  if (opt_cmd == OPT_NO_CMD)
    usage();

  if (subuser) {
    char *suser = strdup(subuser);
    char *p = strchr(suser, ':');
    if (!p) {
      free(suser);
    } else {
      *p = '\0';
      if (user_id) {
        if (strcmp(user_id, suser) != 0) {
          cerr << "bad subuser " << subuser << " for uid " << user_id << std::endl;
          exit(1);
        }
      } else
        user_id = suser;
      subuser = p + 1;
    }
  }

  if (opt_cmd == OPT_KEY_RM && !access_key) {
    cerr << "error: access key was not specified" << std::endl;
    usage();
  }

  user_modify_op = (opt_cmd == OPT_USER_MODIFY || opt_cmd == OPT_SUBUSER_MODIFY ||
                    opt_cmd == OPT_SUBUSER_CREATE || opt_cmd == OPT_SUBUSER_RM ||
                    opt_cmd == OPT_KEY_CREATE || opt_cmd == OPT_KEY_RM);

  store = RGWAccess::init_storage_provider("rados", &g_ceph_context);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }

  if (opt_cmd != OPT_USER_CREATE && opt_cmd != OPT_LOG_SHOW && !user_id) {
    bool found = false;
    string s;
    if (!found && user_email) {
      s = user_email;
      if (rgw_get_user_info_by_email(s, info) >= 0) {
	found = true;
      } else {
	cerr << "could not find user by specified email" << std::endl;
      }
    }
    if (!found && access_key) {
      s = access_key;
      if (rgw_get_user_info_by_access_key(s, info) >= 0) {
	found = true;
      } else {
	cerr << "could not find user by specified access key" << std::endl;
      }
    }
    if (!found && openstack_user) {
      s = openstack_user;
      if (rgw_get_user_info_by_openstack(s, info) >= 0) {
	found = true;
      } else
        cerr << "could not find user by specified openstack username" << std::endl;
    }
    if (found)
      user_id = info.user_id.c_str();
  }


  if (user_modify_op || opt_cmd == OPT_USER_CREATE ||
      opt_cmd == OPT_USER_INFO || opt_cmd == OPT_BUCKET_UNLINK) {
    if (!user_id) {
      cerr << "user_id was not specified, aborting" << std::endl;
      usage();
    }

    string user_id_str = user_id;

    bool found = (rgw_get_user_info_by_uid(user_id_str, info) >= 0);

    if (opt_cmd == OPT_USER_CREATE) {
      if (found) {
        cerr << "error: user already exists" << std::endl;
        exit(1);
      }
    } else if (!found) {
      cerr << "error reading user info, aborting" << std::endl;
      exit(1);
    }
  }

  if (opt_cmd == OPT_SUBUSER_CREATE || opt_cmd == OPT_SUBUSER_MODIFY ||
      opt_cmd == OPT_SUBUSER_RM) {
    if (!subuser) {
      cerr << "subuser creation was requires specifying subuser name" << std::endl;
      exit(1);
    }
    map<string, RGWSubUser>::iterator iter = info.subusers.find(subuser);
    bool found = (iter != info.subusers.end());
    if (opt_cmd == OPT_SUBUSER_CREATE) {
      if (found) {
        cerr << "error: subuser already exists" << std::endl;
        exit(1);
      }
    } else if (!found) {
      cerr << "error: subuser doesn't exist" << std::endl;
      exit(1);
    }
  }

  bool keys_not_requested = (!access_key && !secret_key && !gen_secret && !gen_key &&
                             opt_cmd != OPT_KEY_CREATE);

  if (opt_cmd == OPT_USER_CREATE || (user_modify_op && !keys_not_requested)) {
    int ret;

    if (opt_cmd == OPT_USER_CREATE && !display_name) {
      cerr << "display name was not specified, aborting" << std::endl;
      return 0;
    }

    if (!secret_key || gen_secret) {
      ret = gen_rand_base64(secret_key_buf, sizeof(secret_key_buf));
      if (ret < 0) {
        cerr << "aborting" << std::endl;
        exit(1);
      }
      secret_key = secret_key_buf;
    }
    if (!access_key || gen_key) {
      RGWUserInfo duplicate_check;
      string duplicate_check_id;
      do {
	ret = gen_rand_alphanumeric_upper(public_id_buf, sizeof(public_id_buf));
	if (ret < 0) {
	  cerr << "aborting" << std::endl;
	  exit(1);
	}
	access_key = public_id_buf;
	duplicate_check_id = access_key;
      } while (!rgw_get_user_info_by_access_key(duplicate_check_id, duplicate_check));
    }
  }

  map<string, RGWAccessKey>::iterator kiter;
  map<string, RGWSubUser>::iterator uiter;
  int err;
  switch (opt_cmd) {
  case OPT_USER_CREATE:
  case OPT_USER_MODIFY:
  case OPT_SUBUSER_CREATE:
  case OPT_SUBUSER_MODIFY:
  case OPT_KEY_CREATE:
    if (user_id)
      info.user_id = user_id;
    if (access_key && secret_key) {
      RGWAccessKey k;
      k.id = access_key;
      k.key = secret_key;
      if (subuser)
        k.subuser = subuser;
      info.access_keys[access_key] = k;
   } else if (access_key || secret_key) {
      cerr << "access key modification requires both access key and secret key" << std::endl;
      exit(1);
    }
    if (display_name)
      info.display_name = display_name;
    if (user_email)
      info.user_email = user_email;
    if (auid)
      info.auid = auid;
    if (openstack_user)
      info.openstack_name = openstack_user;
    if (openstack_key)
      info.openstack_key = openstack_key;
    if (subuser) {
      RGWSubUser u;
      u.name = subuser;
      u.perm_mask = perm_mask;

      info.subusers[subuser] = u;
    }
    if ((err = rgw_store_user_info(info)) < 0) {
      cerr << "error storing user info: " << cpp_strerror(-err) << std::endl;
      break;
    }

    show_user_info(info);
    break;

  case OPT_SUBUSER_RM:
    uiter = info.subusers.find(subuser);
    assert (uiter != info.subusers.end());
    info.subusers.erase(uiter);
    if ((err = rgw_store_user_info(info)) < 0) {
      cerr << "error storing user info: " << cpp_strerror(-err) << std::endl;
      break;
    }
    show_user_info(info);
    break;

  case OPT_KEY_RM:
    kiter = info.access_keys.find(access_key);
    if (kiter == info.access_keys.end()) {
      cerr << "key not found" << std::endl;
    } else {
      rgw_remove_key_storage(kiter->second);
      info.access_keys.erase(kiter);
      if ((err = rgw_store_user_info(info)) < 0) {
        cerr << "error storing user info: " << cpp_strerror(-err) << std::endl;
        break;
      }
    }
    show_user_info(info);
    break;

  case OPT_USER_INFO:
    show_user_info(info);
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

  if (opt_cmd == OPT_BUCKET_UNLINK) {
    if (!bucket) {
      cerr << "bucket name was not specified" << std::endl;
      usage();
    }
    string bucket_str(bucket);
    int r = rgw_remove_bucket(user_id, bucket_str);
    if (r < 0)
      cerr << "error unlinking bucket " <<  cpp_strerror(-r) << std::endl;
    return -r;
  }

  if (opt_cmd == OPT_LOG_SHOW) {
    if (!object && (!date || !bucket)) {
      cerr << "object or (both date and bucket) were not specified" << std::endl;
      usage();
    }

    string log_bucket = RGW_LOG_BUCKET_NAME;
    string oid;
    if (object) {
      oid = object;
    } else {
      oid = date;
      oid += "-";
      oid += string(bucket);
    }

    uint64_t size;
    int r = store->obj_stat(log_bucket, oid, &size, NULL);
    if (r < 0) {
      cerr << "error while doing stat on " <<  log_bucket << ":" << oid
	   << " " << cpp_strerror(-r) << std::endl;
      return -r;
    }
    bufferlist bl;
    r = store->read(log_bucket, oid, 0, size, bl);
    if (r < 0) {
      cerr << "error while reading from " <<  log_bucket << ":" << oid
	   << " " << cpp_strerror(-r) << std::endl;
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
           << "\"" << entry.error_code << "\"" << delim
           << entry.bytes_sent << delim
           << entry.bytes_received << delim
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
