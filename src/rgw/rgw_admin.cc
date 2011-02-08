#include <errno.h>

#include <iostream>
#include <string>

using namespace std;

#include "config.h"

#include <cryptopp/osrng.h>
#include "common/common_init.h"

#include "common/armor.h"
#include "rgw_user.h"
#include "rgw_access.h"
#include "rgw_acl.h"


#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

void usage() 
{
  cerr << "usage: radosgw_admin <--user-gen | --user-modify | --read-policy | --list-buckets > [options...]" << std::endl;
  cerr << "options:" << std::endl;
  cerr << "   --uid=<id> (S3 uid)" << std::endl;
  cerr << "   --auth_uid=<auid> (librados uid)" << std::endl;
  cerr << "   --secret=<key>" << std::endl;
  cerr << "   --email=<email>" << std::endl;
  cerr << "   --display-name=<name>" << std::endl;
  cerr << "   --bucket=<bucket>" << std::endl;
  cerr << "   --object=<object>" << std::endl;
  generic_client_usage();
  exit(1);
}

int gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  unsigned char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */

  CryptoPP::AutoSeededRandomPool rng;
  rng.GenerateBlock(buf, sizeof(buf));

  int ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
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
  CryptoPP::AutoSeededRandomPool rng;
  rng.GenerateBlock((unsigned char *)dest, size);

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_table[pos % (sizeof(alphanum_table) - 1)];
  }
  dest[i] = '\0';

  return 0;
}




int main(int argc, char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  common_set_defaults(false);
  common_init(args, "rgw",
	      STARTUP_FLAG_FORCE_FG_LOGGING | STARTUP_FLAG_INIT_KEYS);

  const char *user_id = 0;
  const char *secret_key = 0;
  const char *user_email = 0;
  const char *display_name = 0;
  const char *bucket = 0;
  const char *object = 0;
  bool gen_user = false;
  bool mod_user = false;
  bool read_policy = false;
  bool list_buckets = false;
  bool delete_user = false;
  int actions = 0 ;
  uint64_t auid = 0;
  RGWUserInfo info;
  RGWAccess *store;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("user-gen", 'g')) {
      gen_user = true;
    } else if (CONF_ARG_EQ("user-modify", 'm')) {
      mod_user = true;
    } else if (CONF_ARG_EQ("read-policy", 'p')) {
      read_policy = true;
    } else if (CONF_ARG_EQ("list-buckets", 'l')) {
      list_buckets = true;
    } else if (CONF_ARG_EQ("uid", 'i')) {
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
    } else if (CONF_ARG_EQ("auth_uid", 'a')) {
      CONF_SAFE_SET_ARG_VAL(&auid, OPT_LONGLONG);
    } else if (CONF_ARG_EQ("delete_user", 'd')) {
      delete_user = true;
    } else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      ARGS_USAGE();
    }
  }

  store = RGWAccess::init_storage_provider("rados", argc, argv);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
    return 5; //EIO
  }


  if (mod_user) {
    actions++;

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

  if (gen_user) {
    actions++;
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

  if (gen_user || mod_user) {
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

    int err;
    if ((err = rgw_store_user_info(info)) < 0) {
      cerr << "error storing user info" << strerror(-err) << std::endl;
    } else {
      cout << "User ID: " << info.user_id << std::endl;
      cout << "Secret Key: " << info.secret_key << std::endl;
      cout << "Display Name: " << info.display_name << std::endl;
    }
  }

  if (read_policy) {
    actions++;
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

  if (list_buckets) {
    actions++;
    string id;
    RGWAccessHandle handle;

    if (user_id) {
      RGWUserBuckets buckets;
      if (rgw_get_user_buckets(user_id, buckets) < 0) {
        cout << "could not get buckets for uid " << user_id << std::endl;
      } else {
        cout << "listing buckets for uid " << user_id << std::endl;
        map<string, RGWObjEnt>& m = buckets.get_buckets();
        map<string, RGWObjEnt>::iterator iter;

        for (iter = m.begin(); iter != m.end(); ++iter) {
          RGWObjEnt obj = iter->second;
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

  if (delete_user) {
    ++actions;
    rgw_delete_user(info);
  }

  if (!actions)
    ARGS_USAGE();

  return 0;
}
