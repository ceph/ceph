#include <errno.h>

#include <iostream>
#include <string>

using namespace std;

#include "config.h"

#include <openssl/rand.h>
#include "common/common_init.h"

#include "include/base64.h"
#include "s3/user.h"
#include "s3access.h"
#include "s3acl.h"


#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

void usage() 
{
  cerr << "usage: s3admin <--user-gen | --user-modify | --read-policy | --list-buckets > [options...]" << std::endl;
  cerr << "options:" << std::endl;
  cerr << "   --uid=<id>" << std::endl;
  cerr << "   --key=<key>" << std::endl;
  cerr << "   --email=<email>" << std::endl;
  cerr << "   --display-name=<name>" << std::endl;
  cerr << "   --bucket=<bucket>" << std::endl;
  cerr << "   --object=<object>" << std::endl;
  generic_usage();
}

int gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  unsigned char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */

  int ret = RAND_bytes(buf, sizeof(buf));
  if (!ret) {
    cerr << "RAND_bytes failed, entropy problem?" << std::endl;
    return -1;
  }

  ret = encode_base64((const char *)buf, ((size - 1) * 3 + 4 - 1) / 4, tmp_dest, sizeof(tmp_dest));
  if (ret < 0) {
    cerr << "encode_base64 failed" << std::endl;
    return -1;
  }
  memcpy(dest, tmp_dest, size);
  dest[size] = '\0';

  return 0;
}

static const char alphanum_table[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

int gen_rand_alphanumeric(char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = RAND_bytes((unsigned char *)dest, size);
  if (!ret) {
    cerr << "RAND_bytes failed, entropy problem?" << std::endl;
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

static int rebuild_policy(S3AccessControlPolicy& src, S3AccessControlPolicy& dest)
{
  ACLOwner *owner = (ACLOwner *)src.find_first("Owner");
  if (!owner)
    return -EINVAL;

  S3UserInfo owner_info;
  if (s3_get_user_info(owner->get_id(), owner_info) < 0) {
    cerr << "owner info does not exist" << std::endl;
    return -EINVAL;
  }
  ACLOwner& new_owner = dest.get_owner();
  new_owner.set_id(owner->get_id());
  new_owner.set_name(owner_info.display_name);

  S3AccessControlList& src_acl = src.get_acl();
  S3AccessControlList& acl = dest.get_acl();

  XMLObjIter iter = src_acl.find("Grant");
  ACLGrant *src_grant = (ACLGrant *)iter.get_next();
  while (src_grant) {
    string id = src_grant->get_id();
    
    S3UserInfo grant_user;
    if (s3_get_user_info(id, grant_user) < 0) {
      cerr << "grant user does not exist:" << id << std::endl;
    } else {
      ACLGrant new_grant;
      ACLPermission& perm = src_grant->get_permission();
      new_grant.set_canon(id, grant_user.display_name, perm.get_permissions());
      cerr << "new grant: " << id << ":" << grant_user.display_name << std::endl;
      acl.add_grant(&new_grant);
    }
    src_grant = (ACLGrant *)iter.get_next();
  }

  return 0; 
}


int main(int argc, char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);
  common_init(args, "s3a", true);

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
  int actions = 0 ;
  S3UserInfo info;
  S3Access *store;

  if (g_conf.clock_tare) g_clock.tare();

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
    } else if (CONF_ARG_EQ("key", 'k')) {
      CONF_SAFE_SET_ARG_VAL(&secret_key, OPT_STR);
    } else if (CONF_ARG_EQ("email", 'e')) {
      CONF_SAFE_SET_ARG_VAL(&user_email, OPT_STR);
    } else if (CONF_ARG_EQ("display-name", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&display_name, OPT_STR);
    } else if (CONF_ARG_EQ("bucket", 'b')) {
      CONF_SAFE_SET_ARG_VAL(&bucket, OPT_STR);
    } else if (CONF_ARG_EQ("object", 'o')) {
      CONF_SAFE_SET_ARG_VAL(&object, OPT_STR);
    } else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      ARGS_USAGE();
    }
  }

  store = S3Access::init_storage_provider("rados", argc, argv);
  if (!store) {
    cerr << "couldn't init storage provider" << std::endl;
  }


  if (mod_user) {
    actions++;

    if (!user_id) {
      cerr << "user_id was not specified, aborting" << std::endl;
      return 0;
    }

    string user_id_str = user_id;

    if (s3_get_user_info(user_id_str, info) < 0) {
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
      ret = gen_rand_alphanumeric(public_id_buf, sizeof(public_id_buf));
      if (ret < 0) {
        cerr << "aborting" << std::endl;
        exit(1);
      }
      user_id = public_id_buf;
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

    if (s3_store_user_info(info) < 0) {
      cerr << "error storing user info" << std::endl;
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
                       S3_ATTR_ACL, bl);

    S3AccessControlPolicy policy;
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
    S3AccessHandle handle;

    if (user_id) {
      S3UserBuckets buckets;
      if (s3_get_user_buckets(user_id, buckets) < 0) {
        cout << "could not get buckets for uid " << user_id << std::endl;
      } else {
        cout << "listing buckets for uid " << user_id << std::endl;
        map<string, S3ObjEnt>& m = buckets.get_buckets();
        map<string, S3ObjEnt>::iterator iter;

        for (iter = m.begin(); iter != m.end(); ++iter) {
          S3ObjEnt obj = iter->second;
          cout << obj.name << std::endl;
        }
      }
    } else {
      if (store->list_buckets_init(id, &handle) < 0) {
        cout << "list-buckets: no entries found" << std::endl;
      } else {
        S3ObjEnt obj;
        cout << "listing all buckets" << std::endl;
        while (store->list_buckets_next(id, obj, &handle) >= 0) {
          cout << obj.name << std::endl;
        }
      }
    }
  }

  if (!actions)
    ARGS_USAGE();

  return 0;
}


