#include <iostream>
#include <string>

using namespace std;

#include "config.h"

#include <openssl/rand.h>
#include "common/common_init.h"

#include "include/base64.h"
#include "s3/user.h"


#define SECRET_KEY_LEN 40
#define PUBLIC_ID_LEN 20

void usage() 
{
  cerr << "usage: s3admin <--user-gen | --user_modify> [options...]" << std::endl;
  cerr << "options:" << std::endl;
  cerr << "   --uid=<id>" << std::endl;
  cerr << "   --key=<key>" << std::endl;
  cerr << "   --display-name=<name>" << std::endl;
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


int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "s3a", true);

  const char *user_id = 0;
  const char *secret_key = 0;
  const char *display_name = 0;
  bool gen_user = false;
  bool mod_user = false;
  int actions = 0 ;
  S3UserInfo info;

  if (g_conf.clock_tare) g_clock.tare();

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("user-gen", 'g')) {
      gen_user = true;
    } else if (CONF_ARG_EQ("user-modify", 'g')) {
      mod_user = true;
    } else if (CONF_ARG_EQ("uid", 'i')) {
      CONF_SAFE_SET_ARG_VAL(&user_id, OPT_STR);
    } else if (CONF_ARG_EQ("key", 'k')) {
      CONF_SAFE_SET_ARG_VAL(&secret_key, OPT_STR);
    } else if (CONF_ARG_EQ("display-name", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&display_name, OPT_STR);
    } else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      ARGS_USAGE();
    }
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
      ret = gen_rand_base64(public_id_buf, sizeof(public_id_buf));
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

    if (s3_store_user_info(info) < 0) {
      cerr << "error storing user info" << std::endl;
    } else {
      cout << "User ID: " << info.user_id << std::endl;
      cout << "Secret Key: " << info.secret_key << std::endl;
      cout << "Display Name: " << info.display_name << std::endl;
    }
  }

  if (!actions)
    ARGS_USAGE();


  return 0;
}


