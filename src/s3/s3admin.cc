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
  cerr << "usage: s3admin [--user-gen=display_name]" << std::endl;
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

  const char *display_name = 0;
  bool gen_user = false;
  int actions = 0 ;

  if (g_conf.clock_tare) g_clock.tare();

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("user-gen", 'g')) {
      CONF_SAFE_SET_ARG_VAL(&display_name, OPT_STR);
      gen_user = true;
    } else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      ARGS_USAGE();
    }
  }

  if (gen_user) {
    actions++;
    char secret_key[SECRET_KEY_LEN + 1];
    char public_id[PUBLIC_ID_LEN + 1];
    int ret;

    ret = gen_rand_base64(secret_key, sizeof(secret_key));
    if (ret < 0) {
      cerr << "aborting" << std::endl;
      exit(1);
    }
    ret = gen_rand_base64(public_id, sizeof(public_id));
    if (ret < 0) {
      cerr << "aborting" << std::endl;
      exit(1);
    }
    cout << "User ID: " << public_id << std::endl;
    cout << "Secret Key: " << secret_key << std::endl;
    cout << "Display Name: " << display_name << std::endl;

    S3UserInfo info;
    info.user_id = public_id;
    info.secret_key = secret_key;
    info.display_name = display_name;
    if (s3_store_user_info(info) < 0) {
      cerr << "error storing user info" << std::endl;
    }
  }

  if (!actions)
    ARGS_USAGE();

  return 0;
}


