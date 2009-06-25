#include <string>

#include "s3access.h"
#include "s3acl.h"

#include "include/types.h"

using namespace std;

#define USER_INFO_BUCKET_NAME ".users"

static string ui_bucket = USER_INFO_BUCKET_NAME;


struct S3UserInfo
{
  string user_id;
  string access_key;
  string secret_key;
  string display_name;

  void encode(bufferlist& bl) const {
     ::encode(user_id, bl);
     ::encode(access_key, bl);
     ::encode(secret_key, bl);
     ::encode(display_name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(user_id, bl);
    ::decode(access_key, bl);
    ::decode(secret_key, bl);
    ::decode(display_name, bl);
  }
 
};
WRITE_CLASS_ENCODER(S3UserInfo)

int s3_get_user_info(string user_id, S3UserInfo& info)
{
  bufferlist bl;
  int ret;
  char *data;
  struct s3_err err;

  ret = get_obj(ui_bucket, user_id, &data, 0, -1, NULL, NULL, NULL, NULL, true, &err);
  if (ret < 0) {
    return ret;
  }
  bl.append(data, ret);
  bufferlist::iterator iter = bl.begin();
  info.decode(iter); 
  free(data);
  return 0;
}

int s3_store_user_info(S3UserInfo& info)
{
  bufferlist bl;
  info.encode(bl);
  const char *data = bl.c_str();
  string md5;
  int ret;

  ret = put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), md5);

  return ret;
}
