#include <errno.h>

#include <string>
#include <map>

#include "s3access.h"
#include "s3acl.h"

#include "include/types.h"
#include "s3/user.h"

using namespace std;

static string ui_bucket = USER_INFO_BUCKET_NAME;

int s3_get_user_info(string user_id, S3UserInfo& info)
{
  bufferlist bl;
  int ret;
  char *data;
  struct s3_err err;

  ret = s3store->get_obj(ui_bucket, user_id, &data, 0, -1, NULL, NULL, NULL, NULL, true, &err);
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
  vector<pair<string,bufferlist> > vec;

  ret = s3store->put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), vec);

  if (ret == -ENOENT) {
    std::vector<std::pair<std::string, bufferlist> > attrs;
    ret = s3store->create_bucket(info.user_id /* FIXME currently means nothing */, ui_bucket, attrs);
    if (ret >= 0)
      ret = s3store->put_obj(info.user_id, ui_bucket, info.user_id, data, bl.length(), vec);
  }

  return ret;
}

int s3_get_user_buckets(string user_id, S3UserBuckets& buckets)
{
  bufferlist bl;
  int ret = s3store->get_attr(ui_bucket, user_id, S3_ATTR_BUCKETS, bl);
  if (ret < 0) {
    return ret;
  }

  bufferlist::iterator iter = bl.begin();
  buckets.decode(iter);

  return 0;
}

int s3_put_user_buckets(string user_id, S3UserBuckets& buckets)
{
  bufferlist bl;
  buckets.encode(bl);
  int ret = s3store->set_attr(ui_bucket, user_id, S3_ATTR_BUCKETS, bl);

  return ret;
}
