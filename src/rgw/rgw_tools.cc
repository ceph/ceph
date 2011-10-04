#include <errno.h>

#include "common/errno.h"

#include "include/types.h"

#include "rgw_common.h"
#include "rgw_access.h"
#include "rgw_tools.h"
#include "rgw_bucket.h"

#define READ_CHUNK_LEN (16 * 1024)

int rgw_put_obj(string& uid, rgw_bucket& bucket, string& oid, const char *data, size_t size)
{
  map<string,bufferlist> attrs;

  rgw_obj obj(bucket, oid);

  int ret = rgwstore->put_obj(NULL, uid, obj, data, size, NULL, attrs);

  if (ret == -ENOENT) {
    ret = rgwstore->create_bucket(uid, bucket, attrs, true); //all callers are using system buckets
    if (ret >= 0)
      ret = rgwstore->put_obj(NULL, uid, obj, data, size, NULL, attrs);
  }

  return ret;
}

int rgw_get_obj(rgw_bucket& bucket, string& key, bufferlist& bl)
{
  int ret;
  char *data = NULL;
  struct rgw_err err;
  void *handle = NULL;
  bufferlist::iterator iter;
  int request_len = READ_CHUNK_LEN;
  rgw_obj obj(bucket, key);
  ret = rgwstore->prepare_get_obj(NULL, obj, 0, NULL, NULL, NULL,
                                  NULL, NULL, NULL, NULL, NULL, NULL, &handle, &err);
  if (ret < 0)
    return ret;

  do {
    ret = rgwstore->get_obj(NULL, &handle, obj, &data, 0, request_len - 1);
    if (ret < 0)
      goto done;
    if (ret < request_len)
      break;
    free(data);
    request_len *= 2;
  } while (true);

  bl.append(data, ret);
  free(data);

  ret = 0;
done:
  rgwstore->finish_get_obj(&handle);
  return ret;
}


