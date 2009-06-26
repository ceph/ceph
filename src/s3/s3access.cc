#include <string.h>
#include "s3access.h"
#include "s3fs.h"

static S3FS fs_provider;

S3Access* S3Access::store;

S3Access *S3Access::init_storage_provider(const char *type)
{
  if (strcmp(type, "fs") == 0) {
    store = &fs_provider;
  } else {
    store = NULL;
  }

  return store;
}
