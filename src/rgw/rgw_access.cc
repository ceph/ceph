#include <string.h>
#include "rgw_access.h"
#include "rgw_fs.h"
#include "rgw_rados.h"
#include "rgw_cache.h"

#if 1
static RGWCache<RGWFS> fs_provider;
static RGWCache<RGWRados> rados_provider;
#else
static RGWFS fs_provider;
static RGWRados rados_provider;
#endif

RGWAccess* RGWAccess::store;

RGWAccess::~RGWAccess()
{
}

RGWAccess *RGWAccess::init_storage_provider(const char *type, CephContext *cct)
{
  if (strcmp(type, "rados") == 0) {
    store = &rados_provider;
  } else if (strcmp(type, "fs") == 0) {
    store = &fs_provider;
  } else {
    store = NULL;
  }

  if (store->initialize(cct) < 0)
    store = NULL;

  return store;
}

void RGWAccess::close_storage()
{
  if (!store)
    return;

  store->finalize();
  store = NULL;
}
