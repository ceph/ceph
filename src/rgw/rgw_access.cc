#include <string.h>
#include "rgw_access.h"
#include "rgw_fs.h"
#include "rgw_rados.h"
#include "rgw_cache.h"

#define DOUT_SUBSYS rgw

static RGWCache<RGWFS> cached_fs_provider;
static RGWCache<RGWRados> cached_rados_provider;
static RGWFS fs_provider;
static RGWRados rados_provider;

void RGWObjManifestPart::generate_test_instances(std::list<RGWObjManifestPart*>& o)
{
  o.push_back(new RGWObjManifestPart);

  RGWObjManifestPart *p = new RGWObjManifestPart;
  rgw_bucket b("bucket", ".pool", "marker_", 12);
  p->loc = rgw_obj(b, "object");
  p->loc_ofs = 512 * 1024;
  p->size = 128 * 1024;
  o.push_back(p);
}

void RGWObjManifestPart::dump(Formatter *f) const
{
  f->open_object_section("loc");
  loc.dump(f);
  f->close_section();
  f->dump_unsigned("loc_ofs", loc_ofs);
  f->dump_unsigned("size", size);
}

void RGWObjManifest::generate_test_instances(std::list<RGWObjManifest*>& o)
{
  RGWObjManifest *m = new RGWObjManifest;
  for (int i = 0; i<10; i++) {
    RGWObjManifestPart p;
    rgw_bucket b("bucket", ".pool", "marker_", 12);
    p.loc = rgw_obj(b, "object");
    p.loc_ofs = 0;
    p.size = 512 * 1024;
    m->objs[(uint64_t)i * 512 * 1024] = p;
  }
  m->obj_size = 5 * 1024 * 1024;

  o.push_back(new RGWObjManifest);
}

void RGWObjManifest::dump(Formatter *f) const
{
  map<uint64_t, RGWObjManifestPart>::const_iterator iter = objs.begin();
  f->open_array_section("objs");
  for (; iter != objs.end(); ++iter) {
    f->dump_unsigned("ofs", iter->first);
    f->open_object_section("part");
    iter->second.dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_unsigned("obj_size", obj_size);
}

RGWAccess* RGWAccess::store;

RGWAccess::~RGWAccess()
{
}

RGWAccess *RGWAccess::init_storage_provider(const char *type, CephContext *cct)
{
  int use_cache = cct->_conf->rgw_cache_enabled;
  store = NULL;
  if (!use_cache) {
    if (strcmp(type, "rados") == 0) {
      store = &rados_provider;
    } else if (strcmp(type, "fs") == 0) {
      store = &fs_provider;
    }
  } else {
    if (strcmp(type, "rados") == 0) {
      store = &cached_rados_provider;
    } else if (strcmp(type, "fs") == 0) {
      store = &cached_fs_provider;
    }
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
