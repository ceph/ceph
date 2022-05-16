// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/intrusive_ptr.hpp>
#include "common/ceph_json.h"
#include "common/errno.h"
#include "rgw_metadata.h"
#include "rgw_coroutine.h"
#include "cls/version/cls_version_types.h"

#include "rgw_zone.h"
#include "rgw_tools.h"
#include "rgw_mdlog.h"
#include "rgw_sal.h"

#include "rgw_cr_rados.h"

#include "services/svc_zone.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_cls.h"

#include "include/ceph_assert.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

using namespace std;

void encode_json(const char *name, const obj_version& v, Formatter *f)
{
  f->open_object_section(name);
  f->dump_string("tag", v.tag);
  f->dump_unsigned("ver", v.ver);
  f->close_section();
}

void decode_json_obj(obj_version& v, JSONObj *obj)
{
  JSONDecoder::decode_json("tag", v.tag, obj);
  JSONDecoder::decode_json("ver", v.ver, obj);
}

RGWMetadataHandler_GenericMetaBE::Put::Put(RGWMetadataHandler_GenericMetaBE *_handler,
					   RGWSI_MetaBackend_Handler::Op *_op,
					   string& _entry, RGWMetadataObject *_obj,
					   RGWObjVersionTracker& _objv_tracker,
					   optional_yield _y,
					   RGWMDLogSyncType _type, bool _from_remote_zone):
  handler(_handler), op(_op),
  entry(_entry), obj(_obj),
  objv_tracker(_objv_tracker),
  apply_type(_type),
  y(_y),
  from_remote_zone(_from_remote_zone)
{
}

int RGWMetadataHandler_GenericMetaBE::do_put_operate(Put *put_op, const DoutPrefixProvider *dpp)
{
  int r = put_op->put_pre(dpp);
  if (r != 0) { /* r can also be STATUS_NO_APPLY */
    return r;
  }

  r = put_op->put(dpp);
  if (r != 0) {
    return r;
  }

  r = put_op->put_post(dpp);
  if (r != 0) {  /* e.g., -error or STATUS_APPLIED */
    return r;
  }

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::get(string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_get(op, entry, obj, y, dpp);
  });
}

int RGWMetadataHandler_GenericMetaBE::put(string& entry, RGWMetadataObject *obj, RGWObjVersionTracker& objv_tracker,
                                          optional_yield y, const DoutPrefixProvider *dpp, RGWMDLogSyncType type, bool from_remote_zone)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_put(op, entry, obj, objv_tracker, y, dpp, type, from_remote_zone);
  });
}

int RGWMetadataHandler_GenericMetaBE::remove(string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return do_remove(op, entry, objv_tracker, y, dpp);
  });
}

int RGWMetadataHandler_GenericMetaBE::mutate(const string& entry,
                                             const ceph::real_time& mtime,
                                             RGWObjVersionTracker *objv_tracker,
                                             optional_yield y,
                                             const DoutPrefixProvider *dpp,
                                             RGWMDLogStatus op_type,
                                             std::function<int()> f)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    RGWSI_MetaBackend::MutateParams params(mtime, op_type);
    return op->mutate(entry,
                      params,
                      objv_tracker,
		      y,
                      f,
                      dpp);
  });
}

int RGWMetadataHandler_GenericMetaBE::get_shard_id(const string& entry, int *shard_id)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
    return op->get_shard_id(entry, shard_id);
  });
}

int RGWMetadataHandler_GenericMetaBE::list_keys_init(const DoutPrefixProvider *dpp, const string& marker, void **phandle)
{
  auto op = std::make_unique<RGWSI_MetaBackend_Handler::Op_ManagedCtx>(be_handler);

  int ret = op->list_init(dpp, marker);
  if (ret < 0) {
    return ret;
  }

  *phandle = (void *)op.release();

  return 0;
}

int RGWMetadataHandler_GenericMetaBE::list_keys_next(const DoutPrefixProvider *dpp, void *handle, int max, list<string>& keys, bool *truncated)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);

  int ret = op->list_next(dpp, max, &keys, truncated);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  if (ret == -ENOENT) {
    if (truncated) {
      *truncated = false;
    }
    return 0;
  }

  return 0;
}

void RGWMetadataHandler_GenericMetaBE::list_keys_complete(void *handle)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);
  delete op;
}

string RGWMetadataHandler_GenericMetaBE::get_marker(void *handle)
{
  auto op = static_cast<RGWSI_MetaBackend_Handler::Op_ManagedCtx *>(handle);
  string marker;
  int r = op->list_get_marker(&marker);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): list_get_marker() returned: r=" << r << dendl;
    /* not much else to do */
  }

  return marker;
}

int RGWMetadataHandlerPut_SObj::put_pre(const DoutPrefixProvider *dpp)
{
  int ret = get(&old_obj, dpp);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  exists = (ret != -ENOENT);

  oo.reset(old_obj);

  auto old_ver = (!old_obj ? obj_version() : old_obj->get_version());
  auto old_mtime = (!old_obj ? ceph::real_time() : old_obj->get_mtime());

  // are we actually going to perform this put, or is it too old?
  if (!handler->check_versions(exists, old_ver, old_mtime,
                               objv_tracker.write_version, obj->get_mtime(),
                               apply_type)) {
    return STATUS_NO_APPLY;
  }

  objv_tracker.read_version = old_ver; /* maintain the obj version we just read */

  return 0;
}

int RGWMetadataHandlerPut_SObj::put(const DoutPrefixProvider *dpp)
{
  int ret = put_check(dpp);
  if (ret != 0) {
    return ret;
  }

  return put_checked(dpp);
}

int RGWMetadataHandlerPut_SObj::put_checked(const DoutPrefixProvider *dpp)
{
  RGWSI_MBSObj_PutParams params(obj->get_pattrs(), obj->get_mtime());

  encode_obj(&params.bl);

  int ret = op->put(entry, params, &objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

RGWMetadataHandlerPut_SObj::~RGWMetadataHandlerPut_SObj() {}

int RGWMetadataHandler::attach(RGWMetadataManager *manager)
{
  return manager->register_handler(this);
}

RGWMetadataHandler::~RGWMetadataHandler() {}

obj_version& RGWMetadataObject::get_version()
{
  return objv;
}

int RGWMetadataManager::register_handler(RGWMetadataHandler *handler)
{
  string type = handler->get_type();

  if (handlers.find(type) != handlers.end())
    return -EEXIST;

  handlers[type] = handler;

  return 0;
}

