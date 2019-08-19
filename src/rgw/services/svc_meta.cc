// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp


#include "svc_meta.h"

#include "rgw/rgw_metadata.h"

#define dout_subsys ceph_subsys_rgw


RGWSI_Meta::RGWSI_Meta(CephContext *cct) : RGWServiceInstance(cct) {
}

RGWSI_Meta::~RGWSI_Meta() {}

void RGWSI_Meta::init(RGWSI_SysObj *_sysobj_svc,
                      RGWSI_MDLog *_mdlog_svc,
                      vector<RGWSI_MetaBackend *>& _be_svc)
{
  sysobj_svc = _sysobj_svc;
  mdlog_svc = _mdlog_svc;

  for (auto& be : _be_svc) {
    be_svc[be->get_type()] = be;
  }
}

int RGWSI_Meta::create_be_handler(RGWSI_MetaBackend::Type be_type,
                                  RGWSI_MetaBackend_Handler **phandler)
{
  auto iter = be_svc.find(be_type);
  if (iter == be_svc.end()) {
    ldout(cct, 0) << __func__ << "(): ERROR: backend type not found" << dendl;
    return -EINVAL;
  }

  auto handler = iter->second->alloc_be_handler();

  be_handlers.emplace_back(handler);
  *phandler = handler;

  return 0;
}

