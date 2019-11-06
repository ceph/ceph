// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "svc_meta_be.h"

#include "rgw/rgw_service.h"


class RGWMetadataLog;
class RGWCoroutine;


class RGWSI_Meta : public RGWServiceInstance
{
  RGWSI_SysObj *sysobj_svc{nullptr};
  RGWSI_MDLog *mdlog_svc{nullptr};

  map<RGWSI_MetaBackend::Type, RGWSI_MetaBackend *> be_svc;

  vector<unique_ptr<RGWSI_MetaBackend_Handler> > be_handlers;

public:
  RGWSI_Meta(CephContext *cct);
  ~RGWSI_Meta();

  void init(RGWSI_SysObj *_sysobj_svc,
            RGWSI_MDLog *_mdlog_svc,
            vector<RGWSI_MetaBackend *>& _be_svc);

  int create_be_handler(RGWSI_MetaBackend::Type be_type,
                        RGWSI_MetaBackend_Handler **phandler);
};

