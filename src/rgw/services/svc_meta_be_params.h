

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

#include <variant>

struct RGWSysObjectCtx;

struct RGWSI_MetaBackend_CtxParams_SObj {
  RGWSysObjectCtx *sysobj_ctx{nullptr};

  RGWSI_MetaBackend_CtxParams_SObj() {}
  RGWSI_MetaBackend_CtxParams_SObj(RGWSysObjectCtx * _sysobj_ctx) : sysobj_ctx(_sysobj_ctx) {}
};

using RGWSI_MetaBackend_CtxParams = std::variant<RGWSI_MetaBackend_CtxParams_SObj>;
