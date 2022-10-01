
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

#include "common/ptr_wrapper.h"

#include "svc_meta_be.h"
#include "svc_meta_be_types.h"

class RGWSI_MetaBackend_Handler;

using RGWSI_OTP_BE_Handler = ptr_wrapper<RGWSI_MetaBackend_Handler, RGWSI_META_BE_TYPES::OTP>;
using RGWSI_OTP_BE_Ctx = ptr_wrapper<RGWSI_MetaBackend::Context, RGWSI_META_BE_TYPES::OTP>;

