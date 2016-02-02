// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#include "Compressor.h"
#include "CompressionPlugin.h"


CompressorRef Compressor::create(CephContext *cct, const string &type)
{
  CompressorRef cs_impl = NULL;
  stringstream ss;
  PluginRegistry *reg = cct->get_plugin_registry();
  CompressionPlugin *factory = dynamic_cast<CompressionPlugin*>(reg->get_with_load("compressor", type));
  if (factory == NULL) {
    lderr(cct) << __func__ << " cannot load compressor of type " << type << dendl;
    return NULL;
  }
  int err = factory->factory(&cs_impl, &ss);
  if (err)
    lderr(cct) << __func__ << " factory return error " << err << dendl;
  return cs_impl;
}
