/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Intel Corporation
 *
 * Author: Qiaowei Ren <qiaowei.ren@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */


#include "crypto/openssl/openssl_crypto_plugin.h"

#include "ceph_ver.h"

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();

  return instance->add(type, name, new OpenSSLCryptoPlugin(cct));
}
