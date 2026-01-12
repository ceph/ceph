/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 XSKY Inc.
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 */

#include "acconfig.h"
#include "ceph_ver.h"
#include "common/ceph_context.h"
#include "CompressionPluginMyLZ4.h"

// -----------------------------------------------------------------------------

// 【关键修正】必须加上 extern "C"，否则 Ceph 找不到插件入口！
extern "C" {

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  auto instance = cct->get_plugin_registry();

  return instance->add(type, name, new CompressionPluginMyLZ4(cct));
}

} // extern "C" 的结束大括号