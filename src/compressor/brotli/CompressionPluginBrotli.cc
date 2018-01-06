#include "acconfig.h"
#include "ceph_ver.h"
#include "CompressionPluginBrotli.h"

#ifndef BUILDING_FOR_EMBEDDED

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

int __ceph_plugin_init(CephContext *cct,
                       const std::string& type,
                       const std::string& name)
{
  PluginRegistry *instance = cct->get_plugin_registry();
  return instance->add(type, name, new CompressionPluginBrotli(cct));
}

#endif // !BUILDING_FOR_EMBEDDED
