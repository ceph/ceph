#include "acconfig.h"
#include "ceph_ver.h"
#include "common/ceph_context.h"
#include "ceph_ver.h"
#include "compressor/CompressionPlugin.h"
#include "HracCompressor.h"

class CompressionPluginHrac : public ceph::CompressionPlugin {
public:
  explicit CompressionPluginHrac(CephContext* cct) : CompressionPlugin(cct) {}

  int factory(CompressorRef *cs, std::ostream *ss) override {
    if (compressor == 0) {
      HracCompressor *interface = new HracCompressor(cct);
      compressor = CompressorRef(interface);
    }
    *cs = compressor;
    return 0;
  }
};

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
  return instance->add(type, name, new CompressionPluginHrac(cct));
}

}