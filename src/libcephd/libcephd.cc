#include "acconfig.h"
#include "common/version.h"
#include "common/PluginRegistry.h"
#include "compressor/snappy/CompressionPluginSnappy.h"
#include "compressor/zlib/CompressionPluginZlib.h"
#include "erasure-code/ErasureCodePlugin.h"
#if __x86_64__ && defined(HAVE_BETTER_YASM_ELF64)
#include "erasure-code/isa/ErasureCodePluginIsa.h"
#endif
#include "erasure-code/jerasure/ErasureCodePluginJerasure.h"
#include "erasure-code/jerasure/jerasure_init.h"
#include "erasure-code/lrc/ErasureCodePluginLrc.h"
#include "erasure-code/shec/ErasureCodePluginShec.h"
#include "include/cephd/libcephd.h"

extern "C" void cephd_version(int *pmajor, int *pminor, int *ppatch)
{
  if (pmajor)
    *pmajor = LIBCEPHD_VER_MAJOR;
  if (pminor)
    *pminor = LIBCEPHD_VER_MINOR;
  if (ppatch)
    *ppatch = LIBCEPHD_VER_PATCH;
}

extern "C" const char *ceph_version(int *pmajor, int *pminor, int *ppatch)
{
  int major, minor, patch;
  const char *v = ceph_version_to_str();

  int n = sscanf(v, "%d.%d.%d", &major, &minor, &patch);
  if (pmajor)
    *pmajor = (n >= 1) ? major : 0;
  if (pminor)
    *pminor = (n >= 2) ? minor : 0;
  if (ppatch)
    *ppatch = (n >= 3) ? patch : 0;
  return v;
}

// load the embedded plugins. This is safe to call multiple
// times in the same process
void cephd_preload_embedded_plugins()
{
  int r;

  // load erasure coding plugins
  {
    ErasureCodePlugin* plugin;
    ErasureCodePluginRegistry& reg = ErasureCodePluginRegistry::instance();
    Mutex::Locker l(reg.lock);
    reg.disable_dlclose = true;

    // initialize jerasure (and gf-complete)
    int w[] = { 4, 8, 16, 32 };
    r = jerasure_init(4, w);
    assert(r == 0);

    plugin = new ErasureCodePluginJerasure();
    r = reg.add("jerasure", plugin);
    if (r == -EEXIST) {
      delete plugin;
    }
    assert(r == 0);

    plugin = new ErasureCodePluginLrc();
    r = reg.add("lrc", plugin);
    if (r == -EEXIST) {
      delete plugin;
    }
    assert(r == 0);

    plugin = new ErasureCodePluginShec();
    r = reg.add("shec", plugin);
    if (r == -EEXIST) {
      delete plugin;
    }
    assert(r == 0);

#if __x86_64__ && defined(HAVE_BETTER_YASM_ELF64)
    plugin = new ErasureCodePluginIsa();
    r = reg.add("isa", plugin);
    if (r == -EEXIST) {
      delete plugin;
    }
    assert(r == 0);
#endif
  }

  // now load the compression plugins
  {
    Plugin *plugin;
    PluginRegistry *reg = g_ceph_context->get_plugin_registry();
    Mutex::Locker l(reg->lock);
    reg->disable_dlclose = true;

    plugin = new CompressionPluginSnappy(g_ceph_context);
    r = reg->add("compressor", "snappy", plugin);
    if (r == -EEXIST) {
      delete plugin;
    }
    assert(r == 0);

    plugin = new CompressionPluginZlib(g_ceph_context);
    r = reg->add("compressor", "zlib", plugin);
    if (r == -EEXIST) {
      delete plugin;
    }
    assert(r == 0);
  }
}
