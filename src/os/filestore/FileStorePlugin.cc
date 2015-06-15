// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "common/debug.h"
#include "os/ObjectStore.h"
#include "os/filestore/FileStore.h"
#include "common/PluginRegistry.h"
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "FileStorePlugin: ";
}


class FileStorePlugin : public Plugin {
public:
  FileStorePlugin(CephContext *cct):Plugin(cct) { }
  virtual ObjectStore* factory(CephContext *cct, const string& type,const string& data, const string& journal, osflagbits_t flags)
  {
	dout(10) << __func__ << dendl;	  
    FileStore *fs = NULL;
    fs = new FileStore(data, journal, flags);
    assert(fs);
    return fs;
  }
};

// -----------------------------------------------------------------------------

const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

int __ceph_plugin_init(CephContext * cct, char *plugin_name, char *directory)
{
  PluginRegistry *reg = cct->get_plugin_registry();
  reg->add("ObjectStore", "FileStore", new FileStorePlugin(cct));
  return 0;
}
