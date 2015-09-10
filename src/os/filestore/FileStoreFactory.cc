// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "common/debug.h"
#include "os/ObjectStore.h"
#include "common/PluginRegistry.h"
#include "os/filestore/FileStore.h"
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "FileStoreFactory: ";
}


class FileStoreFactory : public ObjectStore::Factory {
public:
  FileStoreFactory(CephContext *cct):ObjectStore::Factory(cct) { }
  ObjectStore* factory(CephContext *cct, const string& type,const string& data, const string& journal, osflagbits_t flags)
  {
	dout(10) << __func__ << dendl;	  
    FileStore *fs = NULL;
    fs = new FileStore(data, journal, flags);
    assert(fs);
    return fs;
  }
};

// -----------------------------------------------------------------------------

extern "C" const char *__ceph_plugin_version()
{
  return CEPH_GIT_NICE_VER;
}

// -----------------------------------------------------------------------------

extern "C" int __ceph_plugin_init(CephContext * cct, const std::string& plugin_name, const std::string& directory)
{
  dout(10) << __func__ << dendl;
  PluginRegistry *reg = cct->get_plugin_registry();
  reg->add("objectstore", "filestore", new FileStoreFactory(cct));
  return 0;
}
