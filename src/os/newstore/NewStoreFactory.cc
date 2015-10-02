// -----------------------------------------------------------------------------
#include "ceph_ver.h"
#include "common/debug.h"
#include "os/ObjectStore.h"
#include "common/PluginRegistry.h"
#include "os/newstore/NewStore.h"
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_newstore
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "NewStoreFactory: ";
}


class NewStoreFactory : public ObjectStore::Factory {
public:
  NewStoreFactory(CephContext *cct):ObjectStore::Factory(cct) { }
  ObjectStore* factory(CephContext *cct, const string& type,const string& data, const string& journal, osflagbits_t flags)
  {
	dout(10) << __func__ << dendl;	  
    NewStore *fs = NULL;
    fs = new NewStore(cct, data);
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
  reg->add("objectstore", "newstore", new NewStoreFactory(cct));
  return 0;
}
