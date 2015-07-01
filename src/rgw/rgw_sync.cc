#include "rgw_common.h"
#include "rgw_rados.h"


#define dout_subsys ceph_subsys_rgw


class RGWRemoteMetaLog {

  RGWRados *store;

  int num_shards;

  vector<string> markers;

public:
  RGWRemoteMetaLog(RGWRados *_store) : store(_store), num_shards(0) {}
  int init();
};

int RGWRemoteMetaLog::init()
{
  list<pair<string, string> > params;
  params.push_back(make_pair("type", "metadata"));

  bufferlist bl;
  int ret = store->rest_master_conn->get_resource("/admin/log", &params, NULL, bl);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to fetch log info from master, ret=" << ret << dendl;
    return ret;
  }

  return 0;
}



class RGWMetadataSync {
  RGWRados *store;

  RGWRemoteMetaLog master_log;
public:
  RGWMetadataSync(RGWRados *_store) : store(_store), master_log(_store) {}

  int init();

};


int RGWMetadataSync::init()
{
  if (store->is_meta_master()) {
    return 0;
  }

  if (!store->rest_master_conn) {
    lderr(store->ctx()) << "no REST connection to master zone" << dendl;
    return -EIO;
  }

  int ret = master_log.init();
  if (ret < 0) {
    return ret;
  }

  return 0;
}



