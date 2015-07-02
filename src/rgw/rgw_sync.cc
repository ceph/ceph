#include "common/ceph_json.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_sync.h"


#define dout_subsys ceph_subsys_rgw

void rgw_mdlog_info::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("num_objects", num_shards, obj);
}

template <class T>
static int parse_decode_json(T& t, bufferlist& bl)
{
  JSONParser p;
  int ret = p.parse(bl.c_str(), bl.length());
  if (ret < 0) {
    cout << "failed to parse JSON" << std::endl;
    return ret;
  }

  try {
    decode_json_obj(t, &p);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    return -EINVAL;
  }
  return 0;
}


int RGWRemoteMetaLog::init()
{
  conn = store->rest_master_conn;

  list<pair<string, string> > params;
  params.push_back(make_pair("type", "metadata"));

  int ret = conn->get_json_resource("/admin/log", &params, log_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed to fetch mdlog info" << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, num_shards=" << log_info.num_shards << dendl;

  return 0;
}


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



