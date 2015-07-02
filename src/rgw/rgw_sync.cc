#include "common/ceph_json.h"

#include "rgw_common.h"
#include "rgw_rados.h"


#define dout_subsys ceph_subsys_rgw

struct mdlog_info {
  uint32_t num_shards;

  mdlog_info() : num_shards(0) {}

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("num_objects", num_shards, obj);
  }
};

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


class RGWRemoteMetaLog {

  RGWRados *store;

  mdlog_info log_info;

  vector<string> markers;

public:
  RGWRemoteMetaLog(RGWRados *_store) : store(_store) {}
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

  ldout(store->ctx(), 20) << __FILE__ << ": read_data=" << string(bl.c_str(), bl.length()) << dendl;

  ret = parse_decode_json(log_info, bl);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to parse mdlog_info, ret=" << ret << dendl;
    return ret;
  }

  ldout(store->ctx(), 20) << "remote mdlog, num_shards=" << log_info.num_shards << dendl;

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



