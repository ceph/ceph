
#include "common/debug.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"

#include "rgw_sip_data.h"
#include "rgw_bucket.h"
#include "rgw_b64.h"
#include "rgw_datalog.h"

#define dout_subsys ceph_subsys_rgw

void siprovider_data_info::dump(Formatter *f) const
{
  encode_json("id", id, f);
  encode_json("timestamp", timestamp, f);
}

void siprovider_data_info::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
}

int SIProvider_DataFull::do_fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  if (shard_id != 0) {
    return -ERANGE;
  }

  string section = "bucket.instance";

  void *handle;

  result->done = false;
  result->more = true;

  auto m = rgw::from_base64(marker);

  int ret = meta.mgr->list_keys_init(section, m, &handle);
  if (ret < 0) {
    lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
    return ret;
  }

  while (max > 0) {
    std::list<RGWMetadataHandler::KeyInfo> entries;
    bool truncated;

    ret = meta.mgr->list_keys_next(handle, max, entries,
                                   &truncated);
    if (ret < 0) {
      lderr(cct) << "ERROR: " << __func__ << "(): list_keys_init() returned ret=" << ret << dendl;
      return ret;
    }

    if (!entries.empty()) {
      max -= entries.size();

      m = entries.back().marker;

      for (auto& k : entries) {
        auto e = siprovider_data_create_entry(k.key, std::nullopt, rgw::to_base64(k.marker));
        result->entries.push_back(e);
      }
    }

    if (!truncated) {
      result->done = true;
      result->more = false;
      break;
    }
  }


  return 0;
}

SIProvider_DataInc::SIProvider_DataInc(CephContext *_cct,
				       RGWSI_DataLog *_datalog_svc) : SIProvider_SingleStage(_cct,
                                                                                             "data.inc",
                                                                                             SIProvider::StageType::INC,
                                                                                             _cct->_conf->rgw_data_log_num_shards) {
  svc.datalog = _datalog_svc;
}

int SIProvider_DataInc::init()
{
  data_log = svc.datalog;
  return 0;
}

int SIProvider_DataInc::do_fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  if (shard_id >= stage_info.num_shards ||
      shard_id < 0) {
    return -ERANGE;
  }

  utime_t start_time;
  utime_t end_time;

  bool truncated;
  do {
    list<rgw_data_change_log_entry> entries;
    int ret = data_log->list_entries(shard_id, start_time.to_real_time(), end_time.to_real_time(),
                                     max, entries, marker, &marker, &truncated);
    if (ret == -ENOENT) {
      truncated = false;
      break;
    }
    if (ret < 0) {
      lderr(cct) << "ERROR: data_log->list_entries() failed: ret=" << ret << dendl;
      return -ret;
    }

    max -= entries.size();

    for (auto& entry : entries) {
      auto e = siprovider_data_create_entry(entry.entry.key, entry.entry.timestamp, rgw::to_base64(entry.log_id));
      result->entries.push_back(e);
    }
  } while (truncated && max > 0);

  result->done = false; /* FIXME */
  result->more = truncated;

  return 0;
}


int SIProvider_DataInc::do_get_start_marker(int shard_id, std::string *marker) const
{
  marker->clear();
  return 0;
}

int SIProvider_DataInc::do_get_cur_state(int shard_id, std::string *marker) const
{
#warning FIXME
  return 0;
}

int SIProvider_DataInc::do_trim(int shard_id, const std::string& marker)
{
  utime_t start_time, end_time;
  int ret;
  // trim until -ENODATA
  do {
    ret = data_log->trim_entries(shard_id, marker);
  } while (ret == 0);
  if (ret < 0 && ret != -ENODATA) {
    ldout(cct, 20) << "ERROR: data_log->trim(): returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}
