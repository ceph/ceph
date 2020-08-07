
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
  encode_json("key", key, f);
  encode_json("shard_id", shard_id, f);
  encode_json("num_shards", num_shards, f);
  encode_json("timestamp", timestamp, f);
}

void siprovider_data_info::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("shard_id", shard_id, obj);
  JSONDecoder::decode_json("num_shards", num_shards, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
}

int siprovider_data_create_entry(CephContext *cct,
                                 RGWBucketCtl *bucket_ctl,
                                 const string& key,
                                 std::optional<ceph::real_time> timestamp,
                                 const std::string& m,
                                 SIProvider::Entry *result)
{
  rgw_bucket bucket;
  int shard_id;
  rgw_bucket_parse_bucket_key(cct, key, &bucket, &shard_id);

  RGWBucketInfo info;

  int ret = bucket_ctl->read_bucket_instance_info(bucket,
                                                  &info,
                                                  null_yield);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): cannot read bucket instance info for bucket=" << bucket << dendl;
    return ret;
  }

  siprovider_data_info data_info = { key,
                                     shard_id,
                                     (int)info.layout.current_index.layout.normal.num_shards,
                                     timestamp };
  result->key = m;
  data_info.encode(result->data);
  
  return 0;
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
        SIProvider::Entry e;
        ret = siprovider_data_create_entry(cct, ctl.bucket, k.key, std::nullopt,
                                           rgw::to_base64(k.marker), &e);
        if (ret < 0) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): skipping entry,siprovider_data_create_entry() returned error: key=" << k.key << " ret=" << ret << dendl;
          continue;
        }

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
				       RGWDataChangesLog *_datalog_svc,
                                       RGWBucketCtl *_bucket_ctl) : SIProvider_SingleStage(_cct,
                                                                                           "data.inc",
                                                                                           std::nullopt,
                                                                                           std::make_shared<SITypeHandlerProvider_Default<siprovider_data_info> >(),
                                                                                           SIProvider::StageType::INC,
                                                                                           _cct->_conf->rgw_data_log_num_shards,
                                                                                           false) {
  svc.datalog = _datalog_svc;
  ctl.bucket = _bucket_ctl;
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

  auto m = rgw::from_base64(marker);

  utime_t start_time;
  utime_t end_time;

  bool truncated;
  do {
    list<rgw_data_change_log_entry> entries;
    int ret = data_log->list_entries(shard_id, start_time.to_real_time(), end_time.to_real_time(),
                                     max, entries, m, &m, &truncated);
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
      SIProvider::Entry e;
      ret = siprovider_data_create_entry(cct, ctl.bucket, entry.entry.key, entry.entry.timestamp,
                                         rgw::to_base64(entry.log_id), &e);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: " << __func__ << "(): skipping entry,siprovider_data_create_entry() returned error: key=" << entry.entry.key << " ret=" << ret << dendl;
        continue;
      }

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
  RGWDataChangesLogInfo info;
  int ret = data_log->get_info(shard_id, &info);
  if (ret == -ENOENT) {
    ret = 0;
  }
  if (ret < 0) {
    lderr(cct) << "ERROR: data_log->get_info() returned ret=" << ret << dendl;
    return ret;
  }
  *marker = rgw::to_base64(info.marker);
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
