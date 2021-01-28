#include "rgw_sync_info.h"

#include "common/dout.h"
#include "common/ceph_json.h"


#define dout_subsys ceph_subsys_rgw

string SIProvider::stage_type_to_str(SIProvider::StageType st)
{
  switch  (st) {
    case SIProvider::StageType::FULL:
      return "full";
    case SIProvider::StageType::INC:
      return "inc";
    default:
      return "unknown";
  }
}

void rgw_sip_pos::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  encode_json("timestamp", timestamp, f);
}

void rgw_sip_pos::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
}

SIProvider::StageType SIProvider::stage_type_from_str(const string& s)
{
  if (s == "full") {
    return SIProvider::StageType::FULL;
  }
  if (s == "inc") {
    return SIProvider::StageType::INC;
  }

  return SIProvider::StageType::UNKNOWN;
}


void SIProvider::StageInfo::dump(Formatter *f) const
{
  encode_json("sid", sid, f);
  encode_json("next_sid", next_sid, f);
  encode_json("type", SIProvider::stage_type_to_str(type), f);
  encode_json("num_shards", num_shards, f);
  encode_json("disabled", disabled, f);
}

void SIProvider::StageInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("sid", sid, obj);
  JSONDecoder::decode_json("next_sid", next_sid, obj);
  string type_str;
  JSONDecoder::decode_json("type", type_str, obj);
  type = SIProvider::stage_type_from_str(type_str);
  JSONDecoder::decode_json("num_shards", num_shards, obj);
  JSONDecoder::decode_json("disabled", disabled, obj);
}

void SIProvider::Info::dump(Formatter *f) const
{
  encode_json("name", name, f);
  encode_json("data_type", data_type, f);
  encode_json("first_stage", first_stage, f);
  encode_json("last_stage", last_stage, f);
  encode_json("stages", stages, f);
}

void SIProvider::Info::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("data_type", data_type, obj);
  JSONDecoder::decode_json("first_stage", first_stage, obj);
  JSONDecoder::decode_json("last_stage", last_stage, obj);
  JSONDecoder::decode_json("stages", stages, obj);
}

SIProvider::Info SIProviderCommon::get_info(const DoutPrefixProvider *dpp)
{
  vector<SIProvider::StageInfo> stages;

  for (auto& sid : get_stages(dpp)) {
    SIProvider::StageInfo si;
    int r = get_stage_info(dpp, sid, &si);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to retrieve stage info for sip=" << get_name() << ", sid=" << sid << ": r=" << r << dendl;
      /* continuing */
    }
    stages.push_back(si);
  }

  return { get_name(),
           get_data_type(),
           get_first_stage(dpp),
           get_last_stage(dpp),
           stages };
}

int SIProvider_SingleStage::fetch(const DoutPrefixProvider *dpp,
                                  const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_fetch(dpp, shard_id, marker, max, result);
}

int SIProvider_SingleStage::get_start_marker(const DoutPrefixProvider *dpp,
                                             const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_get_start_marker(dpp, shard_id, marker, timestamp);
}

int SIProvider_SingleStage::get_cur_state(const DoutPrefixProvider *dpp,
                                          const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp,
                                          bool *disabled, optional_yield y)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_get_cur_state(dpp, shard_id, marker, timestamp, disabled, y);
}

int SIProvider_SingleStage::trim(const DoutPrefixProvider *dpp,
                                 const stage_id_t& sid, int shard_id, const std::string& marker)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_trim(dpp, shard_id, marker);
}

SIProvider_Container::SIProvider_Container(CephContext *_cct,
                                           const std::string& _name,
                                           std::optional<std::string> _instance,
                                           std::vector<SIProviderRef> _providers) : SIProviderCommon(_cct, _name, _instance),
                                                                                    type_provider(this),
                                                                                    providers(_providers)
{
  std::map<std::string, int> pcount;
  int i = 0;

  for (auto& p : providers) {
    const auto& name = p->get_name();

    int count = pcount[name]++;

    char buf[name.size() + 32 ];
    snprintf(buf, sizeof(buf), "%s/%d", name.c_str(), count);

    providers_index[buf] = i++;
    pids.push_back(buf);
  }
}

bool SIProvider_Container::decode_sid(const stage_id_t& sid,
                                      SIProviderRef *provider,
                                      SIProvider::stage_id_t *provider_sid,
                                      int *index) const
{
  auto pos = sid.find(':');
  if (pos == std::string::npos) {
    return false;
  }

  auto pid = sid.substr(0, pos);
  auto piter = providers_index.find(pid);
  if (piter == providers_index.end()) {
    return false;
  }

  if (index) {
    *index = piter->second;
  }

  *provider = providers[piter->second];
  *provider_sid = sid.substr(pos + 1);

  return true;
}

SIProvider::stage_id_t SIProvider_Container::encode_sid(const std::string& pid,
                                                        const SIProvider::stage_id_t& provider_sid) const
{
  return pid + ":" + provider_sid;
}

SIProvider::stage_id_t SIProvider_Container::get_first_stage(const DoutPrefixProvider *dpp)
{
  if (pids.empty()) {
    return stage_id_t();
  }
  return encode_sid(pids[0], providers[0]->get_first_stage(dpp));
}

SIProvider::stage_id_t SIProvider_Container::get_last_stage(const DoutPrefixProvider *dpp)
{
  if (pids.empty()) {
    return stage_id_t();
  }
  auto i = pids.size() - 1;
  return encode_sid(pids[i], providers[i]->get_last_stage(dpp));
}

int SIProvider_Container::get_next_stage(const DoutPrefixProvider *dpp,
                                         const stage_id_t& sid, stage_id_t *next_sid)
{
  if (pids.empty()) {
    ldpp_dout(dpp, 20) << "NOTICE: " << __func__ << "() called by pids is empty" << dendl;
    return -ENOENT;
  }

  SIProviderRef provider;
  stage_id_t psid;
  int i;

  if (!decode_sid(sid,  &provider, &psid, &i)) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "() psid=" << psid << " failed to decode sid (sid=" << sid << ")" << dendl;
    return -EINVAL;
  }

  SIProvider::stage_id_t next_psid;
  int r = provider->get_next_stage(dpp, psid, &next_psid);
  if (r < 0) {
    if (r != -ENOENT) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "() psid=" << psid << " returned r=" << r << dendl;
      return r;
    }
    if (++i == (int)providers.size()) {
      ldpp_dout(dpp, 20) << __func__ << "() reached the last provider" << dendl;
      return -ENOENT;
    }
    provider = providers[i];
    next_psid = provider->get_first_stage(dpp);
  }

  *next_sid = encode_sid(pids[i], next_psid);
  return 0;
}

std::vector<SIProvider::stage_id_t> SIProvider_Container::get_stages(const DoutPrefixProvider *dpp)
{
  std::vector<stage_id_t> result;

  int i = 0;

  for (auto& provider : providers) {
    auto& pid = pids[i++];

    std::vector<std::string> stages = provider->get_stages(dpp);

    for (auto& psid : stages) {
      result.push_back(encode_sid(pid, psid));
    }
  }

  return result;
}

int SIProvider_Container::get_stage_info(const DoutPrefixProvider *dpp,
                                         const stage_id_t& sid, StageInfo *sinfo)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldpp_dout(dpp, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  int r = provider->get_stage_info(dpp, psid, sinfo);
  if (r < 0) {
    return r;
  }

  sinfo->sid = sid;

  stage_id_t next_stage;
  r = get_next_stage(dpp, sid, &next_stage);
  if (r >= 0) {
    sinfo->next_sid = next_stage;
  } else {
    sinfo->next_sid.reset();
  }

  return 0;
}

int SIProvider_Container::fetch(const DoutPrefixProvider *dpp,
                                const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldpp_dout(dpp, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->fetch(dpp, psid, shard_id, marker, max, result);
}

int SIProvider_Container::get_start_marker(const DoutPrefixProvider *dpp,
                                           const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldpp_dout(dpp, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->get_start_marker(dpp, psid, shard_id, marker, timestamp);
}

int SIProvider_Container::get_cur_state(const DoutPrefixProvider *dpp,
                                        const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp,
                                        bool *disabled, optional_yield y)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldpp_dout(dpp, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->get_cur_state(dpp, psid, shard_id, marker, timestamp, disabled, y);
}

SIProvider::TypeHandler *SIProvider_Container::TypeProvider::get_type_handler()
{
  if (sip->providers.empty()) {
    return nullptr;
  }

  return sip->providers[0]->get_type_handler();
}

int SIProvider_Container::trim(const DoutPrefixProvider *dpp,
                               const stage_id_t& sid, int shard_id, const std::string& marker)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldpp_dout(dpp, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->trim(dpp, psid, shard_id, marker);
}

