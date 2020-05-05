#include "rgw_sync_info.h"

#include "common/dout.h"


#define dout_subsys ceph_subsys_rgw

int SIProvider_SingleStage::fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_fetch(shard_id, marker, max, result);
}

int SIProvider_SingleStage::get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker) const
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_get_start_marker(shard_id, marker);
}

int SIProvider_SingleStage::get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker) const
{
  if (sid != stage_info.sid) {
    return -ERANGE;
  }
  return do_get_cur_state(shard_id, marker);
}

SIProvider_Container::SIProvider_Container(CephContext *_cct,
                                           const std::string& _name,
                                           std::vector<SIProviderRef>& _providers) : SIProvider(_cct, _name),
                                                                                     providers(_providers)
{
  std::map<std::string, int> pcount;
  int i = 0;

  for (auto& p : providers) {
    int count = pcount[p->name]++;

    char buf[p->name.size() + 32 ];
    snprintf(buf, sizeof(buf), "%s/%d", p->name.c_str(), count);

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

SIProvider::stage_id_t SIProvider_Container::get_first_stage() const
{
  if (pids.empty()) {
    return stage_id_t();
  }
  return encode_sid(pids[0], providers[0]->get_first_stage());
}

SIProvider::stage_id_t SIProvider_Container::get_last_stage() const
{
  if (pids.empty()) {
    return stage_id_t();
  }
  auto i = pids.size() - 1;
  return encode_sid(pids[i], providers[i]->get_last_stage());
}

int SIProvider_Container::get_next_stage(const stage_id_t& sid, stage_id_t *next_sid)
{
  if (pids.empty()) {
    ldout(cct, 20) << "NOTICE: " << __func__ << "() called by pids is empty" << dendl;
    return -ENOENT;
  }

  SIProviderRef provider;
  stage_id_t psid;
  int i;

  if (!decode_sid(sid,  &provider, &psid, &i)) {
    ldout(cct, 0) << "ERROR: " << __func__ << "() psid=" << psid << " failed to decode sid (sid=" << sid << ")" << dendl;
    return -EINVAL;
  }

  SIProvider::stage_id_t next_psid;
  int r = provider->get_next_stage(psid, &next_psid);
  if (r < 0) {
    if (r != -ENOENT) {
      ldout(cct, 0) << "ERROR: " << __func__ << "() psid=" << psid << " returned r=" << r << dendl;
      return r;
    }
    if (++i == (int)providers.size()) {
      ldout(cct, 20) << __func__ << "() reached the last provider" << dendl;
      return -ENOENT;
    }
    provider = providers[i];
    next_psid = provider->get_first_stage();
  }

  *next_sid = encode_sid(pids[i], next_psid);
  return 0;
}

std::vector<SIProvider::stage_id_t> SIProvider_Container::get_stages()
{
  std::vector<stage_id_t> result;

  int i = 0;

  for (auto& provider : providers) {
    auto& pid = pids[i++];

    std::vector<std::string> stages = provider->get_stages();

    for (auto& psid : stages) {
      result.push_back(encode_sid(pid, psid));
    }
  }

  return result;
}

int SIProvider_Container::get_stage_info(const stage_id_t& sid, StageInfo *sinfo) const
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  int r = provider->get_stage_info(psid, sinfo);
  if (r < 0) {
    return r;
  }

  sinfo->sid = sid;

  return 0;
}

int SIProvider_Container::fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result)
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->fetch(psid, shard_id, marker, max, result);
}

int SIProvider_Container::get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker) const
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->get_start_marker(psid, shard_id, marker);
}

int SIProvider_Container::get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker) const
{
  SIProviderRef provider;
  stage_id_t psid;

  if (!decode_sid(sid,  &provider, &psid)) {
    ldout(cct, 20) << __func__ << "() can't decode sid: " << dendl;
    return -ENOENT;
  }

  return provider->get_cur_state(psid, shard_id, marker);
}

int SIProviderClient::init_markers()
{
  auto stages = provider->get_stages();

  if (stages.empty()) {
    return 0;
  }

  SIProvider::StageInfo prev;

  for (auto& sid : stages) {
    SIProvider::StageInfo sinfo;
    int r = provider->get_stage_info(sid, &sinfo);
    if (r < 0) {
      return r;
    }
    bool all_history = (prev.type != SIProvider::StageType::FULL ||
                        sinfo.type != SIProvider::StageType::INC);
    auto& stage_markers = initial_stage_markers[sinfo.sid];
    stage_markers.reserve(sinfo.num_shards);
    for (int i = 0; i < sinfo.num_shards; ++i) {
      std::string marker;
      int r = (!all_history ? provider->get_cur_state(sid, i, &marker) : 
                              provider->get_start_marker(sid, i, &marker));
      if (r < 0) {
        return r;
      }
      stage_markers.push_back(marker);
    }
  }

  init_stage(provider->get_first_stage());

  return 0;
}

int SIProviderClient::init_stage(const stage_id_t& new_sid)
{
  int r = provider->get_stage_info(new_sid, &stage_info);
  if (r < 0) {
    return r;
  }

  auto iter = initial_stage_markers.find(stage_info.sid);
  if (iter != initial_stage_markers.end()) {
    markers = std::move(iter->second);
    initial_stage_markers.erase(iter);
  } else {
    markers.resize(stage_info.num_shards);
    markers.clear();
  }

  done.resize(stage_info.num_shards);
  done.clear();

  num_complete = 0;
  return 0;
}


int SIProviderClient::fetch(int shard_id, int max, SIProvider::fetch_result *result) {
  if (shard_id > stage_info.num_shards) {
    return -ERANGE;
  }

  int r = provider->fetch(stage_info.sid, shard_id, markers[shard_id], max, result);
  if (r < 0) {
    return r;
  }

  if (!result->entries.empty()) {
    markers[shard_id] = result->entries.back().key;
  }

  if (result->done && !done[shard_id]) {
    ++num_complete;
    done[shard_id] = result->done;
  }

  return 0;
}

int SIProviderClient::promote_stage(int *new_num_shards)
{
  stage_id_t next_sid;

  int r = provider->get_next_stage(stage_info.sid, &next_sid);
  if (r < 0) {
    return r;
  }

  r = init_stage(next_sid);
  if (r < 0) {
    return r;
  }

  if (new_num_shards) {
    *new_num_shards = stage_num_shards();
  }

  return 0;
}

