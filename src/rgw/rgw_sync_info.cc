#include "rgw_sync_info.h"


int SIProvider_SingleStage::fetch(int snum, int shard_id, std::string marker, int max, fetch_result *result)
{
  if (snum != stage_num) {
    return -ERANGE;
  }
  return do_fetch(shard_id, marker, max, result);
}

int SIProvider_SingleStage::get_start_marker(int snum, int shard_id, std::string *marker) const
{
  if (snum != stage_num) {
    return -ERANGE;
  }
  return do_get_start_marker(shard_id, marker);
}

int SIProvider_SingleStage::get_cur_state(int snum, int shard_id, std::string *marker) const
{
  if (snum != stage_num) {
    return -ERANGE;
  }
  return do_get_cur_state(shard_id, marker);
}

int SIProviderClient::init_markers()
{
  auto stages = provider->get_stages();

  if (stages.empty()) {
    return 0;
  }

  SIProvider::StageInfo prev;

  for (auto& snum : stages) {
    SIProvider::StageInfo sinfo;
    int r = provider->get_stage_info(snum, &sinfo);
    if (r < 0) {
      return r;
    }
    bool all_history = (prev.params.type != SIProvider::StageType::FULL ||
                        sinfo.params.type != SIProvider::StageType::INC);
    auto& stage_markers = initial_stage_markers[sinfo.snum];
    stage_markers.reserve(sinfo.params.num_shards);
    for (int i = 0; i < sinfo.params.num_shards; ++i) {
      std::string marker;
      int r = (!all_history ? provider->get_cur_state(snum, i, &marker) : 
                              provider->get_start_marker(snum, i, &marker));
      if (r < 0) {
        return r;
      }
      stage_markers.push_back(marker);
    }
  }

  init_stage(provider->get_first_stage());

  return 0;
}

int SIProviderClient::init_stage(int new_sid)
{
  int r = provider->get_stage_info(new_sid, &stage_info);
  if (r < 0) {
    return r;
  }

  auto iter = initial_stage_markers.find(stage_info.snum);
  if (iter != initial_stage_markers.end()) {
    markers = std::move(iter->second);
    initial_stage_markers.erase(iter);
  } else {
    markers.resize(stage_info.params.num_shards);
    markers.clear();
  }

  done.resize(stage_info.params.num_shards);
  done.clear();

  num_complete = 0;
  return 0;
}


int SIProviderClient::fetch(int shard_id, int max, SIProvider::fetch_result *result) {
  if (shard_id > stage_info.params.num_shards) {
    return -ERANGE;
  }

  int r = provider->fetch(stage_info.snum, shard_id, markers[shard_id], max, result);
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
  int next_snum;

  int r = provider->get_next_stage(stage_info.snum, &next_snum);
  if (r < 0) {
    return r;
  }

  r = init_stage(next_snum);
  if (r < 0) {
    return r;
  }

  if (new_num_shards) {
    *new_num_shards = stage_num_shards();
  }

  return 0;
}

