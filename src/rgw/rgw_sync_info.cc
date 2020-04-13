#include "rgw_sync_info.h"

int SIProviderClient::init_marker(bool all_history) {
  if (!all_history) {
    return provider->get_cur_state(&marker);
  }
  return provider->get_start_marker(&marker);
}

int SIProviderClient::fetch(int max, SIProvider::fetch_result *result) {
  int r = provider->fetch(marker, max, result);
  if (r < 0) {
    return r;
  }

  if (!result->entries.empty()) {
    marker = result->entries.back().key;
  }

  done = result->done;

  return 0;
}

int SIPShardedStage::init_markers(bool all_history) {
  for (auto iter : shards) {
    int r = iter->init_marker(all_history);
    if (r < 0) {
      return r;
    }
  }
  return 0;
}

int SIPShardedStage::fetch(int shard_id, int max, SIProvider::fetch_result *result) {
  if (shard_id >= (int)shards.size()) {
    return -ENOENT;
  }

  if (done_vec[shard_id]) {
    result->more = false;
    result->done = true;
    return  0;
  }

  int r = shards[shard_id]->fetch(max, result);
  if (r < 0) {
    return r;
  }

  if (result->done) {
    done_vec[shard_id] = true;
    ++done_count;
    complete = (done_count == (int)shards.size());
  }

  return 0;
}


int SIPMultiStageClient::fetch(int shard_id, int max, SIProvider::fetch_result *result) {
  if (cur_stage >= (int)stages.size()) {
    return -ENOENT;
  }

  auto stage = cur_stage_ref();

  int r = stage->fetch(shard_id, max, result);
  if (r < 0) {
    return r;
  }

  if (result->done) {
    if (stage->is_complete()) {
      ++cur_stage;
    }
  }

  return 0;
}

bool SIPMultiStageClient::promote_stage(int *new_num_shards) {
  auto stage = cur_stage_ref();
  if (!stage->is_complete()) {
    return false;
  }

  ++cur_stage;
  if (new_num_shards) {
    *new_num_shards = num_shards();
  }

  return true;
}

int SIPMultiStageClient::init_markers()
{
  if (stages.empty()) {
    return -EINVAL;
  }

  auto prev_type = SIProvider::Type::UNKNOWN;

  for (auto& stage : stages) {
    bool full_history = (prev_type != SIProvider::Type::FULL);

    stage->init_markers(full_history); 

    prev_type = stage->get_provider_type();
  }

  return 0;
}
