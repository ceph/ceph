// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <map>

#include "include/buffer.h"

/*
 * non-stateful entity that is responsible for providing data
 */
class SIProvider
{
public:
  enum StageType {
    UNKNOWN = -1,
    FULL = 0,
    INC = 1,
  };

  using stage_id_t = int32_t;

  struct StageParams {
    std::string name;
    StageType type{UNKNOWN};
    int num_shards{0};
  };

  struct StageInfo {
    stage_id_t sid{-1};
    StageParams params;
  };

  struct Entry {
    std::string key;
    bufferlist data;
  };

  struct fetch_result {
    std::vector<Entry> entries;
    bool more{false}; /* more available now */
    bool done{false}; /* stage done */
  };

  virtual ~SIProvider() {}

  virtual stage_id_t get_first_stage() = 0;
  virtual int get_next_stage(stage_id_t sid, stage_id_t *next_sid) = 0;
  virtual std::vector<stage_id_t> get_stages() = 0;

  virtual int get_stage_info(stage_id_t stage_id, StageInfo *stage_info) const = 0;
  virtual int fetch(stage_id_t stage_id, int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int get_start_marker(stage_id_t stage_id, int shard_id, std::string *marker) const = 0;
  virtual int get_cur_state(stage_id_t stage_id, int shard_id, std::string *marker) const = 0;
};

class SIProvider_SingleStage : public SIProvider
{
  StageInfo stage_info;
  stage_id_t stage_id{0};

protected:
  virtual int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int do_get_start_marker(int shard_id, std::string *marker) const = 0;
  virtual int do_get_cur_state(int shard_id, std::string *marker) const = 0;
public:
  SIProvider_SingleStage(const std::string& name,
                         StageType type,
                         int num_shards) : stage_info({0, {name, type, num_shards}}) {}
  stage_id_t get_first_stage() override {
    return stage_id;
  }
  int get_next_stage(stage_id_t sid, stage_id_t *next_sid) override {
    return -ERANGE;
  }
  std::vector<stage_id_t> get_stages() override {
    return { stage_id };
  }

  int get_stage_info(stage_id_t stage_id, StageInfo *sinfo) const override {
    *sinfo = stage_info;
    return 0;
  }

  int fetch(stage_id_t sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(stage_id_t stage_id, int shard_id, std::string *marker) const override;
  int get_cur_state(stage_id_t stage_id, int shard_id, std::string *marker) const override;
};

using SIProviderRef = std::shared_ptr<SIProvider>;

/*
 * stateful entity that is responsible for fetching data
 */
class SIClient
{
public:
  virtual ~SIClient() {}

  virtual int init_markers() = 0;
  virtual int fetch(int shard_id, int max, SIProvider::fetch_result *result) = 0;

  virtual int load_state() = 0;
  virtual int save_state() = 0;

  virtual SIProvider::StageInfo get_stage_info() const = 0;
};

using SIClientRef = std::shared_ptr<SIClient>;


/*
 * provider client: a client that connects directly to a provider
 */
class SIProviderClient : public SIClient
{
  SIProviderRef provider;
  std::vector<std::string> markers;
  std::map<SIProvider::stage_id_t, std::vector<std::string> > initial_stage_markers;

  SIProvider::StageInfo stage_info;
  int num_complete{0};

  std::vector<bool> done;

  int init_stage(SIProvider::stage_id_t new_stage);

public:
  SIProviderClient(SIProviderRef& _provider) : provider(_provider) {}

  int init_markers() override;

  int fetch(int shard_id, int max, SIProvider::fetch_result *result) override;

  SIProvider::StageInfo get_stage_info() const override {
    return stage_info;
  }

  int num_shards() const {
    return stage_info.params.num_shards;
  }

  bool stage_complete() const {
    return (num_complete == num_shards());
  }

  int promote_stage(int *new_num_shards);
};


class RGWSIPManager
{
  std::map<std::string, SIProviderRef> providers;

public:
  RGWSIPManager() {}

  void register_sip(const std::string& id, SIProviderRef provider) {
    providers[id] = provider;
  }

  SIProviderRef find_sip(const std::string& id) {
    auto iter = providers.find(id);
    if (iter == providers.end()) {
      return nullptr;
    }
    return iter->second;
  }

  std::vector<std::string> list_sip() const {
    std::vector<std::string> result;
    result.reserve(providers.size());
    for (auto& entry : providers) {
      result.push_back(entry.first);
    }
    return result;
  }
};

