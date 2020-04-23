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
  enum Type {
    UNKNOWN = -1,
    FULL = 0,
    INC = 1,
  };

  struct Info {
    Type type{UNKNOWN};
    int num_shards{0};
  };

  struct Entry {
    std::string key;
    bufferlist data;
  };

  struct fetch_result {
    std::vector<Entry> entries;
    bool more{false}; /* more available now */
    bool done{false}; /* completely done */
  };

  virtual ~SIProvider() {}

  virtual Info get_info() const = 0;
  virtual int fetch(int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int get_start_marker(std::string *marker) const = 0;
  virtual int get_cur_state(std::string *marker) const = 0;
};

using SIProviderRef = std::shared_ptr<SIProvider>;

/*
 * stateful entity that is responsible for fetching data
 */
class SIClient
{
public:
  virtual ~SIClient() {}

  virtual int init_marker(bool all_history) = 0;
  virtual int fetch(int shard_id, int max, SIProvider::fetch_result *result) = 0;

  virtual int load_state() = 0;
  virtual int save_state() = 0;

  virtual SIProvider::Info get_provider_info() const = 0;
};

using SIClientRef = std::shared_ptr<SIClient>;


/*
 * provider client: a client that connects directly to a provider
 */
class SIProviderClient : public SIClient
{
  SIProviderRef provider;
  std::string marker;

  bool done{false};

public:
  SIProviderClient(SIProviderRef& _provider) : provider(_provider) {}

  int init_marker(bool all_history) override;

  int fetch(int shard_id, int max, SIProvider::fetch_result *result) override;

  SIProvider::Info get_provider_info() const override {
    return provider->get_info();
  }
};

/*
 * chained client: a client that connects to another client
 */
class SIChainedClient : public SIClient
{
  SIClientRef client;
  std::string marker;

public:
  SIChainedClient(SIClientRef& _client) : client(_client) {}

  int init_marker(bool all_history) override;

  int fetch(int shard_id, int max, SIProvider::fetch_result *result) override;

  SIProvider::Info get_provider_info() const override {
    return client->get_provider_info();
  }
};


/*
 * sharded stage: a collection of clients (each represents a shard) that form a single sync stage
 */
class SIPShardedStage
{
  std::vector<SIClientRef> shards;

  std::vector<bool> done_vec;

  int done_count{0};
  bool complete{false};

public:
  SIPShardedStage(std::vector<SIClientRef>& _shards) : shards(_shards),
                                                       done_vec(_shards.size()) {}

  int init_markers(bool all_history);
  int fetch(int shard_id, int max, SIProvider::fetch_result *result);

  SIProvider::Info get_provider_info() const {
    if (num_shards() == 0) {
      return { SIProvider::Type::UNKNOWN, 0 };
    }

    return shards[0]->get_provider_info();
  }

  int num_shards() const {
    return shards.size();
  };

  bool is_shard_done(int i) {
    return done_vec[i];
  }

  bool is_complete() const {
    return complete;
  }

};

using SIPShardedStageRef = std::shared_ptr<SIPShardedStage>;


/*
 * multi stage: a collection of sync stages
 */
class SIPMultiStageClient
{
  std::vector<SIPShardedStageRef> stages;

  int cur_stage{0};


public:
  SIPMultiStageClient(std::vector<SIPShardedStageRef>& _stages) : stages(_stages) {}

  int init_markers();

  int fetch(int shard_id, int max, SIProvider::fetch_result *result);

  SIPShardedStageRef& cur_stage_ref() {
    return stages[cur_stage];
  }

  const SIPShardedStageRef& cur_stage_ref() const {
    return stages[cur_stage];
  }

  int num_shards() const {
    return cur_stage_ref()->num_shards();
  }

  bool stage_complete() const {
    return cur_stage_ref()->is_complete();
  }

  bool promote_stage(int *new_num_shards);
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

