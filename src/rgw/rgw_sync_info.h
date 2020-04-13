// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>

#include "include/buffer.h"

class SIProvider
{
public:
  enum Type {
    UNKNOWN = -1,
    FULL = 0,
    INC = 1,
  };

  struct info {
    std::string key;
    bufferlist data;
  };

  struct fetch_result {
    std::vector<info> entries;
    bool more{false}; /* more available now */
    bool done{false}; /* completely done */
  };

  virtual ~SIProvider() {}

  virtual Type get_type() const = 0;
  virtual int fetch(std::string marker, int max, fetch_result *result) = 0;
  virtual int get_start_marker(std::string *marker) const = 0;
  virtual int get_cur_state(std::string *marker) const = 0;
};

using SIProviderRef = std::shared_ptr<SIProvider>;

class SIPClient
{
public:
  virtual ~SIPClient() {}

  virtual int fetch(int max, SIProvider::fetch_result *result) = 0;
};

using SIPClientRef = std::shared_ptr<SIPClient>;

class SIProviderClient : public SIPClient
{
  SIProviderRef provider;
  std::string marker;

  bool done{false};

public:
  SIProviderClient(SIProviderRef& _provider) : provider(_provider) {}

  int init_marker(bool all_history);

  int fetch(int max, SIProvider::fetch_result *result) override;

  SIProvider::Type get_provider_type() const {
    return provider->get_type();
  }
};

using SIProviderClientRef = std::shared_ptr<SIProviderClient>;


class SIPShardedStage
{
  std::vector<SIProviderClientRef> shards;

  std::vector<bool> done_vec;

  int done_count{0};
  bool complete{false};

public:
  SIPShardedStage(std::vector<SIProviderClientRef>& _shards) : shards(_shards),
                                                               done_vec(_shards.size()) {}

  int init_markers(bool all_history);
  int fetch(int shard_id, int max, SIProvider::fetch_result *result);

  SIProvider::Type get_provider_type() const {
    if (num_shards() == 0) {
      return SIProvider::Type::UNKNOWN;
    }

    return shards[0]->get_provider_type();
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

