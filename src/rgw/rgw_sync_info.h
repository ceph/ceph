// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <map>

#include "include/buffer.h"

class SIProvider_Container;

/*
 * non-stateful entity that is responsible for providing data
 */
class SIProvider
{
  friend class SIProvider_Container;

protected:
  CephContext *cct;
  std::string name;

public:
  enum StageType {
    UNKNOWN = -1,
    FULL = 0,
    INC = 1,
  };

  using stage_id_t = std::string;

  struct StageInfo {
    stage_id_t sid;
    StageType type{UNKNOWN};
    int num_shards{0};
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

  SIProvider(CephContext *_cct, const std::string& _name) : cct(_cct),
                                                            name(_name) {}
  virtual ~SIProvider() {}

  virtual stage_id_t get_first_stage() const = 0;
  virtual stage_id_t get_last_stage() const = 0;
  virtual int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) = 0;
  virtual std::vector<stage_id_t> get_stages() = 0;

  virtual int get_stage_info(const stage_id_t& sid, StageInfo *stage_info) const = 0;
  virtual int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker) const = 0;
  virtual int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker) const = 0;

  const std::string& get_name() {
    return name;
  }
};

class SIProvider_SingleStage : public SIProvider
{
protected:
  StageInfo stage_info;

  virtual int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int do_get_start_marker(int shard_id, std::string *marker) const = 0;
  virtual int do_get_cur_state(int shard_id, std::string *marker) const = 0;
public:
  SIProvider_SingleStage(CephContext *_cct,
                         const std::string& name,
                         StageType type,
                         int num_shards) : SIProvider(_cct, name),
                                           stage_info({name, type, num_shards}) {}
  stage_id_t get_first_stage() const override {
    return stage_info.sid;
  }
  stage_id_t get_last_stage() const override {
    return stage_info.sid;
  }

  int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) override {
    return -ENOENT;
  }
  std::vector<stage_id_t> get_stages() override {
    return { stage_info.sid };
  }

  int get_stage_info(const stage_id_t& sid, StageInfo *sinfo) const override {
    if (sid != stage_info.sid) {
      return -ERANGE;
    }

    *sinfo = stage_info;
    return 0;
  }

  int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker) const override;
  int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker) const override;
};

using SIProviderRef = std::shared_ptr<SIProvider>;

class SIProvider_Container : public SIProvider
{
protected:
  std::vector<SIProviderRef> providers;
  std::vector<std::string> pids; /* provider ids */
  std::map<std::string, int> providers_index;

  SIProvider::stage_id_t encode_sid(const std::string& pid,
                                    const SIProvider::stage_id_t& provider_sid) const;
  bool decode_sid(const stage_id_t& sid,
                  SIProviderRef *provider,
                  SIProvider::stage_id_t *provider_sid,
                  int *index = nullptr) const;

public:
  SIProvider_Container(CephContext *_cct,
                       const std::string& _name,
                       std::vector<SIProviderRef>& _providers);

  stage_id_t get_first_stage() const override;
  stage_id_t get_last_stage() const override;

  int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) override;
  std::vector<SIProvider::stage_id_t> get_stages() override;

  int get_stage_info(const stage_id_t& sid, StageInfo *sinfo) const override;

  int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker) const override;
  int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker) const override;
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

  virtual int stage_num_shards() const = 0;

  virtual bool is_shard_done(int shard_id) const = 0;
  virtual bool stage_complete() const = 0;
};

using SIClientRef = std::shared_ptr<SIClient>;


/*
 * provider client: a client that connects directly to a provider
 */
class SIProviderClient : public SIClient
{
  SIProviderRef provider;
  std::vector<std::string> markers;

  using stage_id_t = SIProvider::stage_id_t;

  std::map<stage_id_t, std::vector<std::string> > initial_stage_markers;

  SIProvider::StageInfo stage_info;
  int num_complete{0};

  std::vector<bool> done;

  int init_stage(const stage_id_t& new_stage);

public:
  SIProviderClient(SIProviderRef& _provider) : provider(_provider) {}

  int init_markers() override;

  int fetch(int shard_id, int max, SIProvider::fetch_result *result) override;

  SIProvider::StageInfo get_stage_info() const override {
    return stage_info;
  }

  int stage_num_shards() const override {
    return stage_info.num_shards;
  }

  bool is_shard_done(int shard_id) const override {
    return (shard_id < stage_num_shards() &&
            done[shard_id]);
  }

  bool stage_complete() const override {
    return (num_complete == stage_num_shards());
  }

  int promote_stage(int *new_num_shards);
};

class RGWSIPGenerator {
public:
  virtual ~RGWSIPGenerator() {}

  virtual SIProviderRef get(std::optional<std::string> instance) = 0;
};

using RGWSIPGeneratorRef = std::shared_ptr<RGWSIPGenerator>;


class RGWSIPGen_Single : public RGWSIPGenerator
{
  SIProviderRef sip;

public:
  RGWSIPGen_Single(SIProvider *_sip) : sip(_sip) {}

  SIProviderRef get(std::optional<std::string> instance) {
    return sip;
  }
};


class RGWSIPManager
{
  std::map<std::string, RGWSIPGeneratorRef> sip_gens;

public:
  RGWSIPManager() {}

  void register_sip(const std::string& id, RGWSIPGeneratorRef gen) {
    sip_gens[id] = gen;
  }

  SIProviderRef find_sip(const std::string& id, std::optional<std::string> instance) {
    auto iter = sip_gens.find(id);
    if (iter == sip_gens.end()) {
      return nullptr;
    }
    return iter->second->get(instance);
  }

  std::vector<std::string> list_sip() const {
    std::vector<std::string> result;
    result.reserve(sip_gens.size());
    for (auto& entry : sip_gens) {
      result.push_back(entry.first);
    }
    return result;
  }
};

