// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <memory>
#include <map>
#include <functional>

#include "include/buffer.h"
#include "common/ceph_json.h"

#include "services/svc_sip_marker.h"

class SIProvider_Container;

namespace ceph {
  class Formatter;
}

/*
 * non-stateful entity that is responsible for providing data
 */

class JSONObj;

struct rgw_sip_pos {
  string marker;
  ceph::real_time timestamp;

  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};

class SIProvider {
public:
  enum StageType {
    UNKNOWN = -1,
    FULL = 0,
    INC = 1,
  };

  using stage_id_t = RGWSI_SIP_Marker::stage_id_t;

  struct StageInfo {
    stage_id_t sid;
    std::optional<stage_id_t> next_sid;
    StageType type{UNKNOWN};
    int num_shards{0};
    bool disabled{false};

    void dump(Formatter *f) const;
    void decode_json(JSONObj *obj);
  };

  struct Info {
    std::string name;
    std::string data_type;
    stage_id_t first_stage;
    stage_id_t last_stage;
    std::vector<StageInfo> stages;

    void dump(Formatter *f) const;
    void decode_json(JSONObj *obj);
  };

  struct Entry {
    std::string key;

    bufferlist data; /* encoded EntryInfoBase */
  };

  struct EntryInfoBase {
    virtual ~EntryInfoBase() {}

    virtual string get_data_type() const = 0;

    virtual void encode(bufferlist& bl) const = 0;
    virtual void decode(bufferlist::const_iterator& bl) = 0;

    virtual void dump(Formatter *f) const = 0;
    virtual void decode_json(JSONObj *obj) = 0;
  };

  struct fetch_result {
    std::vector<Entry> entries;
    bool more{false}; /* more available now */
    bool done{false}; /* stage done */
  };

  class TypeHandler {
  public:
    virtual ~TypeHandler() {}

    virtual string get_data_type() const = 0;

    virtual int handle_entry(const SIProvider::stage_id_t& sid,
                             SIProvider::Entry& entry,
                             std::function<int(SIProvider::EntryInfoBase&)> f) = 0;

    virtual int decode_json_results(const stage_id_t& sid,
                                    JSONObj *obj,
                                    SIProvider::fetch_result *result) = 0;
  };

  class TypeHandlerProvider {
  public:
    virtual ~TypeHandlerProvider() {}

    virtual TypeHandler *get_type_handler() = 0;
  };

  using TypeHandlerProviderRef = std::shared_ptr<TypeHandlerProvider>;

  virtual ~SIProvider() {}

  virtual Info get_info() = 0;

  virtual stage_id_t get_first_stage() = 0;
  virtual stage_id_t get_last_stage() = 0;
  virtual int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) = 0;
  virtual std::vector<stage_id_t> get_stages() = 0;

  virtual int get_stage_info(const stage_id_t& sid, StageInfo *stage_info) = 0;
  virtual int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) = 0;
  virtual int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) = 0;

  virtual const std::string& get_name() const = 0;
  virtual const std::optional<std::string>& get_instance() const = 0;
  virtual std::string get_id() const {
    const auto& instance = get_instance();
    if (!instance) {
      return get_name();
    }
    return get_name() + "/" + *instance;
  }

  virtual TypeHandlerProvider *get_type_provider() = 0;
  virtual TypeHandler *get_type_handler() {
    auto tp = get_type_provider();
    if (!tp) {
      return nullptr;
    }
    return tp->get_type_handler();
  }

  virtual int trim(const stage_id_t& sid,
                   int shard_id,
                   const std::string& marker) = 0;

  string get_data_type() {
    auto th = get_type_handler();
    return th->get_data_type();
  }
};

class SIProviderCommon : public SIProvider
{
protected:
  CephContext *cct;
  std::string name;
  std::optional<std::string> instance;

public:
  SIProviderCommon(CephContext *_cct,
                   const std::string& _name,
                   std::optional<std::string> _instance) : cct(_cct),
                                                           name(_name),
                                                           instance(_instance) {}

  SIProvider::Info get_info() override;

  const std::string& get_name() const override {
    return name;
  }

  const std::optional<std::string>& get_instance() const override {
    return instance;
  }
};


class SIProvider_SingleStage : public SIProviderCommon
{
protected:
  StageInfo stage_info;
  SIProvider::TypeHandlerProviderRef type_provider;

  virtual int do_fetch(int shard_id, std::string marker, int max, fetch_result *result) = 0;
  virtual int do_get_start_marker(int shard_id, std::string *marker, ceph::real_time *timestamp) const = 0;
  virtual int do_get_cur_state(int shard_id, std::string *marker, ceph::real_time *timestamp) const = 0;
  virtual int do_trim(int shard_id, const std::string& marker) = 0;
public:
  SIProvider_SingleStage(CephContext *_cct,
                         const std::string& name,
                         std::optional<std::string> instance,
                         SIProvider::TypeHandlerProviderRef _type_provider,
                         StageType type,
                         int num_shards,
                         bool disabled) : SIProviderCommon(_cct, name, instance),
                                          stage_info({name, nullopt, type, num_shards, disabled}),
                                          type_provider(_type_provider) {}

  SIProvider::TypeHandlerProvider *get_type_provider() override {
    return type_provider.get();
  }

  stage_id_t get_first_stage() override {
    return stage_info.sid;
  }
  stage_id_t get_last_stage() override {
    return stage_info.sid;
  }

  int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) override {
    return -ENOENT;
  }
  std::vector<stage_id_t> get_stages() override {
    return { stage_info.sid };
  }

  int get_stage_info(const stage_id_t& sid, StageInfo *sinfo) override {
    if (sid != stage_info.sid) {
      return -ERANGE;
    }

    *sinfo = stage_info;
    return 0;
  }

  int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;
  int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;

  int trim(const stage_id_t& sid, int shard_id, const std::string& marker) override;
};

using SIProviderRef = std::shared_ptr<SIProvider>;

class SIProvider_Container : public SIProviderCommon
{
  friend class TypeProvider;

  class TypeProvider : public SIProvider::TypeHandlerProvider {
    SIProvider_Container *sip;
  public:
    TypeProvider(SIProvider_Container *_sip) : sip(_sip) {}

    SIProvider::TypeHandler *get_type_handler() override;
  } type_provider;

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
                       std::optional<std::string> _instance,
                       std::vector<SIProviderRef>& _providers);

  stage_id_t get_first_stage() override;
  stage_id_t get_last_stage() override;

  int get_next_stage(const stage_id_t& sid, stage_id_t *next_sid) override;
  std::vector<SIProvider::stage_id_t> get_stages() override;

  int get_stage_info(const stage_id_t& sid, StageInfo *sinfo) override;

  int fetch(const stage_id_t& sid, int shard_id, std::string marker, int max, fetch_result *result) override;
  int get_start_marker(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;
  int get_cur_state(const stage_id_t& sid, int shard_id, std::string *marker, ceph::real_time *timestamp) override;

  SIProvider::TypeHandlerProvider *get_type_provider() override {
    return &type_provider;
  }

  int trim(const stage_id_t& sid, int shard_id, const std::string& marker) override;
};

template<class T>
class SITypeHandlerProvider_Default : public SIProvider::TypeHandlerProvider {
  class TypeHandler : public SIProvider::TypeHandler
  {
    struct ExpandedEntry {
      std::string key;
      T info;

      void decode_json(JSONObj *obj) {
        JSONDecoder::decode_json("key", key, obj);
        JSONDecoder::decode_json("info", info, obj);
      }
    };

  public:
    TypeHandler() {}

    int handle_entry(const SIProvider::stage_id_t& sid,
                     SIProvider::Entry& entry,
                     std::function<int(SIProvider::EntryInfoBase&)> f) override {
      T t;
      try {
        decode(t, entry.data);
      } catch (buffer::error& err) {
        return -EINVAL;
      }
      return f(t);
    }

    int decode_json_results(const SIProvider::stage_id_t& sid,
                            JSONObj *obj,
                            SIProvider::fetch_result *result) override {
      std::vector<ExpandedEntry> entries;
      try {
        JSONDecoder::decode_json("more", result->more, obj);
        JSONDecoder::decode_json("done", result->done, obj);
        JSONDecoder::decode_json("entries", entries, obj);
      } catch (JSONDecoder::err& e) {
        return -EINVAL;
      }
      for (auto& e : entries) {
        auto& entry = result->entries.emplace_back();
        entry.key = std::move(e.key);
        e.info.encode(entry.data);
      }
      return 0;
    }

    string get_data_type() const {
      T t;
      return t.get_data_type();
    }
  } type_handler;
public:
  SITypeHandlerProvider_Default() {}

  SIProvider::TypeHandler *get_type_handler() override {
    return &type_handler;
  }
};

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

  virtual SIProvider::StageInfo get_stage_info() = 0;

  virtual int stage_num_shards() = 0;

  virtual bool is_shard_done(int shard_id) = 0;
  virtual bool stage_complete() = 0;
};

using SIClientRef = std::shared_ptr<SIClient>;


/*
 * provider client: a client that connects directly to a provider
 */
class SIProviderClient : public SIClient
{
  SIProviderRef provider;

  using stage_id_t = SIProvider::stage_id_t;

  struct State {
    std::vector<std::string> markers;
    std::map<stage_id_t, std::vector<std::string> > initial_stage_markers;
    SIProvider::StageInfo stage_info;
    int num_complete{0};
    std::vector<bool> done;
  } state;

  int init_stage(const stage_id_t& new_stage);

public:
  SIProviderClient(SIProviderRef& _provider) : provider(_provider) {}

  int init_markers() override;

  int fetch(int shard_id, int max, SIProvider::fetch_result *result) override;

  SIProvider::StageInfo get_stage_info() override {
    return state.stage_info;
  }

  int stage_num_shards() override {
    return state.stage_info.num_shards;
  }

  bool is_shard_done(int shard_id) override {
    return (shard_id < stage_num_shards() &&
            state.done[shard_id]);
  }

  bool stage_complete() override {
    return (state.num_complete == stage_num_shards());
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

  SIProviderRef get(std::optional<std::string> instance) override {
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

