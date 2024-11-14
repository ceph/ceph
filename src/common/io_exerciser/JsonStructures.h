#include <memory>
#include <string>
#include <vector>

#include "OpType.h"
#include "include/types.h"

/* Overview
 *
 * class JSONStructure
 *   Stores elements of a JSONStructure in C++ friendly format so they do not
 *   have to be parsed from strings. Includes encode and decode functions to
 *   provide easy ways to convert from c++ structures to json structures.
 *
 */

class JSONObj;

namespace ceph {
namespace io_exerciser {
namespace json {
class JSONStructure {
 public:
  JSONStructure(std::shared_ptr<Formatter> formatter =
                    std::make_shared<JSONFormatter>(false));

  virtual ~JSONStructure() = default;

  std::string encode_json();
  virtual void decode_json(JSONObj* obj) = 0;
  virtual void dump() const = 0;

 protected:
  std::shared_ptr<Formatter> formatter;

 private:
  std::ostringstream oss;
};

class OSDMapRequest : public JSONStructure {
 public:
  OSDMapRequest(const std::string& pool_name, const std::string& object,
                const std::string& nspace,
                std::shared_ptr<Formatter> formatter =
                    std::make_shared<JSONFormatter>(false));
  OSDMapRequest(std::shared_ptr<Formatter> formatter =
                    std::make_shared<JSONFormatter>(false));

  std::string prefix = "osd map";
  std::string pool;
  std::string object;
  std::string nspace;
  std::string format = "json";

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDMapReply : public JSONStructure {
 public:
  OSDMapReply(std::shared_ptr<Formatter> formatter =
                  std::make_shared<JSONFormatter>(false));

  epoch_t epoch;
  std::string pool;
  uint64_t pool_id;
  std::string objname;
  std::string raw_pgid;
  std::string pgid;
  std::vector<int> up;
  int up_primary;
  std::vector<int> acting;
  int acting_primary;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDPoolGetRequest : public JSONStructure {
 public:
  OSDPoolGetRequest(const std::string& pool_name,
                    std::shared_ptr<ceph::Formatter> formatter =
                        std::make_shared<JSONFormatter>(false));
  OSDPoolGetRequest(JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter =
                                      std::make_shared<JSONFormatter>(false));

  std::string prefix = "osd pool get";
  std::string pool;
  std::string var = "erasure_code_profile";
  std::string format = "json";

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDPoolGetReply : public JSONStructure {
 public:
  OSDPoolGetReply(std::shared_ptr<ceph::Formatter> formatter =
                      std::make_shared<JSONFormatter>(false));

  std::string erasure_code_profile;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDECProfileGetRequest : public JSONStructure {
 public:
  OSDECProfileGetRequest(const std::string& profile_name,
                         std::shared_ptr<ceph::Formatter> formatter =
                             std::make_shared<JSONFormatter>(false));
  OSDECProfileGetRequest(std::shared_ptr<ceph::Formatter> formatter =
                             std::make_shared<JSONFormatter>(false));

  std::string prefix = "osd pool get";
  std::string name;
  std::string format = "json";

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDECProfileGetReply : public JSONStructure {
 public:
  OSDECProfileGetReply(std::shared_ptr<ceph::Formatter> formatter =
                           std::make_shared<JSONFormatter>(false));

  std::string crush_device_class;
  std::string crush_failure_domain;
  int crush_num_failure_domains;
  int crush_osds_per_failure_domain;
  std::string crush_root;
  bool jerasure_per_chunk_alignment;
  int k;
  int m;
  std::string plugin;
  std::string technique;
  std::string w;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDECProfileSetRequest : public JSONStructure {
 public:
  OSDECProfileSetRequest(const std::string& name,
                         const std::vector<std::string>& profile,
                         std::shared_ptr<Formatter> formatter =
                             std::make_shared<JSONFormatter>(false));
  OSDECProfileSetRequest(std::shared_ptr<Formatter> formatter =
                             std::make_shared<JSONFormatter>(false));

  std::string prefix = "osd erasure-code-profile set";
  std::string name;
  std::vector<std::string> profile;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDECPoolCreateRequest : public JSONStructure {
 public:
  OSDECPoolCreateRequest(const std::string& pool,
                         const std::string& erasure_code_profile,
                         std::shared_ptr<Formatter> formatter =
                             std::make_shared<JSONFormatter>(false));
  OSDECPoolCreateRequest(std::shared_ptr<Formatter> formatter =
                             std::make_shared<JSONFormatter>(false));

  std::string prefix = "osd pool create";
  std::string pool;
  std::string pool_type = "erasure";
  int pg_num = 8;
  int pgp_num = 8;
  std::string erasure_code_profile;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class OSDSetRequest : public JSONStructure {
 public:
  OSDSetRequest(const std::string& key,
                const std::optional<bool>& yes_i_really_mean_it = std::nullopt,
                std::shared_ptr<Formatter> formatter =
                    std::make_shared<JSONFormatter>(false));
  OSDSetRequest(std::shared_ptr<Formatter> formatter =
                    std::make_shared<JSONFormatter>(false));

  std::string prefix = "osd set";
  std::string key;
  std::optional<bool> yes_i_really_mean_it = std::nullopt;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class BalancerOffRequest : public JSONStructure {
 public:
  BalancerOffRequest(std::shared_ptr<Formatter> formatter =
                         std::make_shared<JSONFormatter>(false));

  std::string prefix = "balancer off";

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class BalancerStatusRequest : public JSONStructure {
 public:
  BalancerStatusRequest(std::shared_ptr<Formatter> formatter =
                            std::make_shared<JSONFormatter>(false));

  std::string prefix = "balancer status";

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class BalancerStatusReply : public JSONStructure {
 public:
  BalancerStatusReply(std::shared_ptr<Formatter> formatter =
                          std::make_shared<JSONFormatter>(false));

  bool active;
  std::string last_optimization_duration;
  std::string last_optimization_started;
  std::string mode;
  bool no_optimization_needed;
  std::string optimize_result;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class ConfigSetRequest : public JSONStructure {
 public:
  ConfigSetRequest(const std::string& who, const std::string& name,
                   const std::string& value,
                   const std::optional<bool>& force = std::nullopt,
                   std::shared_ptr<Formatter> formatter =
                       std::make_shared<JSONFormatter>(false));
  ConfigSetRequest(std::shared_ptr<Formatter> formatter =
                       std::make_shared<JSONFormatter>(false));

  std::string prefix = "config set";
  std::string who;
  std::string name;
  std::string value;
  std::optional<bool> force;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class InjectECErrorRequest : public JSONStructure {
 public:
  InjectECErrorRequest(InjectOpType injectOpType, const std::string& pool,
                       const std::string& objname, int shardid,
                       const std::optional<uint64_t>& type,
                       const std::optional<uint64_t>& when,
                       const std::optional<uint64_t>& duration,
                       std::shared_ptr<Formatter> formatter =
                           std::make_shared<JSONFormatter>(false));
  InjectECErrorRequest(std::shared_ptr<Formatter> formatter =
                           std::make_shared<JSONFormatter>(false));

  std::string prefix;
  std::string pool;
  std::string objname;
  int shardid;
  std::optional<uint64_t> type;
  std::optional<uint64_t> when;
  std::optional<uint64_t> duration;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};

class InjectECClearErrorRequest : public JSONStructure {
 public:
  InjectECClearErrorRequest(InjectOpType injectOpType, const std::string& pool,
                            const std::string& objname, int shardid,
                            const std::optional<uint64_t>& type,
                            std::shared_ptr<Formatter> formatter =
                                std::make_shared<JSONFormatter>(false));

  InjectECClearErrorRequest(std::shared_ptr<Formatter> formatter =
                                std::make_shared<JSONFormatter>(false));

  std::string prefix;
  std::string pool;
  std::string objname;
  int shardid;
  std::optional<uint64_t> type;

  void decode_json(JSONObj* obj) override;
  void dump() const override;
};
}  // namespace json
}  // namespace io_exerciser
}  // namespace ceph