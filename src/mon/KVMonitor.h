// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <optional>

#include "mon/PaxosService.h"
#include "include/encoding.h"
#include "include/ceph_features.h"  // For CEPH_FEATURE definitions

class MonSession;

extern const std::string KV_PREFIX;

class KVMonitor : public PaxosService
{
public:
  struct RangeDeleteOp {
    std::string prefix;
    std::string start;
    std::string end;
    RangeDeleteOp() = default;
    RangeDeleteOp(const std::string& p, const std::string& s, const std::string& e) : prefix(p), start(s), end(e) {}

    void encode(ceph::buffer::list &bl) const {
      ENCODE_START(1, 1, bl);
      encode(prefix, bl);
      encode(start, bl);
      encode(end, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator &bl) {
      DECODE_START(1, bl);
      decode(prefix, bl);
      decode(start, bl);
      decode(end, bl);
      DECODE_FINISH(bl);
    }
  };


private:
  version_t version = 0;
  std::map<std::string,std::optional<ceph::buffer::list>> pending;
  std::vector<RangeDeleteOp> pending_range_deletes;

  bool _have_prefix(const std::string &prefix);

public:
  KVMonitor(Monitor &m, Paxos &p, const std::string& service_name);

  void init() override;

  void get_store_prefixes(std::set<std::string>& s) const override;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void on_active() override;
  void tick() override;

  int validate_osd_destroy(const int32_t id, const uuid_d& uuid);
  void do_osd_destroy(int32_t id, uuid_d& uuid);
  int validate_osd_new(
      const uuid_d& uuid,
      const std::string& dmcrypt_key,
      std::stringstream& ss);
  void do_osd_new(const uuid_d& uuid, const std::string& dmcrypt_key);

  void check_sub(MonSession *s);
  void check_sub(Subscription *sub);
  void check_all_subs();

  bool maybe_send_update(Subscription *sub);


  // used by other services to adjust kv content; note that callers MUST ensure that
  // propose_pending() is called and a commit is forced to provide atomicity and
  // proper subscriber notifications.
  void enqueue_set(const std::string& key, bufferlist &v) {
    pending[key] = v;
  }
  void enqueue_rm(const std::string& key) {
    pending[key].reset();
  }

protected:
  // Helper methods for range operations
  int _validate_range_params(const std::string& prefix, const std::string& start, const std::string& end, std::ostream& ss) const;
};

WRITE_CLASS_ENCODER(KVMonitor::RangeDeleteOp)
