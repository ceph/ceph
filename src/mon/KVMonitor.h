// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <optional>

#include "mon/PaxosService.h"

class MonSession;

extern const std::string KV_PREFIX;

class KVMonitor : public PaxosService
{
public:
  /**
   * A range of keys removed by "config-key rm-range".
   *
   * Recorded in the commit delta so that peers and subscribers can tell a
   * range was removed without the delta having to list every key in it.
   */
  struct RangeDeleteOp {
    std::string prefix;
    std::string start;
    std::string end;

    RangeDeleteOp() = default;
    RangeDeleteOp(const std::string& p, const std::string& s,
                  const std::string& e)
      : prefix(p), start(s), end(e) {}

    /**
     * The half-open key range [begin, end) covered by this op.
     *
     * With both bounds the range is [prefix/start, prefix/end); with
     * neither it is every key prefixed by 'prefix'. A single bound leaves
     * the other side open. An empty *end_bound means unbounded above.
     */
    void get_bounds(std::string *begin, std::string *end_bound) const;

    /** Whether this removal affects any key prefixed by 'p'. */
    bool overlaps_prefix(const std::string& p) const;

    void encode(ceph::buffer::list& bl) const {
      ENCODE_START(1, 1, bl);
      encode(prefix, bl);
      encode(start, bl);
      encode(end, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      DECODE_START(1, bl);
      decode(prefix, bl);
      decode(start, bl);
      decode(end, bl);
      DECODE_FINISH(bl);
    }
  };

  /**
   * Exclusive upper bound for "every key prefixed by p".
   *
   * Increments the last byte of p that is not 0xff, dropping the trailing
   * 0xff run. Returns an empty string if p consists only of 0xff bytes (or
   * is empty), meaning unbounded above. Note that appending a printable
   * sentinel would not work: keys are arbitrary byte strings, so anything
   * sorting above the sentinel would be missed.
   */
  static std::string prefix_upper_bound(const std::string& p);

  /**
   * Validate "config-key rm-range" arguments; returns 0 or -EINVAL with an
   * explanation written to ss.
   */
  static int validate_range_params(const std::string& prefix,
                                   const std::string& start,
                                   const std::string& end,
                                   std::ostream& ss);

  /**
   * Encode a commit delta.
   *
   * Deltas that carry no range removals are written in the original format
   * so that a monitor without range support can still read them. Deltas
   * that do are prefixed with a marker that cannot begin the original
   * format, which keeps decoding self-describing rather than dependent on
   * whichever features happen to be present when the delta is read back.
   */
  static void encode_delta(
    const std::map<std::string,std::optional<ceph::buffer::list>>& key_ops,
    const std::vector<RangeDeleteOp>& range_ops,
    ceph::buffer::list *bl);

  static void decode_delta(
    const ceph::buffer::list& bl,
    std::map<std::string,std::optional<ceph::buffer::list>> *key_ops,
    std::vector<RangeDeleteOp> *range_ops);

private:
  version_t version = 0;
  std::map<std::string,std::optional<ceph::buffer::list>> pending;
  std::vector<RangeDeleteOp> pending_range_deletes;

  bool _have_prefix(const std::string &prefix);

  /** Whether the quorum can read deltas containing range removals. */
  bool _range_ops_supported() const;

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
};

WRITE_CLASS_ENCODER(KVMonitor::RangeDeleteOp)
