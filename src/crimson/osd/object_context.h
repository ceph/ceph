// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <optional>
#include <utility>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include "common/fmt_common.h"
#include "common/intrusive_lru.h"
#include "osd/object_state.h"
#include "crimson/common/exception.h"
#include "crimson/common/tri_mutex.h"
#include "crimson/osd/osd_operation.h"

namespace ceph {
  class Formatter;
}

namespace crimson::common {
  class ConfigProxy;
}

namespace crimson::osd {

class Watch;
struct SnapSetContext;
using SnapSetContextRef = boost::intrusive_ptr<SnapSetContext>;

template <typename OBC>
struct obc_to_hoid {
  using type = hobject_t;
  const type &operator()(const OBC &obc) {
    return obc.obs.oi.soid;
  }
};

struct SnapSetContext :
  public boost::intrusive_ref_counter<SnapSetContext,
                                     boost::thread_unsafe_counter>
{
  hobject_t oid;
  SnapSet snapset;
  bool exists = false;
  /**
   * exists
   *
   * Because ObjectContext's are cached, we need to be able to express the case
   * where the object to which a cached ObjectContext refers does not exist.
   * ObjectContext's for yet-to-be-created objects are initialized with exists=false.
   * The ObjectContext for a deleted object will have exists set to false until it falls
   * out of cache (or another write recreates the object).
   */
  explicit SnapSetContext(const hobject_t& o) :
    oid(o), exists(false) {}
};

class ObjectContext : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    hobject_t, ObjectContext, obc_to_hoid<ObjectContext>>>
{
private:
  tri_mutex lock;

public:
  ObjectState obs;
  SnapSetContextRef ssc;
  // the watch / notify machinery rather stays away from the hot and
  // frequented paths. std::map is used mostly because of developer's
  // convenience.
  using watch_key_t = std::pair<uint64_t, entity_name_t>;
  std::map<watch_key_t, seastar::shared_ptr<crimson::osd::Watch>> watchers;

  CommonOBCPipeline obc_pipeline;

  ObjectContext(hobject_t hoid) : lock(hoid.to_str()),
                                  obs(std::move(hoid)) {}

  void update_from(
    std::pair<ObjectState, SnapSetContextRef> obc_data) {
    obs = obc_data.first;
    ssc = obc_data.second;
  }

  const hobject_t &get_oid() const {
    return obs.oi.soid;
  }

  bool is_head() const {
    return get_oid().is_head();
  }

  hobject_t get_head_oid() const {
    return get_oid().get_head();
  }

  const SnapSet &get_head_ss() const {
    ceph_assert(is_head());
    ceph_assert(ssc);
    return ssc->snapset;
  }

  void set_head_state(ObjectState &&_obs, SnapSetContextRef &&_ssc) {
    ceph_assert(is_head());
    obs = std::move(_obs);
    ssc = std::move(_ssc);
    fully_loaded = true;
  }

  void set_clone_state(ObjectState &&_obs) {
    ceph_assert(!is_head());
    obs = std::move(_obs);
    fully_loaded = true;
  }

  void set_clone_ssc(SnapSetContextRef head_ssc) {
    ceph_assert(!is_head());
    ssc = head_ssc;
  }

  /// pass the provided exception to any waiting consumers of this ObjectContext
  template<typename Exception>
  void interrupt(Exception ex) {
    lock.abort(std::move(ex));
  }

  bool is_loaded() const {
    return fully_loaded;
  }

  bool is_valid() const {
    return !invalidated;
  }

private:
  boost::intrusive::list_member_hook<> obc_accessing_hook;
  uint64_t list_link_cnt = 0;

  /**
   * loading_started
   *
   * ObjectContext instances may be used for pipeline stages
   * prior to actually being loaded.
   *
   * ObjectContextLoader::load_and_lock* use loading_started
   * to determine whether to initiate loading or simply take
   * the desired lock directly.
   *
   * If loading_started is not set, the task must set it and
   * (syncronously) take an exclusive lock.  That exclusive lock
   * must be held until the loading completes, at which point the
   * lock may be relaxed or released.
   *
   * If loading_started is set, it is safe to directly take
   * the desired lock, once the lock is obtained loading may
   * be assumed to be complete.
   *
   * loading_started, once set, remains set for the lifetime
   * of the object.
   */
  bool loading_started = false;

  /// true once set_*_state has been called, used for debugging
  bool fully_loaded = false;

  /**
   * invalidated
   *
   * Set to true upon eviction from cache.  This happens to all
   * cached obc's upon interval change and to the target of
   * a repop received on a replica to ensure that the cached
   * state is refreshed upon subsequent replica read.
   */
  bool invalidated = false;

  friend class ObjectContextRegistry;
  friend class ObjectContextLoader;
public:

  template <typename ListType>
  void append_to(ListType& list) {
    if (list_link_cnt++ == 0) {
      list.push_back(*this);
    }
  }

  template <typename ListType>
  void remove_from(ListType&& list) {
    assert(list_link_cnt > 0);
    if (--list_link_cnt == 0) {
      list.erase(std::decay_t<ListType>::s_iterator_to(*this));
    }
  }

  template <typename FormatContext>
  auto fmt_print_ctx(FormatContext & ctx) const {
    return fmt::format_to(
      ctx.out(), "ObjectContext({}, oid={}, refcount={})",
      (void*)this,
      get_oid(),
      get_use_count());
  }

  using obc_accessing_option_t = boost::intrusive::member_hook<
    ObjectContext,
    boost::intrusive::list_member_hook<>,
    &ObjectContext::obc_accessing_hook>;

  bool empty() const {
    return !lock.is_acquired();
  }
  bool is_request_pending() const {
    return lock.is_acquired();
  }
};
using ObjectContextRef = ObjectContext::Ref;

class ObjectContextRegistry : public md_config_obs_t  {
  ObjectContext::lru_t obc_lru;

public:
  ObjectContextRegistry(crimson::common::ConfigProxy &conf);
  ~ObjectContextRegistry();

  std::pair<ObjectContextRef, bool> get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get_or_create(hoid);
  }
  ObjectContextRef maybe_get_cached_obc(const hobject_t &hoid) {
    return obc_lru.get(hoid);
  }

  void clear_range(const hobject_t &from,
                   const hobject_t &to) {
    obc_lru.clear_range(from, to, [](auto &obc) {
      obc.invalidated = true;
    });
  }

  void invalidate_on_interval_change() {
    obc_lru.clear([](auto &obc) {
      obc.invalidated = true;
    });
  }

  template <class F>
  void for_each(F&& f) {
    obc_lru.for_each(std::forward<F>(f));
  }

  std::vector<std::string> get_tracked_keys() const noexcept final;
  void handle_conf_change(const crimson::common::ConfigProxy& conf,
                          const std::set <std::string> &changed) final;
};

std::optional<hobject_t> resolve_oid(const SnapSet &ss,
                                     const hobject_t &oid);

} // namespace crimson::osd

template <>
struct fmt::formatter<RWState::State> : fmt::ostream_formatter {};
