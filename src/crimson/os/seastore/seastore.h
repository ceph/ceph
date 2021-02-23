// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <unordered_map>
#include <map>
#include <typeinfo>
#include <vector>

#include <optional>
#include <seastar/core/future.hh>

#include "include/uuid.h"

#include "os/Transaction.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/collection_manager.h"

namespace crimson::os::seastore {

class SeastoreCollection;
class Onode;
using OnodeRef = boost::intrusive_ptr<Onode>;
class TransactionManager;

class SeaStore final : public FuturizedStore {
  uuid_d osd_fsid;

public:

  SeaStore(
    TransactionManagerRef tm,
    CollectionManagerRef cm,
    OnodeManagerRef om
  ) : transaction_manager(std::move(tm)),
      collection_manager(std::move(cm)),
      onode_manager(std::move(om)) {}

  ~SeaStore();
    
  seastar::future<> stop() final;
  seastar::future<> mount() final;
  seastar::future<> umount() final;

  seastar::future<> mkfs(uuid_d new_osd_fsid) final;
  seastar::future<store_statfs_t> stat() const final;

  read_errorator::future<ceph::bufferlist> read(
    CollectionRef c,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    uint32_t op_flags = 0) final;
  read_errorator::future<ceph::bufferlist> readv(
    CollectionRef c,
    const ghobject_t& oid,
    interval_set<uint64_t>& m,
    uint32_t op_flags = 0) final;
  get_attr_errorator::future<ceph::bufferptr> get_attr(
    CollectionRef c,
    const ghobject_t& oid,
    std::string_view name) const final;
  get_attrs_ertr::future<attrs_t> get_attrs(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<struct stat> stat(
    CollectionRef c,
    const ghobject_t& oid) final;

  read_errorator::future<omap_values_t> omap_get_values(
    CollectionRef c,
    const ghobject_t& oid,
    const omap_keys_t& keys) final;

  /// Retrieves paged set of values > start (if present)
  using omap_get_values_ret_bare_t = std::tuple<bool, omap_values_t>;
  using omap_get_values_ret_t = read_errorator::future<
    omap_get_values_ret_bare_t>;
  omap_get_values_ret_t omap_get_values(
    CollectionRef c,           ///< [in] collection
    const ghobject_t &oid,     ///< [in] oid
    const std::optional<std::string> &start ///< [in] start, empty for begin
    ) final; ///< @return <done, values> values.empty() iff done

  read_errorator::future<bufferlist> omap_get_header(
    CollectionRef c,
    const ghobject_t& oid) final;

  seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects(
    CollectionRef c,
    const ghobject_t& start,
    const ghobject_t& end,
    uint64_t limit) const final;

  seastar::future<CollectionRef> create_new_collection(const coll_t& cid) final;
  seastar::future<CollectionRef> open_collection(const coll_t& cid) final;
  seastar::future<std::vector<coll_t>> list_collections() final;

  seastar::future<> do_transaction(
    CollectionRef ch,
    ceph::os::Transaction&& txn) final;

  seastar::future<OmapIteratorRef> get_omap_iterator(
    CollectionRef ch,
    const ghobject_t& oid) final;
  seastar::future<std::map<uint64_t, uint64_t>> fiemap(
    CollectionRef ch,
    const ghobject_t& oid,
    uint64_t off,
    uint64_t len) final;

  seastar::future<> write_meta(const std::string& key,
		  const std::string& value) final;
  seastar::future<std::tuple<int, std::string>> read_meta(const std::string& key) final;
  uuid_d get_fsid() const final;

  unsigned get_max_attr_name_length() const final {
    return 256;
  }

private:
  struct internal_context_t {
    CollectionRef ch;
    ceph::os::Transaction ext_transaction;

    internal_context_t(
      CollectionRef ch,
      ceph::os::Transaction &&_ext_transaction)
      : ch(ch), ext_transaction(std::move(_ext_transaction)),
	iter(ext_transaction.begin()) {}

    TransactionRef transaction;
    std::vector<OnodeRef> onodes;

    ceph::os::Transaction::iterator iter;

    void reset(TransactionRef &&t) {
      transaction = std::move(t);
      onodes.clear();
      iter = ext_transaction.begin();
    }
  };

  static void on_error(ceph::os::Transaction &t);

  template <typename F>
  auto repeat_with_internal_context(
    CollectionRef ch,
    ceph::os::Transaction &&t,
    F &&f) {
    return seastar::do_with(
      internal_context_t{ ch, std::move(t) },
      std::forward<F>(f),
      [](auto &ctx, auto &f) {
	return repeat_eagain([&]() {
	  ctx.reset(make_transaction());
	  return std::invoke(f, ctx);
	}).handle_error(
	  crimson::ct_error::eagain::pass_further{},
	  crimson::ct_error::all_same_way([&ctx](auto e) {
	    on_error(ctx.ext_transaction);
	  })
	);
      });
  }

  template <typename Ret, typename F>
  auto repeat_with_onode(
    CollectionRef ch,
    const ghobject_t &oid,
    F &&f) {
    return seastar::do_with(
      oid,
      Ret{},
      TransactionRef(),
      OnodeRef(),
      std::forward<F>(f),
      [=](auto &oid, auto &ret, auto &t, auto &onode, auto &f) {
	return repeat_eagain([&, this] {
	  t = make_transaction();
	  return onode_manager->get_onode(
	    *t, oid
	  ).safe_then([&, this](auto onode_ret) {
	    onode = std::move(onode_ret);
	    return f(*t, *onode);
	  }).safe_then([&ret](auto _ret) {
	    ret = _ret;
	  });
	}).safe_then([&ret] {
	  return seastar::make_ready_future<Ret>(ret);
	});
      });
  }


  friend class SeaStoreOmapIterator;
  omap_get_values_ret_t omap_list(
    CollectionRef ch,
    const ghobject_t &oid,
    const std::optional<string> &_start,
    OMapManager::omap_list_config_t config);

  TransactionManagerRef transaction_manager;
  CollectionManagerRef collection_manager;
  OnodeManagerRef onode_manager;

  using tm_ertr = TransactionManager::base_ertr;
  using tm_ret = tm_ertr::future<>;
  tm_ret _do_transaction_step(
    internal_context_t &ctx,
    CollectionRef &col,
    std::vector<OnodeRef> &onodes,
    ceph::os::Transaction::iterator &i);

  tm_ret _remove(
    internal_context_t &ctx,
    OnodeRef &onode);
  tm_ret _touch(
    internal_context_t &ctx,
    OnodeRef &onode);
  tm_ret _write(
    internal_context_t &ctx,
    OnodeRef &onode,
    uint64_t offset, size_t len, const ceph::bufferlist& bl,
    uint32_t fadvise_flags);
  tm_ret _omap_set_values(
    internal_context_t &ctx,
    OnodeRef &onode,
    std::map<std::string, ceph::bufferlist> &&aset);
  tm_ret _omap_set_header(
    internal_context_t &ctx,
    OnodeRef &onode,
    const ceph::bufferlist &header);
  tm_ret _omap_rmkeys(
    internal_context_t &ctx,
    OnodeRef &onode,
    const omap_keys_t& aset);
  tm_ret _omap_rmkeyrange(
    internal_context_t &ctx,
    OnodeRef &onode,
    const std::string &first,
    const std::string &last);
  tm_ret _truncate(
    internal_context_t &ctx,
    OnodeRef &onode, uint64_t size);
  tm_ret _setattrs(
    internal_context_t &ctx,
    OnodeRef &onode,
    std::map<std::string,bufferptr>& aset);
  tm_ret _create_collection(
    internal_context_t &ctx,
    const coll_t& cid, int bits);
  tm_ret _remove_collection(
    internal_context_t &ctx,
    const coll_t& cid);

  boost::intrusive_ptr<SeastoreCollection> _get_collection(const coll_t& cid);
};

}
