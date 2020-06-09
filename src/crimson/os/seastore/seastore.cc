// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "common/safe_io.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"

#include "crimson/os/futurized_collection.h"

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/cache.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

using crimson::common::local_conf;

namespace crimson::os::seastore {

struct SeastoreCollection final : public FuturizedCollection {
  template <typename... T>
  SeastoreCollection(T&&... args) :
    FuturizedCollection(std::forward<T>(args)...) {}
};

SeaStore::SeaStore(const std::string& path)
  : segment_manager(segment_manager::create_ephemeral(
		      segment_manager::DEFAULT_TEST_EPHEMERAL)),
    cache(std::make_unique<Cache>(*segment_manager)),
    journal(new Journal(*segment_manager)),
    lba_manager(
      lba_manager::create_lba_manager(*segment_manager, *cache)),
    transaction_manager(
      new TransactionManager(
	*segment_manager,
	*journal,
	*cache,
	*lba_manager)),
    onode_manager(onode_manager::create_ephemeral())
{}

SeaStore::~SeaStore() = default;

seastar::future<> SeaStore::stop()
{
  return seastar::now();
}

seastar::future<> SeaStore::mount()
{
  return seastar::now();
}

seastar::future<> SeaStore::umount()
{
  return seastar::now();
}

seastar::future<> SeaStore::mkfs(uuid_d new_osd_fsid)
{
  return seastar::now();
}

seastar::future<store_statfs_t> SeaStore::stat() const
{
  logger().debug("{}", __func__);
  store_statfs_t st;
  return seastar::make_ready_future<store_statfs_t>(st);
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
SeaStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  return seastar::make_ready_future<std::tuple<std::vector<ghobject_t>, ghobject_t>>(
    std::make_tuple(std::vector<ghobject_t>(), end));
}

seastar::future<CollectionRef> SeaStore::create_new_collection(const coll_t& cid)
{
  auto c = _get_collection(cid);
  return seastar::make_ready_future<CollectionRef>(c);
}

seastar::future<CollectionRef> SeaStore::open_collection(const coll_t& cid)
{
  return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
}

seastar::future<std::vector<coll_t>> SeaStore::list_collections()
{
  return seastar::make_ready_future<std::vector<coll_t>>();
}

SeaStore::read_errorator::future<ceph::bufferlist> SeaStore::read(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  uint32_t op_flags)
{
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

SeaStore::read_errorator::future<ceph::bufferlist> SeaStore::readv(
  CollectionRef ch,
  const ghobject_t& oid,
  interval_set<uint64_t>& m,
  uint32_t op_flags)
{
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

SeaStore::get_attr_errorator::future<ceph::bufferptr> SeaStore::get_attr(
  CollectionRef ch,
  const ghobject_t& oid,
  std::string_view name) const
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug("{} {} {}",
                __func__, c->get_cid(), oid);
  return crimson::ct_error::enoent::make();
}

SeaStore::get_attrs_ertr::future<SeaStore::attrs_t> SeaStore::get_attrs(
  CollectionRef ch,
  const ghobject_t& oid)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug("{} {} {}",
		 __func__, c->get_cid(), oid);
  return crimson::ct_error::enoent::make();
}

seastar::future<struct stat> stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  return seastar::make_ready_future<struct stat>();
}


seastar::future<struct stat> SeaStore::stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  struct stat st;
  return seastar::make_ready_future<struct stat>(st);
}

seastar::future<SeaStore::omap_values_t>
SeaStore::omap_get_values(CollectionRef ch,
                           const ghobject_t& oid,
                           const omap_keys_t& keys)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug("{} {} {}",
                __func__, c->get_cid(), oid);
  return seastar::make_ready_future<omap_values_t>();
}


seastar::future<ceph::bufferlist> omap_get_header(
  CollectionRef c,
  const ghobject_t& oid)
{
  return seastar::make_ready_future<bufferlist>();
}

seastar::future<std::tuple<bool, SeaStore::omap_values_t>>
SeaStore::omap_get_values(
    CollectionRef ch,
    const ghobject_t &oid,
    const std::optional<string> &start
  ) {
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug(
    "{} {} {}",
    __func__, c->get_cid(), oid);
  return seastar::make_ready_future<std::tuple<bool, omap_values_t>>(
    std::make_tuple(false, omap_values_t()));
}

seastar::future<FuturizedStore::OmapIteratorRef> get_omap_iterator(
  CollectionRef ch,
  const ghobject_t& oid)
{
  return seastar::make_ready_future<FuturizedStore::OmapIteratorRef>();
}

seastar::future<std::map<uint64_t, uint64_t>> fiemap(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  return seastar::make_ready_future<std::map<uint64_t, uint64_t>>();
}

seastar::future<> SeaStore::do_transaction(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  return seastar::do_with(
    _t.begin(),
    transaction_manager->create_transaction(),
    std::vector<OnodeRef>(),
    std::move(_t),
    std::move(_ch),
    [this](auto &iter, auto &trans, auto &onodes, auto &t, auto &ch) {
      return onode_manager->get_or_create_onodes(
	*trans, iter.get_objects()).safe_then(
	  [this, &iter, &trans, &onodes, &t, &ch](auto &&read_onodes) {
	    onodes = std::move(read_onodes);
	    return seastar::do_until(
	      [&iter]() { return iter.have_op(); },
	      [this, &iter, &trans, &onodes, &t, &ch]() {
		return _do_transaction_step(trans, ch, onodes, iter).safe_then(
		  [this, &trans] {
		    return transaction_manager->submit_transaction(std::move(trans));
		  }).handle_error(
		    // TODO: add errorator::do_until
		    crimson::ct_error::eagain::handle([]() {
		      // TODO retry
		    }),
		    write_ertr::all_same_way([&t](auto e) {
		      logger().error(" transaction dump:\n");
		      JSONFormatter f(true);
		      f.open_object_section("transaction");
		      t.dump(&f);
		      f.close_section();
		      std::stringstream str;
		      f.flush(str);
		      logger().error("{}", str.str());
		      abort();
		    }));
	      });
	  }).safe_then([this, &trans, &onodes]() {
	    return onode_manager->write_dirty(*trans, onodes);
	  }).safe_then([]() {
	    // TODO: complete transaction!
	    return;
	  }).handle_error(
	    write_ertr::all_same_way([&t](auto e) {
	      logger().error(" transaction dump:\n");
	      JSONFormatter f(true);
	      f.open_object_section("transaction");
	      t.dump(&f);
	      f.close_section();
	      std::stringstream str;
	      f.flush(str);
	      logger().error("{}", str.str());
	      abort();
	    })).then([&t]() {
	      for (auto i : {
		  t.get_on_applied(),
		    t.get_on_commit(),
		    t.get_on_applied_sync()}) {
		if (i) {
		  i->complete(0);
		}
	      }
	    });
    });
}

SeaStore::write_ertr::future<> SeaStore::_do_transaction_step(
  TransactionRef &trans,
  CollectionRef &col,
  std::vector<OnodeRef> &onodes,
  ceph::os::Transaction::iterator &i)
{
  auto get_onode = [&onodes](size_t i) -> OnodeRef& {
    ceph_assert(i < onodes.size());
    return onodes[i];
  };

  using ceph::os::Transaction;
  try {
    switch (auto op = i.decode_op(); op->op) {
    case Transaction::OP_NOP:
      return write_ertr::now();
    case Transaction::OP_REMOVE:
    {
      return _remove(trans, get_onode(op->oid));
    }
    break;
    case Transaction::OP_TOUCH:
    {
      return _touch(trans, get_onode(op->oid));
    }
    break;
    case Transaction::OP_WRITE:
    {
      uint64_t off = op->off;
      uint64_t len = op->len;
      uint32_t fadvise_flags = i.get_fadvise_flags();
      ceph::bufferlist bl;
      i.decode_bl(bl);
      return _write(trans, get_onode(op->oid), off, len, bl, fadvise_flags);
    }
    break;
    case Transaction::OP_TRUNCATE:
    {
      uint64_t off = op->off;
      return _truncate(trans, get_onode(op->oid), off);
    }
    break;
    case Transaction::OP_SETATTR:
    {
      std::string name = i.decode_string();
      ceph::bufferlist bl;
      i.decode_bl(bl);
      std::map<std::string, bufferptr> to_set;
      to_set[name] = bufferptr(bl.c_str(), bl.length());
      return _setattrs(trans, get_onode(op->oid), to_set);
    }
    break;
    case Transaction::OP_MKCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      return _create_collection(trans, cid, op->split_bits);
    }
    break;
    case Transaction::OP_OMAP_SETKEYS:
    {
      std::map<std::string, ceph::bufferlist> aset;
      i.decode_attrset(aset);
      return _omap_set_values(trans, get_onode(op->oid), std::move(aset));
    }
    break;
    case Transaction::OP_OMAP_SETHEADER:
    {
      ceph::bufferlist bl;
      i.decode_bl(bl);
      return _omap_set_header(trans, get_onode(op->oid), bl);
    }
    break;
    case Transaction::OP_OMAP_RMKEYS:
    {
      omap_keys_t keys;
      i.decode_keyset(keys);
      return _omap_rmkeys(trans, get_onode(op->oid), keys);
    }
    break;
    case Transaction::OP_OMAP_RMKEYRANGE:
    {
      string first, last;
      first = i.decode_string();
      last = i.decode_string();
      return _omap_rmkeyrange(trans, get_onode(op->oid), first, last);
    }
    break;
    case Transaction::OP_COLL_HINT:
    {
      ceph::bufferlist hint;
      i.decode_bl(hint);
      return write_ertr::now();
    }
    default:
      logger().error("bad op {}", static_cast<unsigned>(op->op));
      return crimson::ct_error::input_output_error::make();
    }
  } catch (std::exception &e) {
    logger().error("{} got exception {}", __func__, e);
    return crimson::ct_error::input_output_error::make();
  }
}

SeaStore::write_ertr::future<> SeaStore::_remove(
  TransactionRef &trans,
  OnodeRef &onode)
{
  logger().debug("{} onode={}",
                __func__, *onode);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_touch(
  TransactionRef &trans,
  OnodeRef &onode)
{
  logger().debug("{} onode={}",
                __func__, *onode);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_write(
  TransactionRef &trans,
  OnodeRef &onode,
  uint64_t offset, size_t len, const ceph::bufferlist& bl,
  uint32_t fadvise_flags)
{
  logger().debug("{}: {} {} ~ {}",
                __func__, *onode, offset, len);
  assert(len == bl.length());

/*
  return onode_manager->get_or_create_onode(cid, oid).safe_then([=, &bl](auto ref) {
    return;
  }).handle_error(
    crimson::ct_error::enoent::handle([]() {
      return;
    }),
    OnodeManager::open_ertr::pass_further{}
  );
  */
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_set_values(
  TransactionRef &trans,
  OnodeRef &onode,
  std::map<std::string, ceph::bufferlist> &&aset)
{
  logger().debug(
    "{}: {} {} keys",
    __func__, *onode, aset.size());

  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_set_header(
  TransactionRef &trans,
  OnodeRef &onode,
  const ceph::bufferlist &header)
{
  logger().debug(
    "{}: {} {} bytes",
    __func__, *onode, header.length());
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_rmkeys(
  TransactionRef &trans,
  OnodeRef &onode,
  const omap_keys_t& aset)
{
  logger().debug(
    "{} {} {} keys",
    __func__, *onode, aset.size());
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_omap_rmkeyrange(
  TransactionRef &trans,
  OnodeRef &onode,
  const std::string &first,
  const std::string &last)
{
  logger().debug(
    "{} {} first={} last={}",
    __func__, *onode, first, last);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_truncate(
  TransactionRef &trans,
  OnodeRef &onode,
  uint64_t size)
{
  logger().debug("{} onode={} size={}",
                __func__, *onode, size);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_setattrs(
  TransactionRef &trans,
  OnodeRef &onode,
  std::map<std::string,bufferptr>& aset)
{
  logger().debug("{} onode={}",
                __func__, *onode);
  return write_ertr::now();
}

SeaStore::write_ertr::future<> SeaStore::_create_collection(
  TransactionRef &trans,
  const coll_t& cid, int bits)
{
  return write_ertr::now();
}

boost::intrusive_ptr<SeastoreCollection> SeaStore::_get_collection(const coll_t& cid)
{
  return new SeastoreCollection{cid};
}

seastar::future<> SeaStore::write_meta(const std::string& key,
					const std::string& value)
{
  return seastar::make_ready_future<>();
}

seastar::future<std::tuple<int, std::string>> SeaStore::read_meta(const std::string& key)
{
  return seastar::make_ready_future<std::tuple<int, std::string>>(
    std::make_tuple(0, ""s));
}

uuid_d SeaStore::get_fsid() const
{
  return osd_fsid;
}

}
