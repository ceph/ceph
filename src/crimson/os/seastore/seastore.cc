// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include <algorithm>

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include "common/safe_io.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"

#include "crimson/os/futurized_collection.h"

#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/onode_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

using crimson::common::local_conf;

namespace crimson::os::seastore {

SeaStore::~SeaStore() {}

struct SeastoreCollection final : public FuturizedCollection {
  template <typename... T>
  SeastoreCollection(T&&... args) :
    FuturizedCollection(std::forward<T>(args)...) {}
};

seastar::future<> SeaStore::stop()
{
  return transaction_manager->close(
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::stop"
    }
  );

}

seastar::future<> SeaStore::mount()
{
  return transaction_manager->mount(
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::mount"
    }
  );
}

seastar::future<> SeaStore::umount()
{
  return seastar::now();
}

seastar::future<> SeaStore::mkfs(uuid_d new_osd_fsid)
{
  return transaction_manager->mkfs(
  ).safe_then([this] {
    return seastar::do_with(
      make_transaction(),
      [this](auto &t) {
	return onode_manager->mkfs(*t
	).safe_then([this, &t] {
	  return collection_manager->mkfs(*t);
	}).safe_then([this, &t](auto coll_root) {
	  transaction_manager->write_collection_root(
	    *t,
	    coll_root);
	  return transaction_manager->submit_transaction(
	    std::move(t));
	});
      });
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::mkfs"
    }
  );
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
  return repeat_with_internal_context(
    c,
    ceph::os::Transaction{},
    [this, cid](auto &ctx) {
      return _create_collection(
	ctx,
	cid,
	4 /* TODO */
      ).safe_then([this, &ctx] {
	return transaction_manager->submit_transaction(std::move(ctx.transaction));
      });
    }).then([c] {
      return CollectionRef(c);
    });
}

seastar::future<CollectionRef> SeaStore::open_collection(const coll_t& cid)
{
  return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
}

seastar::future<std::vector<coll_t>> SeaStore::list_collections()
{
  return seastar::do_with(
    std::vector<coll_t>(),
    [this](auto &ret) {
      return repeat_eagain([this, &ret] {

	return seastar::do_with(
	  make_transaction(),
	  [this, &ret](auto &t) {
	    return transaction_manager->read_collection_root(*t
	    ).safe_then([this, &ret, &t](auto coll_root) {
	      return collection_manager->list(
		coll_root,
		*t);
	    }).safe_then([this, &ret, &t](auto colls) {
	      ret.resize(colls.size());
	      std::transform(
		colls.begin(), colls.end(), ret.begin(),
		[](auto p) { return p.first; });
	    });
	  });
      }).safe_then([&ret] {
	return seastar::make_ready_future<std::vector<coll_t>>(ret);
      });
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid error in SeaStore::list_collections"
      }
    );
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

seastar::future<struct stat> SeaStore::stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  return repeat_with_onode<struct stat>(
    c,
    oid,
    [=](auto &t, auto &onode) {
      struct stat st;
      auto &olayout = onode.get_layout();
      st.st_size = olayout.size;
      st.st_blksize = 4096;
      st.st_blocks = (st.st_size + st.st_blksize - 1) / st.st_blksize;
      st.st_nlink = 1;
      return seastar::make_ready_future<struct stat>();
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid error in SeaStore::stat"
       }
    );
}

using crimson::os::seastore::omap_manager::BtreeOMapManager;

using omap_int_ertr_t = OMapManager::base_ertr::extend<
  crimson::ct_error::enoent
  >;

auto
SeaStore::omap_get_header(
  CollectionRef c,
  const ghobject_t& oid)
  -> read_errorator::future<bufferlist>
{
  return seastar::make_ready_future<bufferlist>();
}

auto
SeaStore::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const omap_keys_t &keys)
  -> read_errorator::future<omap_values_t>
{
  using int_ret_t = omap_int_ertr_t::future<omap_values_t>;
  auto c = static_cast<SeastoreCollection*>(ch.get());
  return repeat_with_onode<omap_values_t>(
    c,
    oid,
    [this, &oid, &keys](auto &t, auto &onode) -> int_ret_t {
      auto omap_root = onode.get_layout().omap_root.get();
      if (omap_root.is_null()) {
	return seastar::make_ready_future<omap_values_t>();
      } else {
	return seastar::do_with(
	  BtreeOMapManager(*transaction_manager),
	  omap_root,
	  omap_values_t(),
	  [&, this](auto &manager, auto &root, auto &ret) -> int_ret_t {
	    return crimson::do_for_each(
	      keys.begin(),
	      keys.end(),
	      [&, this](auto &key) {
		return manager.omap_get_value(
		  root,
		  t,
		  key
		).safe_then([&ret, &key](auto &&p) {
		  if (p) {
		    bufferlist bl;
		    bl.append(*p);
		    ret.emplace(
		      std::make_pair(
			std::move(key),
			std::move(bl)));
		  }
		  return seastar::now();
		});
	      }).safe_then([&ret] {
		return std::move(ret);
	      });
	  });
      }
    });
}

SeaStore::omap_get_values_ret_t SeaStore::omap_list(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<string> &start,
  OMapManager::omap_list_config_t config)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  logger().debug(
    "{} {} {}",
    __func__, c->get_cid(), oid);
  using ret_bare_t = std::tuple<bool, SeaStore::omap_values_t>;
  using int_ret_t = omap_int_ertr_t::future<ret_bare_t>;
  return repeat_with_onode<ret_bare_t>(
    c,
    oid,
    [this, config, &oid, &start](auto &t, auto &onode) -> int_ret_t {
      auto omap_root = onode.get_layout().omap_root.get();
      if (omap_root.is_null()) {
	return seastar::make_ready_future<ret_bare_t>(
	  true, omap_values_t{}
	);
      } else {
	return seastar::do_with(
	  BtreeOMapManager(*transaction_manager),
	  omap_root,
	  [&, config, this](auto &manager, auto &root) -> int_ret_t {
	    return manager.omap_list(
	      root,
	      t,
	      start,
	      config
	    ).safe_then([](auto &&p) {
	      return seastar::make_ready_future<ret_bare_t>(
		p.first, p.second
	      );
	    });
	  });
      }
    });
}

SeaStore::omap_get_values_ret_t SeaStore::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<string> &start)
{
  return omap_list(
    ch, oid, start,
    OMapManager::omap_list_config_t::with_inclusive(false));
}

class SeaStoreOmapIterator : public FuturizedStore::OmapIterator {
  using omap_values_t = FuturizedStore::omap_values_t;

  SeaStore &seastore;
  CollectionRef ch;
  const ghobject_t oid;

  omap_values_t current;
  omap_values_t::iterator iter;

  seastar::future<> repopulate_from(
    std::optional<std::string> from,
    bool inclusive) {
    return seastar::do_with(
      from,
      [this, inclusive](auto &from) {
	return seastore.omap_list(
	  ch,
	  oid,
	  from,
	  OMapManager::omap_list_config_t::with_inclusive(inclusive)
	).safe_then([this](auto p) {
	  auto &[complete, values] = p;
	  current.swap(values);
	  if (current.empty()) {
	    assert(complete);
	  }
	  iter = current.begin();
	});
      }).handle_error(
	crimson::ct_error::assert_all{
	  "Invalid error in SeaStoreOmapIterator::repopulate_from"
        }
      );
  }
public:
  SeaStoreOmapIterator(
    SeaStore &seastore,
    CollectionRef ch,
    const ghobject_t &oid) :
    seastore(seastore),
    ch(ch),
    oid(oid),
    iter(current.begin())
  {}

  seastar::future<> seek_to_first() final {
    return repopulate_from(
      std::nullopt,
      false);
  }
  seastar::future<> upper_bound(const std::string &after) final {
    return repopulate_from(
      after,
      false);
  }
  seastar::future<> lower_bound(const std::string &from) final {
    return repopulate_from(
      from,
      true);
  }
  bool valid() const {
    return iter != current.end();
  }
  seastar::future<> next() final {
    assert(valid());
    auto prev = iter++;
    if (iter == current.end()) {
      return repopulate_from(
	prev->first,
	false);
    } else {
      return seastar::now();
    }
  }
  std::string key() {
    return iter->first;
  }
  ceph::buffer::list value() {
    return iter->second;
  }
  int status() const {
    return 0;
  }
  ~SeaStoreOmapIterator() {}
};

seastar::future<FuturizedStore::OmapIteratorRef> SeaStore::get_omap_iterator(
  CollectionRef ch,
  const ghobject_t& oid)
{
  return seastar::make_ready_future<FuturizedStore::OmapIteratorRef>(
    new SeaStoreOmapIterator(
      *this,
      ch,
      oid));
}

seastar::future<std::map<uint64_t, uint64_t>> SeaStore::fiemap(
  CollectionRef ch,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  return seastar::make_ready_future<std::map<uint64_t, uint64_t>>();
}

void SeaStore::on_error(ceph::os::Transaction &t) {
  logger().error(" transaction dump:\n");
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t.dump(&f);
  f.close_section();
  std::stringstream str;
  f.flush(str);
  logger().error("{}", str.str());
  abort();
}

seastar::future<> SeaStore::do_transaction(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  return repeat_with_internal_context(
    _ch,
    std::move(_t),
    [this](auto &ctx) {
      return onode_manager->get_or_create_onodes(
	*ctx.transaction, ctx.iter.get_objects()
      ).safe_then([this, &ctx](auto &&read_onodes) {
	ctx.onodes = std::move(read_onodes);
	return crimson::do_until(
	  [this, &ctx] {
	    return _do_transaction_step(
	      ctx, ctx.ch, ctx.onodes, ctx.iter
	    ).safe_then([&ctx] {
	      return seastar::make_ready_future<bool>(!ctx.iter.have_op());
	    });
	  });
      }).safe_then([this, &ctx] {
	return onode_manager->write_dirty(*ctx.transaction, ctx.onodes);
      }).safe_then([this, &ctx] {
	return transaction_manager->submit_transaction(std::move(ctx.transaction));
      }).safe_then([&ctx]() {
	for (auto i : {
	    ctx.ext_transaction.get_on_applied(),
	    ctx.ext_transaction.get_on_commit(),
	    ctx.ext_transaction.get_on_applied_sync()}) {
	  if (i) {
	    i->complete(0);
	  }
	}
	return tm_ertr::now();
      });
    });
}

SeaStore::tm_ret SeaStore::_do_transaction_step(
  internal_context_t &ctx,
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
      return tm_ertr::now();
    case Transaction::OP_REMOVE:
    {
      return _remove(ctx, get_onode(op->oid));
    }
    break;
    case Transaction::OP_TOUCH:
    {
      return _touch(ctx, get_onode(op->oid));
    }
    break;
    case Transaction::OP_WRITE:
    {
      uint64_t off = op->off;
      uint64_t len = op->len;
      uint32_t fadvise_flags = i.get_fadvise_flags();
      ceph::bufferlist bl;
      i.decode_bl(bl);
      return _write(ctx, get_onode(op->oid), off, len, bl, fadvise_flags);
    }
    break;
    case Transaction::OP_TRUNCATE:
    {
      uint64_t off = op->off;
      return _truncate(ctx, get_onode(op->oid), off);
    }
    break;
    case Transaction::OP_SETATTR:
    {
      std::string name = i.decode_string();
      ceph::bufferlist bl;
      i.decode_bl(bl);
      std::map<std::string, bufferptr> to_set;
      to_set[name] = bufferptr(bl.c_str(), bl.length());
      return _setattrs(ctx, get_onode(op->oid), to_set);
    }
    break;
    case Transaction::OP_MKCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      return _create_collection(ctx, cid, op->split_bits);
    }
    break;
    case Transaction::OP_RMCOLL:
    {
      coll_t cid = i.get_cid(op->cid);
      return _remove_collection(ctx, cid);
    }
    break;
    case Transaction::OP_OMAP_SETKEYS:
    {
      std::map<std::string, ceph::bufferlist> aset;
      i.decode_attrset(aset);
      return _omap_set_values(ctx, get_onode(op->oid), std::move(aset));
    }
    break;
    case Transaction::OP_OMAP_SETHEADER:
    {
      ceph::bufferlist bl;
      i.decode_bl(bl);
      return _omap_set_header(ctx, get_onode(op->oid), bl);
    }
    break;
    case Transaction::OP_OMAP_RMKEYS:
    {
      omap_keys_t keys;
      i.decode_keyset(keys);
      return _omap_rmkeys(ctx, get_onode(op->oid), keys);
    }
    break;
    case Transaction::OP_OMAP_RMKEYRANGE:
    {
      string first, last;
      first = i.decode_string();
      last = i.decode_string();
      return _omap_rmkeyrange(ctx, get_onode(op->oid), first, last);
    }
    break;
    case Transaction::OP_COLL_HINT:
    {
      ceph::bufferlist hint;
      i.decode_bl(hint);
      return tm_ertr::now();
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

SeaStore::tm_ret SeaStore::_remove(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  logger().debug("{} onode={}",
                __func__, *onode);
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_touch(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  logger().debug("{} onode={}",
                __func__, *onode);
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_write(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t offset, size_t len, const ceph::bufferlist& bl,
  uint32_t fadvise_flags)
{
  logger().debug("{}: {} {} ~ {}",
                __func__, *onode, offset, len);
  assert(len == bl.length());
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_omap_set_values(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, ceph::bufferlist> &&aset)
{
  logger().debug(
    "{}: {} {} keys",
    __func__, *onode, aset.size());

  return seastar::do_with(
    BtreeOMapManager(*transaction_manager),
    onode->get_layout().omap_root.get(),
    std::move(aset),
    [&ctx, &onode, this](
      auto &omap_manager,
      auto &onode_omap_root,
      auto &keys
    ) {

      tm_ertr::future<> maybe_create_root =
	!onode_omap_root.is_null() ?
	tm_ertr::now() :
	omap_manager.initialize_omap(*ctx.transaction
	).safe_then([&onode_omap_root](auto new_root) {
	  onode_omap_root = new_root;
	});

      return maybe_create_root.safe_then([&, this] {
	return crimson::do_for_each(
	  keys.begin(),
	  keys.end(),
	  [&, this](auto &p) {
	    return omap_manager.omap_set_key(
	      onode_omap_root,
	      *ctx.transaction,
	      p.first,
	      p.second);
	  });
      }).safe_then([&, this] {
	if (onode_omap_root.must_update()) {
	  onode->get_mutable_layout(*ctx.transaction
	  ).omap_root.update(onode_omap_root);
	}
      });
    });
}

SeaStore::tm_ret SeaStore::_omap_set_header(
  internal_context_t &ctx,
  OnodeRef &onode,
  const ceph::bufferlist &header)
{
  logger().debug(
    "{}: {} {} bytes",
    __func__, *onode, header.length());
  assert(0 == "not supported yet");
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_omap_rmkeys(
  internal_context_t &ctx,
  OnodeRef &onode,
  const omap_keys_t& keys)
{
  logger().debug(
    "{} {} {} keys",
    __func__, *onode, keys.size());
  auto omap_root = onode->get_layout().omap_root.get();
  if (omap_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(*transaction_manager),
      onode->get_layout().omap_root.get(),
      [&ctx, &onode, &keys, this](
	auto &omap_manager,
	auto &omap_root) {
	return crimson::do_for_each(
	  keys.begin(),
	  keys.end(),
	  [&, this](auto &p) {
	    return omap_manager.omap_rm_key(
	      omap_root,
	      *ctx.transaction,
	      p);
	  }).safe_then([&, this] {
	    if (omap_root.must_update()) {
	      onode->get_mutable_layout(*ctx.transaction
	      ).omap_root.update(omap_root);
	    }
	  });
      });
  }
}

SeaStore::tm_ret SeaStore::_omap_rmkeyrange(
  internal_context_t &ctx,
  OnodeRef &onode,
  const std::string &first,
  const std::string &last)
{
  logger().debug(
    "{} {} first={} last={}",
    __func__, *onode, first, last);
  assert(0 == "not supported yet");
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_truncate(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t size)
{
  logger().debug("{} onode={} size={}",
                __func__, *onode, size);
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_setattrs(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string,bufferptr>& aset)
{
  logger().debug("{} onode={}",
                __func__, *onode);
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_create_collection(
  internal_context_t &ctx,
  const coll_t& cid, int bits)
{
  return transaction_manager->read_collection_root(
    *ctx.transaction
  ).safe_then([=, &ctx](auto _cmroot) {
    return seastar::do_with(
      _cmroot,
      [=, &ctx](auto &cmroot) {
	return collection_manager->create(
	  cmroot,
	  *ctx.transaction,
	  cid,
	  bits
	).safe_then([=, &ctx, &cmroot] {
	  if (cmroot.must_update()) {
	    transaction_manager->write_collection_root(
	      *ctx.transaction,
	      cmroot);
	  }
	});
      });
  }).handle_error(
    tm_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::_create_collection"
    }
  );
}

SeaStore::tm_ret SeaStore::_remove_collection(
  internal_context_t &ctx,
  const coll_t& cid)
{
  return transaction_manager->read_collection_root(
    *ctx.transaction
  ).safe_then([=, &ctx](auto _cmroot) {
    return seastar::do_with(
      _cmroot,
      [=, &ctx](auto &cmroot) {
	return collection_manager->remove(
	  cmroot,
	  *ctx.transaction,
	  cid
	).safe_then([=, &ctx, &cmroot] {
	  // param here denotes whether it already existed, probably error
	  if (cmroot.must_update()) {
	    transaction_manager->write_collection_root(
	      *ctx.transaction,
	      cmroot);
	  }
	});
      });
  }).handle_error(
    tm_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::_create_collection"
    }
  );
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
