// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastore.h"

#include <algorithm>

#include <boost/algorithm/string/trim.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/shared_mutex.hh>

#include "common/safe_io.h"
#include "os/Transaction.h"

#include "crimson/common/buffer_io.h"

#include "crimson/os/futurized_collection.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/onode_manager.h"
#include "crimson/os/seastore/object_data_handler.h"

using crimson::common::local_conf;

namespace crimson::os::seastore {

SeaStore::SeaStore(
  SegmentManagerRef sm,
  TransactionManagerRef tm,
  CollectionManagerRef cm,
  OnodeManagerRef om)
  : segment_manager(std::move(sm)),
    transaction_manager(std::move(tm)),
    collection_manager(std::move(cm)),
    onode_manager(std::move(om))
{}

SeaStore::~SeaStore() = default;

seastar::future<> SeaStore::stop()
{
  return seastar::now();
}

seastar::future<> SeaStore::mount()
{
  return segment_manager->mount(
  ).safe_then([this] {
    return transaction_manager->mount();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::mount"
    }
  );
}

seastar::future<> SeaStore::umount()
{
  return transaction_manager->close(
  ).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::umount"
    }
  );
}

seastar::future<> SeaStore::mkfs(uuid_d new_osd_fsid)
{
  return segment_manager->mkfs(
    seastore_meta_t{new_osd_fsid}
  ).safe_then([this] {
    return segment_manager->mount();
  }).safe_then([this] {
    return transaction_manager->mkfs();
  }).safe_then([this] {
    return transaction_manager->mount();
  }).safe_then([this] {
    return seastar::do_with(
      transaction_manager->create_transaction(),
      [this](auto &t) {
	return onode_manager->mkfs(*t
	).safe_then([this, &t] {
	  return with_trans_intr(
	    *t,
	    [this](auto &t) {
	      return collection_manager->mkfs(t);
	    });
	}).safe_then([this, &t](auto coll_root) {
	  transaction_manager->write_collection_root(
	    *t,
	    coll_root);
	  return transaction_manager->submit_transaction(
	    *t);
	});
      });
  }).safe_then([this] {
    return umount();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid error in SeaStore::mkfs"
    }
  );
}

seastar::future<store_statfs_t> SeaStore::stat() const
{
  LOG_PREFIX(SeaStore::stat);
  DEBUG("");
  return seastar::make_ready_future<store_statfs_t>(
    transaction_manager->store_stat()
  );
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
SeaStore::list_objects(CollectionRef ch,
                        const ghobject_t& start,
                        const ghobject_t& end,
                        uint64_t limit) const
{
  using RetType = typename OnodeManager::list_onodes_bare_ret;
  return seastar::do_with(
      RetType(),
      [this, start, end, limit] (auto& ret) {
    return repeat_eagain2([this, start, end, limit, &ret] {
      return seastar::do_with(
          transaction_manager->create_transaction(),
          [this, start, end, limit, &ret] (auto& t) {
        return onode_manager->list_onodes(*t, start, end, limit
        ).safe_then([&ret] (auto&& _ret) {
          ret = std::move(_ret);
        });
      });
    }).then([&ret] {
      return std::move(ret);
    });
  });
}

seastar::future<CollectionRef> SeaStore::create_new_collection(const coll_t& cid)
{
  LOG_PREFIX(SeaStore::create_new_collection);
  DEBUG("{}", cid);
  return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
}

seastar::future<CollectionRef> SeaStore::open_collection(const coll_t& cid)
{
  LOG_PREFIX(SeaStore::open_collection);
  DEBUG("{}", cid);
  return list_collections().then([cid, this] (auto colls) {
    if (auto found = std::find(colls.begin(), colls.end(), cid);
	found != colls.end()) {
      return seastar::make_ready_future<CollectionRef>(_get_collection(cid));
    } else {
      return seastar::make_ready_future<CollectionRef>();
    }
  });
}

seastar::future<std::vector<coll_t>> SeaStore::list_collections()
{
  return seastar::do_with(
    std::vector<coll_t>(),
    [this](auto &ret) {
      return repeat_eagain([this, &ret] {

	return seastar::do_with(
	  transaction_manager->create_transaction(),
	  [this, &ret](auto &t) {
	    return transaction_manager->read_collection_root(*t
	    ).safe_then([this, &t](auto coll_root) {
	      return with_trans_intr(
		*t,
		[this, &coll_root](auto &t) {
		  return collection_manager->list(
		    coll_root,
		    t);
		});
	    }).safe_then([&ret](auto colls) {
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
  LOG_PREFIX(SeaStore::read);
  DEBUG("oid {} offset {} len {}", oid, offset, len);
  return repeat_with_onode<ceph::bufferlist>(
    ch,
    oid,
    [=](auto &t, auto &onode) -> ObjectDataHandler::read_ret {
      size_t size = onode.get_layout().size;

      if (offset >= size) {
	return seastar::make_ready_future<ceph::bufferlist>();
      }

      size_t corrected_len = (len == 0) ?
	size - offset :
	std::min(size - offset, len);

      return ObjectDataHandler().read(
	ObjectDataHandler::context_t{
	  *transaction_manager,
	  t,
	  onode,
	},
	offset,
	corrected_len);
    });
}

SeaStore::read_errorator::future<ceph::bufferlist> SeaStore::readv(
  CollectionRef ch,
  const ghobject_t& oid,
  interval_set<uint64_t>& m,
  uint32_t op_flags)
{
  return read_errorator::make_ready_future<ceph::bufferlist>();
}

using crimson::os::seastore::omap_manager::BtreeOMapManager;

SeaStore::get_attr_errorator::future<ceph::bufferlist> SeaStore::get_attr(
  CollectionRef ch,
  const ghobject_t& oid,
  std::string_view name) const
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  LOG_PREFIX(SeaStore::get_attr);
  DEBUG("{} {}", c->get_cid(), oid);
  return repeat_with_onode<ceph::bufferlist>(
    c, oid, [=](auto &t, auto& onode)
    -> _omap_get_value_ertr::future<ceph::bufferlist> {
    auto& layout = onode.get_layout();
    if (name == OI_ATTR && layout.oi_size) {
      ceph::bufferlist bl;
      bl.append(ceph::bufferptr(&layout.oi[0], layout.oi_size));
      return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
    }
    if (name == SS_ATTR && layout.ss_size) {
      ceph::bufferlist bl;
      bl.append(ceph::bufferptr(&layout.ss[0], layout.ss_size));
      return seastar::make_ready_future<ceph::bufferlist>(std::move(bl));
    }
    return _omap_get_value(
      t,
      layout.xattr_root.get(),
      name);
  }).handle_error(crimson::ct_error::input_output_error::handle([FNAME] {
    ERROR("EIO when getting attrs");
    abort();
  }), crimson::ct_error::pass_further_all{});
}

SeaStore::get_attrs_ertr::future<SeaStore::attrs_t> SeaStore::get_attrs(
  CollectionRef ch,
  const ghobject_t& oid)
{
  LOG_PREFIX(SeaStore::get_attrs);
  auto c = static_cast<SeastoreCollection*>(ch.get());
  DEBUG("{} {}", c->get_cid(), oid);
  return repeat_with_onode<attrs_t>(
    c, oid, [=](auto &t, auto& onode) {
    auto& layout = onode.get_layout();
    return _omap_list(layout.xattr_root, t, std::nullopt,
      OMapManager::omap_list_config_t::with_inclusive(false)
    ).safe_then([&layout](auto p) {
      auto& attrs = std::get<1>(p);
      ceph::bufferlist bl;
      if (layout.oi_size) {
        bl.append(ceph::bufferptr(&layout.oi[0], layout.oi_size));
        attrs.emplace(OI_ATTR, std::move(bl));
      }
      if (layout.ss_size) {
        bl.clear();
        bl.append(ceph::bufferptr(&layout.ss[0], layout.ss_size));
        attrs.emplace(SS_ATTR, std::move(bl));
      }
      return seastar::make_ready_future<omap_values_t>(std::move(attrs));
    });
  }).handle_error(crimson::ct_error::input_output_error::handle([FNAME] {
    ERROR("EIO when getting attrs");
    abort();
  }), crimson::ct_error::pass_further_all{});
}

seastar::future<struct stat> SeaStore::stat(
  CollectionRef c,
  const ghobject_t& oid)
{
  LOG_PREFIX(SeaStore::stat);
  return repeat_with_onode<struct stat>(
    c,
    oid,
    [=, &oid](auto &t, auto &onode) {
      struct stat st;
      auto &olayout = onode.get_layout();
      st.st_size = olayout.size;
      st.st_blksize = transaction_manager->get_block_size();
      st.st_blocks = (st.st_size + st.st_blksize - 1) / st.st_blksize;
      st.st_nlink = 1;
      DEBUGT("cid {}, oid {}, return size {}", t, c->get_cid(), oid, st.st_size);
      return seastar::make_ready_future<struct stat>(st);
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid error in SeaStore::stat"
       }
    );
}

auto
SeaStore::omap_get_header(
  CollectionRef c,
  const ghobject_t& oid)
  -> read_errorator::future<bufferlist>
{
  return seastar::make_ready_future<bufferlist>();
}

SeaStore::read_errorator::future<SeaStore::omap_values_t>
SeaStore::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const omap_keys_t &keys)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  return repeat_with_onode<omap_values_t>(
    c,
    oid,
    [this, keys](auto &t, auto &onode) {
      omap_root_t omap_root = onode.get_layout().omap_root.get();
      return _omap_get_values(
	t,
	std::move(omap_root),
	keys);
    });
}

SeaStore::_omap_get_value_ret SeaStore::_omap_get_value(
  Transaction &t,
  omap_root_t &&root,
  std::string_view key) const
{
  return seastar::do_with(
    BtreeOMapManager(transaction_manager->get_tm()),
    std::move(root),
    std::string(key),
    [&t](auto &manager, auto& root, auto& key) -> _omap_get_value_ret {
      if (root.is_null()) {
	return crimson::ct_error::enodata::make();
      }
      return with_trans_intr(
	t,
	[&](auto &t) {
	  return manager.omap_get_value(
	    root, t, key
	  );
	}).safe_then([](auto opt) -> _omap_get_value_ret {
	  if (!opt) {
	    return crimson::ct_error::enodata::make();
	  }
	  return seastar::make_ready_future<ceph::bufferlist>(std::move(*opt));
	});
    });
}

SeaStore::_omap_get_values_ret SeaStore::_omap_get_values(
  Transaction &t,
  omap_root_t &&omap_root,
  const omap_keys_t &keys) const
{
  if (omap_root.is_null()) {
    return seastar::make_ready_future<omap_values_t>();
  }
  return seastar::do_with(
    BtreeOMapManager(transaction_manager->get_tm()),
    std::move(omap_root),
    omap_values_t(),
    [&](auto &manager, auto &root, auto &ret) {
      return with_trans_intr(
	t,
	[&](auto &t) {
	  return trans_intr::do_for_each(
	    keys.begin(),
	    keys.end(),
	    [&](auto &key) {
	      return manager.omap_get_value(
		root,
		t,
		key
	      ).si_then([&ret, &key](auto &&p) {
		if (p) {
		  bufferlist bl;
		  bl.append(*p);
		  ret.emplace(
		    std::move(key),
		    std::move(bl));
		}
		return seastar::now();
	      });
	    }).si_then([&ret] {
	      return std::move(ret);
	    });
	});
    });
}

SeaStore::_omap_list_ret SeaStore::_omap_list(
  const omap_root_le_t& omap_root,
  Transaction& t,
  const std::optional<std::string>& start,
  OMapManager::omap_list_config_t config) const
{
  auto root = omap_root.get();
  if (root.is_null()) {
    return seastar::make_ready_future<_omap_list_bare_ret>(
      true, omap_values_t{}
    );
  }
  return seastar::do_with(
    BtreeOMapManager(transaction_manager->get_tm()),
    root,
    start,
    [&t, config](auto &manager, auto& root, auto& start) {
      return with_trans_intr(
	t,
	[&](auto &t) {
	  return manager.omap_list(root, t, start, config);
	});
    });
}

SeaStore::omap_get_values_ret_t SeaStore::omap_list(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<string> &start,
  OMapManager::omap_list_config_t config)
{
  auto c = static_cast<SeastoreCollection*>(ch.get());
  LOG_PREFIX(SeaStore::omap_list);
  DEBUG("{} {}", c->get_cid(), oid);
  using ret_bare_t = std::tuple<bool, SeaStore::omap_values_t>;
  return repeat_with_onode<ret_bare_t>(
    c,
    oid,
    [this, config, &start](auto &t, auto &onode) {
      return _omap_list(
	onode.get_layout().omap_root,
	t, start, config
      );
    });
}

SeaStore::omap_get_values_ret_t SeaStore::omap_get_values(
  CollectionRef ch,
  const ghobject_t &oid,
  const std::optional<string> &start)
{
  return seastar::do_with(oid, start,
			  [this, ch=std::move(ch)](auto& oid, auto& start) {
    return omap_list(
      ch, oid, start,
      OMapManager::omap_list_config_t::with_inclusive(false));
  });
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
  LOG_PREFIX(SeaStore::on_error);
  ERROR(" transaction dump:\n");
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t.dump(&f);
  f.close_section();
  std::stringstream str;
  f.flush(str);
  ERROR("{}", str.str());
  abort();
}

seastar::future<> SeaStore::do_transaction(
  CollectionRef _ch,
  ceph::os::Transaction&& _t)
{
  // repeat_with_internal_context ensures ordering via collection lock
  return repeat_with_internal_context(
    _ch,
    std::move(_t),
    [this](auto &ctx) {
      return onode_manager->get_or_create_onodes(
	*ctx.transaction, ctx.iter.get_objects()
      ).safe_then([this, &ctx](auto &&read_onodes) {
	ctx.onodes = std::move(read_onodes);
	return crimson::repeat(
	  [this, &ctx]() -> tm_ertr::future<seastar::stop_iteration> {
	    if (ctx.iter.have_op()) {
	      return _do_transaction_step(
		ctx, ctx.ch, ctx.onodes, ctx.iter
	      ).safe_then([] {
		return seastar::make_ready_future<seastar::stop_iteration>(
		  seastar::stop_iteration::no);
	      });
	    } else {
	      return seastar::make_ready_future<seastar::stop_iteration>(
		seastar::stop_iteration::yes);
	    };
	  });
      }).safe_then([this, &ctx] {
	return onode_manager->write_dirty(*ctx.transaction, ctx.onodes);
      }).safe_then([this, &ctx] {
        // There are some validations in onode tree during onode value
        // destruction in debug mode, which need to be done before calling
        // submit_transaction().
        ctx.onodes.clear();
	return transaction_manager->submit_transaction(*ctx.transaction);
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
  LOG_PREFIX(SeaStore::_do_transaction_step);
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
      return _write(
	ctx, get_onode(op->oid), off, len, std::move(bl),
	fadvise_flags);
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
      std::map<std::string, bufferlist> to_set;
      ceph::bufferlist& bl = to_set[name];
      i.decode_bl(bl);
      return _setattrs(ctx, get_onode(op->oid), std::move(to_set));
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
      return _omap_set_header(ctx, get_onode(op->oid), std::move(bl));
    }
    break;
    case Transaction::OP_OMAP_RMKEYS:
    {
      omap_keys_t keys;
      i.decode_keyset(keys);
      return _omap_rmkeys(ctx, get_onode(op->oid), std::move(keys));
    }
    break;
    case Transaction::OP_OMAP_RMKEYRANGE:
    {
      string first, last;
      first = i.decode_string();
      last = i.decode_string();
      return _omap_rmkeyrange(
	ctx, get_onode(op->oid),
	std::move(first), std::move(last));
    }
    break;
    case Transaction::OP_COLL_HINT:
    {
      ceph::bufferlist hint;
      i.decode_bl(hint);
      return tm_ertr::now();
    }
    default:
      ERROR("bad op {}", static_cast<unsigned>(op->op));
      return crimson::ct_error::input_output_error::make();
    }
  } catch (std::exception &e) {
    ERROR("got exception {}", e);
    return crimson::ct_error::input_output_error::make();
  }
}

SeaStore::tm_ret SeaStore::_remove(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStore::_remove);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  return onode_manager->erase_onode(*ctx.transaction, onode);
}

SeaStore::tm_ret SeaStore::_touch(
  internal_context_t &ctx,
  OnodeRef &onode)
{
  LOG_PREFIX(SeaStore::_touch);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_write(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t offset, size_t len,
  ceph::bufferlist &&_bl,
  uint32_t fadvise_flags)
{
  LOG_PREFIX(SeaStore::_write);
  DEBUGT("onode={} {}~{}", *ctx.transaction, *onode, offset, len);
  {
    auto &object_size = onode->get_mutable_layout(*ctx.transaction).size;
    object_size = std::max<uint64_t>(
      offset + len,
      object_size);
  }
  return seastar::do_with(
    std::move(_bl),
    [=, &ctx, &onode](auto &bl) {
      return ObjectDataHandler().write(
	ObjectDataHandler::context_t{
	  *transaction_manager,
	  *ctx.transaction,
	  *onode,
	},
	offset,
	bl);
    });
}

SeaStore::omap_set_kvs_ret
SeaStore::_omap_set_kvs(
  const omap_root_le_t& omap_root,
  Transaction& t,
  omap_root_le_t& mutable_omap_root,
  std::map<std::string, ceph::bufferlist>&& kvs)
{
  return seastar::do_with(
    BtreeOMapManager(transaction_manager->get_tm()),
    omap_root.get(),
    [&, keys=std::move(kvs)](auto &omap_manager, auto &root) {
      return with_trans_intr(
	t,
	[&](auto &t) {
	  tm_iertr::future<> maybe_create_root =
	    !root.is_null() ?
	    tm_iertr::now() :
	    omap_manager.initialize_omap(
	      t
	    ).si_then([&root](auto new_root) {
	      root = new_root;
	    });
	  return maybe_create_root.si_then(
	    [&, keys=std::move(keys)]() mutable {
	      return omap_manager.omap_set_keys(root, t, std::move(keys));
	    }).si_then([&] {
	      return tm_iertr::make_ready_future<omap_root_t>(std::move(root));
	    }).si_then([&mutable_omap_root](auto root) {
	      if (root.must_update()) {
		mutable_omap_root.update(root);
	      }
	    });
	});
    });
}

SeaStore::tm_ret SeaStore::_omap_set_values(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, ceph::bufferlist> &&aset)
{
  LOG_PREFIX(SeaStore::_omap_set_values);
  DEBUGT("{} {} keys", *ctx.transaction, *onode, aset.size());
  return _omap_set_kvs(
    onode->get_layout().omap_root,
    *ctx.transaction,
    onode->get_mutable_layout(*ctx.transaction).omap_root,
    std::move(aset));
}

SeaStore::tm_ret SeaStore::_omap_set_header(
  internal_context_t &ctx,
  OnodeRef &onode,
  ceph::bufferlist &&header)
{
  LOG_PREFIX(SeaStore::_omap_set_header);
  DEBUGT("{} {} bytes", *ctx.transaction, *onode, header.length());
  assert(0 == "not supported yet");
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_omap_rmkeys(
  internal_context_t &ctx,
  OnodeRef &onode,
  omap_keys_t &&keys)
{
  LOG_PREFIX(SeaStore::_omap_rmkeys);
  DEBUGT("{} {} keys", *ctx.transaction, *onode, keys.size());
  auto omap_root = onode->get_layout().omap_root.get();
  if (omap_root.is_null()) {
    return seastar::now();
  } else {
    return seastar::do_with(
      BtreeOMapManager(transaction_manager->get_tm()),
      onode->get_layout().omap_root.get(),
      std::move(keys),
      [&ctx, &onode](
	auto &omap_manager,
	auto &omap_root,
	auto &keys) {
	return with_trans_intr(
	  *ctx.transaction,
	  [&](auto &t) {
	    return trans_intr::do_for_each(
	      keys.begin(),
	      keys.end(),
	      [&](auto &p) {
		return omap_manager.omap_rm_key(
		  omap_root,
		  *ctx.transaction,
		  p);
	      }).si_then([&] {
		if (omap_root.must_update()) {
		  onode->get_mutable_layout(*ctx.transaction
		  ).omap_root.update(omap_root);
		}
	      });
	  });
      });
  }
}

SeaStore::tm_ret SeaStore::_omap_rmkeyrange(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::string first,
  std::string last)
{
  LOG_PREFIX(SeaStore::_omap_rmkeyrange);
  DEBUGT("{} first={} last={}", *ctx.transaction, *onode, first, last);
  assert(0 == "not supported yet");
  return tm_ertr::now();
}

SeaStore::tm_ret SeaStore::_truncate(
  internal_context_t &ctx,
  OnodeRef &onode,
  uint64_t size)
{
  LOG_PREFIX(SeaStore::_truncate);
  DEBUGT("onode={} size={}", *ctx.transaction, *onode, size);
  onode->get_mutable_layout(*ctx.transaction).size = size;
  return ObjectDataHandler().truncate(
    ObjectDataHandler::context_t{
      *transaction_manager,
      *ctx.transaction,
      *onode
    },
    size);
}

SeaStore::tm_ret SeaStore::_setattrs(
  internal_context_t &ctx,
  OnodeRef &onode,
  std::map<std::string, bufferlist>&& aset)
{
  LOG_PREFIX(SeaStore::_setattrs);
  DEBUGT("onode={}", *ctx.transaction, *onode);
  auto& layout = onode->get_mutable_layout(*ctx.transaction);
  if (auto it = aset.find(OI_ATTR); it != aset.end()) {
    auto& val = it->second;
    if (likely(val.length() <= onode_layout_t::MAX_OI_LENGTH)) {
      layout.oi_size = val.length();
      maybe_inline_memcpy(
	&layout.oi[0],
	val.c_str(),
	val.length(),
	onode_layout_t::MAX_OI_LENGTH);
      aset.erase(it);
    } else {
      layout.oi_size = 0;
    }
  }

  if (auto it = aset.find(SS_ATTR); it != aset.end()) {
    auto& val = it->second;
    if (likely(val.length() <= onode_layout_t::MAX_SS_LENGTH)) {
      layout.ss_size = val.length();
      maybe_inline_memcpy(
	&layout.ss[0],
	val.c_str(),
	val.length(),
	onode_layout_t::MAX_SS_LENGTH);
      it = aset.erase(it);
    } else {
      layout.ss_size = 0;
    }
  }

  if (aset.empty()) {
    return tm_ertr::now();
  }

  return _omap_set_kvs(
    onode->get_layout().xattr_root,
    *ctx.transaction,
    layout.xattr_root,
    std::move(aset));
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
	return with_trans_intr(
	  *ctx.transaction,
	  [=, &cmroot](auto &t) {
	    return collection_manager->create(
	      cmroot,
	      t,
	      cid,
	      bits);
	  }).safe_then([=, &ctx, &cmroot] {
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
	return with_trans_intr(
	  *ctx.transaction,
	  [=, &cmroot](auto &t) {
	    return collection_manager->remove(
	      cmroot,
	      t,
	      cid);
	  }).safe_then([=, &ctx, &cmroot] {
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
  LOG_PREFIX(SeaStore::write_meta);
  DEBUG("key: {}; value: {}", key, value);
  return seastar::do_with(
    TransactionRef(),
    key,
    value,
    [this, FNAME](auto &t, auto& key, auto& value) {
      return repeat_eagain([this, FNAME, &t, &key, &value] {
	t = transaction_manager->create_transaction();
	DEBUGT("Have transaction, key: {}; value: {}", *t, key, value);
        return transaction_manager->update_root_meta(
	  *t, key, value
	).safe_then([this, &t] {
	  return transaction_manager->submit_transaction(*t);
	});
      });
    }).handle_error(
      crimson::ct_error::assert_all{"Invalid error in SeaStore::write_meta"}
    );
}

seastar::future<std::tuple<int, std::string>> SeaStore::read_meta(const std::string& key)
{
  LOG_PREFIX(SeaStore::read_meta);
  DEBUG("key: {}", key);
  return seastar::do_with(
    std::tuple<int, std::string>(),
    TransactionRef(),
    key,
    [this](auto &ret, auto &t, auto& key) {
      return repeat_eagain([this, &ret, &t, &key] {
	t = transaction_manager->create_transaction();
	return transaction_manager->read_root_meta(
	  *t, key
	).safe_then([&ret](auto v) {
	  if (v) {
	    ret = std::make_tuple(0, std::move(*v));
	  } else {
	    ret = std::make_tuple(-1, std::string(""));
	  }
	});
      }).safe_then([&ret] {
	return std::move(ret);
      });
    }).handle_error(
      crimson::ct_error::assert_all{"Invalid error in SeaStore::read_meta"}
    );
}

uuid_d SeaStore::get_fsid() const
{
  return segment_manager->get_meta().seastore_id;
}

std::unique_ptr<SeaStore> make_seastore(
  const std::string &device,
  const ConfigValues &config)
{
  auto sm = std::make_unique<
    segment_manager::block::BlockSegmentManager
    >(device + "/block");

  auto segment_cleaner = std::make_unique<SegmentCleaner>(
    SegmentCleaner::config_t::get_default(),
    false /* detailed */);

  auto journal = std::make_unique<Journal>(*sm);
  auto cache = std::make_unique<Cache>(*sm);
  auto lba_manager = lba_manager::create_lba_manager(*sm, *cache);

  journal->set_segment_provider(&*segment_cleaner);

  auto tm = std::make_unique<TransactionManager>(
    *sm,
    std::move(segment_cleaner),
    std::move(journal),
    std::move(cache),
    std::move(lba_manager));

  auto cm = std::make_unique<collection_manager::FlatCollectionManager>(*tm);
  return std::make_unique<SeaStore>(
    std::move(sm),
    std::move(tm),
    std::move(cm),
    std::make_unique<crimson::os::seastore::onode::FLTreeOnodeManager>(*tm));
}

}
