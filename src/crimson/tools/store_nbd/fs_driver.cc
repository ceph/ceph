// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>
#include <fmt/format.h>

#include "os/Transaction.h"
#include "fs_driver.h"

using namespace crimson;
using namespace crimson::os;

coll_t get_coll(unsigned num) {
  return coll_t(spg_t(pg_t(0, num)));
}

ghobject_t get_log_object(unsigned coll)
{
  return ghobject_t(
    shard_id_t::NO_SHARD,
    0,
    (coll << 16),
    "",
    "",
    0,
    ghobject_t::NO_GEN);
}

std::string make_log_key(
  unsigned i)
{
  return fmt::format("log_entry_{}", i);
}

void add_log_entry(
  unsigned i,
  unsigned entry_size,
  std::map<std::string, ceph::buffer::list> *omap)
{
  assert(omap);
  bufferlist bl;
  bl.append(ceph::buffer::create('0', entry_size));

  omap->emplace(std::make_pair(make_log_key(i), bl));
}

void populate_log(
  ceph::os::Transaction &t,
  FSDriver::pg_analogue_t &pg,
  unsigned entry_size,
  unsigned entries)
{
  t.touch(pg.collection->get_cid(), pg.log_object);
  // omap_clear not yet implemented, TODO
  // t.omap_clear(pg.collection->get_cid(), pg.log_object);

  std::map<std::string, ceph::buffer::list> omap;
  for (unsigned i = 0; i < entries; ++i) {
    add_log_entry(i, entry_size, &omap);
  }

  t.omap_setkeys(
    pg.collection->get_cid(),
    pg.log_object,
    omap);

  pg.log_head = entries;
}

void update_log(
  ceph::os::Transaction &t,
  FSDriver::pg_analogue_t &pg,
  unsigned entry_size,
  unsigned entries)
{
  ++pg.log_head;
  std::map<std::string, ceph::buffer::list> key;
  add_log_entry(pg.log_head, entry_size, &key);

  t.omap_setkeys(
    pg.collection->get_cid(),
    pg.log_object,
    key);


  while ((pg.log_head - pg.log_tail) > entries) {
    t.omap_rmkey(
      pg.collection->get_cid(),
      pg.log_object,
      make_log_key(pg.log_tail));
    ++pg.log_tail;
  }
}

FSDriver::offset_mapping_t FSDriver::map_offset(off_t offset)
{
  uint32_t objid = offset / config.object_size;
  uint32_t collid = objid % config.num_pgs;
  return offset_mapping_t{
    collections[collid],
    ghobject_t(
      shard_id_t::NO_SHARD,
      0,
      (collid << 16) | (objid + 1),
      "",
      "",
      0,
      ghobject_t::NO_GEN),
    offset % config.object_size
  };
}

seastar::future<> FSDriver::write(
  off_t offset,
  bufferptr ptr)
{
  auto mapping = map_offset(offset);
  ceph_assert(mapping.offset + ptr.length() <= config.object_size);
  ceph::os::Transaction t;
  bufferlist bl;
  bl.append(ptr);
  t.write(
    mapping.pg.collection->get_cid(),
    mapping.object,
    mapping.offset,
    ptr.length(),
    bl,
    0);

  if (config.oi_enabled() ) {
    bufferlist attr;
    attr.append(ceph::buffer::create(config.oi_size, '0'));
    t.setattr(
      mapping.pg.collection->get_cid(),
      mapping.object,
      "_",
      attr);
  }

  if (config.log_enabled()) {
    update_log(
      t,
      mapping.pg,
      config.log_entry_size,
      config.log_size);
  }

  return sharded_fs->do_transaction(
    mapping.pg.collection,
    std::move(t));
}

seastar::future<bufferlist> FSDriver::read(
  off_t offset,
  size_t size)
{
  auto mapping = map_offset(offset);
  ceph_assert((mapping.offset + size) <= config.object_size);
  return sharded_fs->read(
    mapping.pg.collection,
    mapping.object,
    mapping.offset,
    size,
    0
  ).handle_error(
    crimson::ct_error::enoent::handle([size](auto &e) {
      bufferlist bl;
      bl.append_zero(size);
      return seastar::make_ready_future<bufferlist>(std::move(bl));
    }),
    crimson::ct_error::assert_all{"Unrecoverable error in FSDriver::read"}
  ).then([size](auto &&bl) {
    if (bl.length() < size) {
      bl.append_zero(size - bl.length());
    }
    return seastar::make_ready_future<bufferlist>(std::move(bl));
  });
}

seastar::future<> FSDriver::mkfs()
{
  return init(
  ).then([this] {
    assert(fs);
    uuid_d uuid;
    uuid.generate_random();
    return fs->mkfs(uuid).handle_error(
      crimson::stateful_ec::handle([] (const auto& ec) {
        crimson::get_logger(ceph_subsys_test)
          .error("error creating empty object store in {}: ({}) {}",
          crimson::common::local_conf().get_val<std::string>("osd_data"),
          ec.value(), ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([this] {
    return fs->stop();
  }).then([this] {
    return init();
  }).then([this] {
    return fs->mount(
    ).handle_error(
      crimson::stateful_ec::handle([] (const auto& ec) {
        crimson::get_logger(
	  ceph_subsys_test
	).error(
	  "error mounting object store in {}: ({}) {}",
	  crimson::common::local_conf().get_val<std::string>("osd_data"),
	  ec.value(),
	  ec.message());
	std::exit(EXIT_FAILURE);
      }));
  }).then([this] {
    return seastar::do_for_each(
      boost::counting_iterator<unsigned>(0),
      boost::counting_iterator<unsigned>(config.num_pgs),
      [this](auto i) {
	return sharded_fs->create_new_collection(get_coll(i)
	).then([this, i](auto coll) {
	  ceph::os::Transaction t;
	  t.create_collection(get_coll(i), 0);
	  return sharded_fs->do_transaction(coll, std::move(t));
	});
      });
  }).then([this] {
    return fs->umount();
  }).then([this] {
    return fs->stop();
  }).then([this] {
    fs.reset();
    return seastar::now();
  });
}

seastar::future<> FSDriver::mount()
{
  ceph_assert(config.path);
  return (
    config.mkfs ? mkfs() : seastar::now()
  ).then([this] {
    return init();
  }).then([this] {
    return fs->mount(
    ).handle_error(
      crimson::stateful_ec::handle([] (const auto& ec) {
        crimson::get_logger(
	  ceph_subsys_test
	).error(
	  "error mounting object store in {}: ({}) {}",
	  crimson::common::local_conf().get_val<std::string>("osd_data"),
	  ec.value(),
	  ec.message());
        std::exit(EXIT_FAILURE);
      }));
  }).then([this] {
    return seastar::do_for_each(
      boost::counting_iterator<unsigned>(0),
      boost::counting_iterator<unsigned>(config.num_pgs),
      [this](auto i) {
	return sharded_fs->open_collection(get_coll(i)
	).then([this, i](auto ref) {
	  collections[i].collection = ref;
	  collections[i].log_object = get_log_object(i);
	  if (config.log_enabled()) {
	    ceph::os::Transaction t;
	    if (config.prepopulate_log_enabled()) {
	      populate_log(
		t,
		collections[i],
		config.log_entry_size,
		config.log_size);
	    }
	    return sharded_fs->do_transaction(
	      collections[i].collection,
	      std::move(t));
	  } else {
	    return seastar::now();
	  }
	});
      });
  }).then([this] {
    return fs->stat();
  }).then([this](auto s) {
    size = s.total;
  });
};

seastar::future<> FSDriver::close()
{
  collections.clear();
  return fs->umount(
  ).then([this] {
    return fs->stop();
  }).then([this] {
    fs.reset();
    return seastar::now();
  });
}

seastar::future<> FSDriver::init()
{
  fs.reset();
  fs = FuturizedStore::create(
    config.get_fs_type(),
    *config.path,
    crimson::common::local_conf().get_config_values()
  );
  return fs->start().then([this] {
    sharded_fs = &(fs->get_sharded_store());
  });
}
