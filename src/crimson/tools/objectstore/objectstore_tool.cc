// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <fmt/format.h>
#include <sstream>

#include "common/hobject.h"
#include "common/JSONFormatter.h"
#include "crimson/os/alienstore/alien_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "include/expected.hpp"
#include "objectstore_tool.h"
#include "osd/osd_types.h"
#include "seastar/core/future.hh"
#include "seastar/core/smp.hh"

using crimson::os::FuturizedStore;

namespace crimson {
namespace tools {
namespace kvstore {

seastar::future<> StoreTool::stop()
{
  co_await store->umount();
  co_await store->stop();
  co_return;
}

seastar::future<> StoreTool::do_gc()
{
  co_await store->do_gc();
}

seastar::future<std::vector<crimson::os::coll_core_t>> StoreTool::list_pgs()
{
  co_return co_await store->list_collections();
}

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
StoreTool::list_objects(const coll_t& cid, ghobject_t next)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, next]() -> seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return std::make_tuple(std::vector<ghobject_t>(), ghobject_t::get_max());
    }
    co_return co_await store->get_sharded_store().list_objects(
      coll, next, ghobject_t::get_max(), 100);
  });
}

seastar::future<>
StoreTool::omap_iterate(
  const coll_t &cid,
  const ghobject_t &oid,
  const std::optional<std::string> &start,
  FuturizedStore::Shard::omap_iterate_cb_t callback)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, start, callback]() -> seastar::future<>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return;
    }

    ObjectStore::omap_iter_seek_t start_from = ObjectStore::omap_iter_seek_t::min_lower_bound();
    if (start) {
      start_from = ObjectStore::omap_iter_seek_t{
        start.value(),
        ObjectStore::omap_iter_seek_t::UPPER_BOUND};
    }
    co_await store->get_sharded_store().omap_iterate(coll, oid, start_from, callback
    ).safe_then([] (auto ret) {
      ceph_assert (ret == ObjectStore::omap_iter_ret_t::NEXT);
    }).handle_error(
      crimson::os::FuturizedStore::Shard::read_errorator::assert_all("unexpected error")
    );
  });
}

seastar::future<std::string> StoreTool::get_omap(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& key)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key]() -> seastar::future<std::string>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return std::string();
    }

    std::set<std::string> to_get;
    to_get.insert(key);
    auto&& vals = co_await store->get_sharded_store().omap_get_values(
      coll, oid, to_get).handle_error(
      crimson::os::FuturizedStore::Shard::read_errorator::assert_all("unexpected error")
    );
    auto it = vals.find(key);
    if (it != vals.end()) {
      co_return it->second.to_str();
    }
    co_return std::string();
  });
}

seastar::future<bool> StoreTool::set_omap(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& key,
  const std::string& value)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key, value]() mutable
      -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }
    ceph::os::Transaction txn;
    std::map<std::string, ceph::bufferlist> omap_values;
    ceph::bufferlist bl;
    bl.append(value.c_str(), value.length());
    omap_values[key] = std::move(bl);
    txn.omap_setkeys(cid, oid, omap_values);
    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

seastar::future<bool> StoreTool::remove_omap(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& key)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }
    ceph::os::Transaction txn;
    txn.omap_rmkey(cid, oid, key);
    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}


seastar::future<std::string> StoreTool::get_omap_header(
  const coll_t& cid,
  const ghobject_t& oid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid]() -> seastar::future<std::string>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return std::string();
    }

    auto&& header = co_await store->get_sharded_store().omap_get_header(
      coll, oid).safe_then(
      [](auto&& header) {
        return seastar::make_ready_future<ceph::bufferlist>(std::move(header));
      },
      crimson::os::FuturizedStore::Shard::get_attr_errorator::all_same_way(
        [](auto&& error) {
          return seastar::make_ready_future<ceph::bufferlist>(ceph::bufferlist());
        }
      )
    );
    co_return header.to_str();
  });
}

seastar::future<bool> StoreTool::set_omap_header(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& header)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, header]() mutable
      -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }
    ceph::os::Transaction txn;
    ceph::bufferlist bl;
    bl.append(header.c_str(), header.length());
    txn.omap_setheader(cid, oid, bl);
    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}
// Object data operations
seastar::future<std::string> StoreTool::get_bytes(
  const coll_t& cid,
  const ghobject_t& oid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid]() -> seastar::future<std::string>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return std::string();
    }

    auto stat_result = co_await store->get_sharded_store().stat(coll, oid).handle_error(
      crimson::os::FuturizedStore::Shard::stat_ertr::assert_all("stat failed in get_bytes")
    );
    uint64_t total_size = stat_result.st_size;

    if (total_size == 0) {
      co_return std::string();
    }

    std::string result;
    result.reserve(total_size);

    uint64_t offset = 0;
    const uint64_t max_read = 1024 * 1024; // 1MB chunks

    while (offset < total_size) {
      uint64_t len = std::min(max_read, total_size - offset);

      auto read_result = co_await store->get_sharded_store().read(coll, oid, offset, len).safe_then(
        [](auto&& bl) -> ceph::bufferlist {
          return std::move(bl);
        },
        FuturizedStore::Shard::read_errorator::all_same_way(
          [](const std::error_code& e) -> ceph::bufferlist {
            throw std::runtime_error("read failed: " + e.message());
          })
      );

      if (read_result.length() == 0) {
        break;
      }

      result.append(read_result.c_str(), read_result.length());
      offset += read_result.length();
    }

    co_return result;
  });
}

seastar::future<bool> StoreTool::set_bytes(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& data)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, data]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    ceph::os::Transaction txn;
    txn.touch(cid, oid);
    txn.truncate(cid, oid, 0);
    ceph::bufferlist bl;
    bl.append(data.c_str(), data.length());
    txn.write(cid, oid, 0, data.length(), bl, 0);

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

// Object attribute operations
seastar::future<tl::expected<FuturizedStore::Shard::attrs_t, std::string>> StoreTool::get_attrs(
  const coll_t& cid,
  const ghobject_t& oid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid]() -> seastar::future<tl::expected<FuturizedStore::Shard::attrs_t, std::string>>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      co_return tl::make_unexpected(std::string("Failed to open collection: collection does not exist"));
    }
    using attrs_t = FuturizedStore::Shard::attrs_t;
    using ret_t = tl::expected<attrs_t, std::string>;

    co_return co_await store->get_sharded_store()
      .get_attrs(coll, oid)   // ertr::future<attrs_t>
      .safe_then(
        [](attrs_t&& a) {
          return ret_t{tl::in_place, std::move(a)};
        },
        FuturizedStore::Shard::get_attrs_ertr::all_same_way(
          [](const std::error_code& e) {
            return ret_t{tl::unexpected<std::string>(e.message())};
          })
      );
  });
}

seastar::future<tl::expected<std::string, std::string>> StoreTool::get_attr(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& key)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key]() -> seastar::future<tl::expected<std::string, std::string>>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      co_return tl::make_unexpected(std::string("Failed to open collection: collection does not exist"));
    }
    using return_type = tl::expected<std::string, std::string>;
    co_return co_await store->get_sharded_store().get_attr(coll, oid, key).safe_then(
      [](auto&& bl) {
        return return_type{tl::in_place, bl.to_str()};
      },
      FuturizedStore::Shard::get_attr_errorator::all_same_way(
        [](const std::error_code& e) {
          return return_type{tl::unexpected<std::string>(e.message())};
        }
      )
    );
  });
}

seastar::future<bool> StoreTool::set_attr(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& key,
  const std::string& value)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key, value]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    ceph::os::Transaction txn;
    ceph::bufferlist bl;
    bl.append(value.c_str(), value.length());
    txn.setattr(cid, oid, key, bl);

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

seastar::future<bool> StoreTool::remove_attr(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& key)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, key]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    ceph::os::Transaction txn;
    txn.rmattr(cid, oid, key);

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

// Object management operations
seastar::future<bool> StoreTool::remove_object(
  const coll_t& cid,
  const ghobject_t& oid,
  bool all,
  bool force)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, all, force]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    // First check if the object exists
    auto stat_result = co_await store->get_sharded_store().stat(coll, oid).handle_error(
      crimson::os::FuturizedStore::Shard::stat_ertr::all_same_way(
        []() -> struct stat {
          return {};  // zero st_nlink signals object-not-found or IO error
        })
    );
    if (stat_result.st_nlink == 0) {
      fmt::println(std::cerr, "Object {} does not exist or stat failed", oid);
      co_return false;
    }

    ceph::os::Transaction txn;

    //TODO: Implement proper snapset handling when crimson supports snapshots
    if (all) {
      fmt::println(std::cout, "removeall: removing object {} and all clones", oid);
      txn.remove(cid, oid);

    } else {
      fmt::println(std::cout, "remove: removing object {}{}", oid, force ? " (forced)" : "");

      if (oid.hobj.has_snapset() && !force) {
        fmt::println(std::cerr, "Object {} may have snapset - use removeall or force to remove", oid);
      }
      txn.remove(cid, oid);
    }

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));

    // Flush to ensure the transaction is persisted to disk before unmounting
    co_await store->get_sharded_store().flush(coll);

    fmt::println(std::cout, "Successfully removed object {}", oid);
    co_return true;
  });
}

// Object info operations
seastar::future<std::string> StoreTool::dump_object_info(
  const coll_t& cid,
  const ghobject_t& oid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid]() -> seastar::future<std::string>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return std::string();
    }

    // Read the object_info attribute
    ceph::bufferlist attr_bl;
    bool success = co_await store->get_sharded_store().get_attr(coll, oid, OI_ATTR
    ).safe_then([&attr_bl](auto&& bl) {
      attr_bl = std::move(bl);
      return seastar::make_ready_future<bool>(true);
    }).handle_error(
      FuturizedStore::Shard::get_attr_errorator::all_same_way(
        [](const std::error_code& e) {
          fmt::println(std::cerr, "Failed to get object info attribute: {}", e.message());
          return seastar::make_ready_future<bool>(false);
        }
      )
    );

    if (!success || attr_bl.length() == 0) {
      fmt::println(std::cerr, "Object info attribute is empty or not found");
      co_return std::string();
    }

    // Decode object_info_t
    object_info_t oi;
    auto bp = attr_bl.cbegin();
    try {
      decode(oi, bp);
    } catch (...) {
      fmt::println(std::cerr, "Failed to decode object info");
      co_return std::string();
    }

    // Format output using JSONFormatter
    std::ostringstream out;
    ceph::JSONFormatter formatter(true);
    formatter.open_object_section("obj");
    formatter.open_object_section("id");
    oid.dump(&formatter);
    formatter.close_section();
    formatter.open_object_section("info");
    oi.dump(&formatter);
    formatter.close_section();
    formatter.close_section();
    formatter.flush(out);

    co_return out.str();
  });
}

seastar::future<bool> StoreTool::set_object_size(
  const coll_t& cid,
  const ghobject_t& oid,
  uint64_t size)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, size]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    ceph::os::Transaction txn;
    txn.truncate(cid, oid, size);

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

seastar::future<bool> StoreTool::clear_data_digest(
  const coll_t& cid,
  const ghobject_t& oid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    ceph::os::Transaction txn;
    txn.rmattr(cid, oid, "data_digest");

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

seastar::future<bool> StoreTool::corrupt_info(
  const coll_t& cid,
  const ghobject_t& oid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    // Read the object_info attribute
    ceph::bufferlist attr_bl;
    bool success = co_await store->get_sharded_store().get_attr(coll, oid, OI_ATTR
    ).safe_then([&attr_bl](auto&& bl) {
      attr_bl = std::move(bl);
      return seastar::make_ready_future<bool>(true);
    }).handle_error(
      FuturizedStore::Shard::get_attr_errorator::all_same_way(
        [](const std::error_code& e) {
          fmt::println(std::cerr, "Failed to get object info attribute: {}", e.message());
          return seastar::make_ready_future<bool>(false);
        }
      )
    );

    if (!success || attr_bl.length() == 0) {
      fmt::println(std::cerr, "Object info attribute is empty or not found");
      co_return false;
    }

    // Decode object_info_t
    object_info_t oi;
    auto bp = attr_bl.cbegin();
    try {
      decode(oi, bp);
    } catch (...) {
      fmt::println(std::cerr, "Failed to decode object info");
      co_return false;
    }

    // Corrupt the object_info by modifying alloc_hint_flags
    oi.alloc_hint_flags += 0xff;

    // Re-encode and write back
    ceph::bufferlist new_attr_bl;
    encode(oi, new_attr_bl, -1);  // using full features

    ceph::os::Transaction txn;
    txn.setattr(cid, oid, OI_ATTR, new_attr_bl);

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

seastar::future<bool> StoreTool::clear_snapset(
  const coll_t& cid,
  const ghobject_t& oid,
  const std::string& arg)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid, oid, arg]() -> seastar::future<bool>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return false;
    }

    // Read the snapset attribute
    ceph::bufferlist attr_bl;
    bool success = co_await store->get_sharded_store().get_attr(coll, oid, SS_ATTR
    ).safe_then([&attr_bl](auto&& bl) {
      attr_bl = std::move(bl);
      return seastar::make_ready_future<bool>(true);
    }).handle_error(
      FuturizedStore::Shard::get_attr_errorator::all_same_way(
        [](const std::error_code& e) {
          fmt::println(std::cerr, "Failed to get snapset attribute: {}", e.message());
          return seastar::make_ready_future<bool>(false);
        }
      )
    );

    if (!success || attr_bl.length() == 0) {
      fmt::println(std::cerr, "Snapset attribute is empty or not found");
      co_return false;
    }

    // Decode SnapSet
    SnapSet snapset;
    auto bp = attr_bl.cbegin();
    try {
      decode(snapset, bp);
    } catch (...) {
      fmt::println(std::cerr, "Failed to decode snapset");
      co_return false;
    }

    // Selectively clear snapset fields based on arg parameter
    // This matches the behavior of ceph-objectstore-tool

    // Use "corrupt" to clear entire SnapSet
    // Use "seq" to just corrupt SnapSet.seq
    if (arg == "corrupt" || arg == "seq")
      snapset.seq = 0;
    // Use "snaps" to just clear SnapSet.clone_snaps
    if (arg == "corrupt" || arg == "snaps")
      snapset.clone_snaps.clear();
    // By default just clear clone, clone_overlap and clone_size
    std::string clear_arg = arg;
    if (arg == "corrupt")
      clear_arg = "";
    if (clear_arg == "" || clear_arg == "clones")
      snapset.clones.clear();
    if (clear_arg == "" || clear_arg == "clone_overlap")
      snapset.clone_overlap.clear();
    if (clear_arg == "" || clear_arg == "clone_size")
      snapset.clone_size.clear();
    // Break all clone sizes by adding 1
    if (clear_arg == "size") {
      for (auto& [snap, size] : snapset.clone_size) {
        ++size;
      }
    }

    // Re-encode and write back
    ceph::bufferlist new_attr_bl;
    encode(snapset, new_attr_bl);

    ceph::os::Transaction txn;
    txn.setattr(cid, oid, SS_ATTR, new_attr_bl);

    co_await store->get_sharded_store().do_transaction(coll, std::move(txn));
    co_await store->get_sharded_store().flush(coll);
    co_return true;
  });
}

// PG-level operations
seastar::future<pg_info_t> StoreTool::get_pg_info(const coll_t& cid)
{
  return seastar::smp::submit_to(
    shard_id,
    [this, cid]() -> seastar::future<pg_info_t>
  {
    auto coll = co_await store->get_sharded_store().open_collection(cid
    ).handle_exception([](std::exception_ptr) {
      return seastar::make_ready_future<FuturizedStore::Shard::CollectionRef>(nullptr);
    });
    if (!coll) {
      fmt::println(std::cerr, "Failed to open collection: collection does not exist");
      co_return pg_info_t();
    }

    pg_info_t info;
    spg_t pgid;
    if (cid.is_pg(&pgid)) {
      info.pgid = pgid;
    }
    co_return info;
  });
}

} // namespace kvstore
} // namespace tools
} // namespace crimson
