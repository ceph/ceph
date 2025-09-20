// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include <bit>
#include "StupidAllocator.h"
#include "BitmapAllocator.h"
#include "AvlAllocator.h"
#include "BtreeAllocator.h"
#include "Btree2Allocator.h"
#include "HybridAllocator.h"
#include "common/debug.h"
#include "common/admin_socket.h"
#include "AllocatorBase.h"

#define dout_subsys ceph_subsys_bluestore
using TOPNSPC::common::cmd_getval;

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;

class AllocatorBase::SocketHook : public AdminSocketHook {
  AllocatorBase *alloc;

  friend class AllocatorBase;
  std::string name;
public:
  SocketHook(AllocatorBase *alloc, std::string_view _name) :
    alloc(alloc), name(_name)
  {
    AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
    if (name.empty()) {
      name = to_string((uintptr_t)this);
    }
    if (admin_socket) {
      int r = admin_socket->register_command(
	("bluestore allocator dump " + name).c_str(),
	this,
	"dump allocator free regions");
      if (r != 0)
        alloc = nullptr; //some collision, disable
      if (alloc) {
        r = admin_socket->register_command(
	  ("bluestore allocator score " + name).c_str(),
	  this,
	  "give score on allocator fragmentation (0-no fragmentation, 1-absolute fragmentation)");
        ceph_assert(r == 0);
        r = admin_socket->register_command(
          ("bluestore allocator fragmentation " + name).c_str(),
          this,
          "give allocator fragmentation (0-no fragmentation, 1-absolute fragmentation)");
        ceph_assert(r == 0);
        r = admin_socket->register_command(
	  ("bluestore allocator fragmentation histogram " + name +
           " name=alloc_unit,type=CephInt,req=false" +
           " name=num_buckets,type=CephInt,req=false").c_str(),
	  this,
	  "build allocator free regions state histogram");
        ceph_assert(r == 0);
      }
    }
  }
  ~SocketHook()
  {
    AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
    if (admin_socket && alloc) {
      admin_socket->unregister_commands(this);
    }
  }

  int call(std::string_view command,
	   const cmdmap_t& cmdmap,
	   const bufferlist&,
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& out) override {
    int r = 0;
    if (command == "bluestore allocator dump " + name) {
      f->open_object_section("allocator_dump");
      f->dump_unsigned("capacity", alloc->get_capacity());
      f->dump_unsigned("alloc_unit", alloc->get_block_size());
      f->dump_string("alloc_type", alloc->get_type());
      f->dump_string("alloc_name", name);

      f->open_array_section("extents");
      auto iterated_allocation = [&](size_t off, size_t len) {
        ceph_assert(len > 0);
        f->open_object_section("free");
        char off_hex[30];
        char len_hex[30];
        snprintf(off_hex, sizeof(off_hex) - 1, "0x%zx", off);
        snprintf(len_hex, sizeof(len_hex) - 1, "0x%zx", len);
        f->dump_string("offset", off_hex);
        f->dump_string("length", len_hex);
        f->close_section();
      };
      alloc->foreach(iterated_allocation);
      f->close_section();
      f->close_section();
    } else if (command == "bluestore allocator score " + name) {
      f->open_object_section("fragmentation_score");
      f->dump_float("fragmentation_rating", alloc->get_fragmentation_score());
      f->close_section();
    } else if (command == "bluestore allocator fragmentation " + name) {
      f->open_object_section("fragmentation");
      f->dump_float("fragmentation_rating", alloc->get_fragmentation());
      f->close_section();
    } else if (command == "bluestore allocator fragmentation histogram " + name) {
      int64_t alloc_unit = alloc->get_block_size();
      cmd_getval(cmdmap, "alloc_unit", alloc_unit);
      if (alloc_unit <= 0  ||
          p2align(alloc_unit, alloc->get_block_size()) != alloc_unit) {
        ss << "Invalid allocation unit: '" << alloc_unit
           << "', to be aligned with: '" << alloc->get_block_size()
           << "'" << std::endl;
        return -EINVAL;
      }
      int64_t num_buckets = 8;
      cmd_getval(cmdmap, "num_buckets", num_buckets);
      if (num_buckets < 2) {
        ss << "Invalid amount of buckets (min=2): '" << num_buckets
           << "'" << std::endl;
        return -EINVAL;
      }

      AllocatorBase::FreeStateHistogram hist(num_buckets);
      alloc->foreach(
        [&](size_t off, size_t len) {
          hist.record_extent(uint64_t(alloc_unit), off, len);
        });
      f->open_array_section("extent_counts");
      hist.foreach(
        [&](uint64_t max_len, uint64_t total, uint64_t aligned, uint64_t units) {
          f->open_object_section("c");
          f->dump_unsigned("max_len", max_len);
          f->dump_unsigned("total", total);
          f->dump_unsigned("aligned", aligned);
          f->dump_unsigned("units", units);
          f->close_section();
        }
      );
      f->close_section();
    } else {
      ss << "Invalid command" << std::endl;
      r = -ENOSYS;
    }
    return r;
  }

};


AllocatorBase::AllocatorBase(std::string_view name,
                     int64_t _capacity,
                     int64_t _block_size)
  : Allocator(name, _capacity, _block_size)
{
  asok_hook = new SocketHook(this, name);
}

AllocatorBase::~AllocatorBase()
{
  delete asok_hook;
}

const string& AllocatorBase::get_name() const {
  return asok_hook->name;
}

/*************
* AllocatorBase::FreeStateHistogram
*************/
using std::function;

void AllocatorBase::FreeStateHistogram::record_extent(uint64_t alloc_unit,
                                                  uint64_t off,
                                                  uint64_t len)
{
  size_t idx = myTraits._get_bucket(len);
  ceph_assert(idx < buckets.size());
  ++buckets[idx].total;

  // now calculate the bucket for the chunk after alignment,
  // resulting chunks shorter than alloc_unit are discarded
  auto delta = p2roundup(off, alloc_unit) - off;
  if (len >= delta + alloc_unit) {
    len -= delta;
    idx = myTraits._get_bucket(len);
    ceph_assert(idx < buckets.size());
    ++buckets[idx].aligned;
    buckets[idx].alloc_units += len / alloc_unit;
  }
}
void AllocatorBase::FreeStateHistogram::foreach(
  function<void(uint64_t max_len,
                uint64_t total,
                uint64_t aligned,
                uint64_t unit)> cb)
{
  size_t i = 0;
  for (const auto& b : buckets) {
    cb(myTraits._get_bucket_max(i),
      b.total, b.aligned, b.alloc_units);
    ++i;
  }
}
