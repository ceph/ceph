// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include <bit>
#include "StupidAllocator.h"
#include "BitmapAllocator.h"
#include "AvlAllocator.h"
#include "BtreeAllocator.h"
#include "HybridAllocator.h"
#include "common/debug.h"
#include "common/admin_socket.h"
#define dout_subsys ceph_subsys_bluestore
using TOPNSPC::common::cmd_getval;

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;

class Allocator::SocketHook : public AdminSocketHook {
  Allocator *alloc;

  friend class Allocator;
  std::string name;
public:
  SocketHook(Allocator *alloc, std::string_view _name) :
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
      int64_t alloc_unit = 4096;
      cmd_getval(cmdmap, "alloc_unit", alloc_unit);
      if (alloc_unit == 0  ||
          p2align(alloc_unit, alloc->get_block_size()) != alloc_unit) {
        ss << "Invalid allocation unit: '" << alloc_unit
           << ", to be aligned with: '" << alloc->get_block_size()
           << std::endl;
        return -EINVAL;
      }
      int64_t num_buckets = 8;
      cmd_getval(cmdmap, "num_buckets", num_buckets);
      if (num_buckets < 2) {
        ss << "Invalid amount of buckets (min=2): '" << num_buckets
           << std::endl;
        return -EINVAL;
      }

      Allocator::FreeStateHistogram hist;
      hist.resize(num_buckets);
      alloc->build_free_state_histogram(alloc_unit, hist);
      f->open_array_section("extent_counts");
      for(int i = 0; i < num_buckets; i++) {
        f->open_object_section("c");
        f->dump_unsigned("max_len",
          hist[i].get_max(i, num_buckets)
        );
        f->dump_unsigned("total", hist[i].total);
        f->dump_unsigned("aligned", hist[i].aligned);
        f->dump_unsigned("units", hist[i].alloc_units);
        f->close_section();
      }
      f->close_section();
    } else {
      ss << "Invalid command" << std::endl;
      r = -ENOSYS;
    }
    return r;
  }

};
Allocator::Allocator(std::string_view name,
                     int64_t _capacity,
                     int64_t _block_size)
 : device_size(_capacity),
   block_size(_block_size)
{
  asok_hook = new SocketHook(this, name);
}


Allocator::~Allocator()
{
  delete asok_hook;
}

const string& Allocator::get_name() const {
  return asok_hook->name;
}

Allocator *Allocator::create(
  CephContext* cct,
  std::string_view type,
  int64_t size,
  int64_t block_size,
  std::string_view name)
{
  Allocator* alloc = nullptr;
  if (type == "stupid") {
    alloc = new StupidAllocator(cct, size, block_size, name);
  } else if (type == "bitmap") {
    alloc = new BitmapAllocator(cct, size, block_size, name);
  } else if (type == "avl") {
    return new AvlAllocator(cct, size, block_size, name);
  } else if (type == "btree") {
    return new BtreeAllocator(cct, size, block_size, name);
  } else if (type == "hybrid") {
    return new HybridAllocator(cct, size, block_size,
      cct->_conf.get_val<uint64_t>("bluestore_hybrid_alloc_mem_cap"),
      name);
  }
  if (alloc == nullptr) {
    lderr(cct) << "Allocator::" << __func__ << " unknown alloc type "
	     << type << dendl;
  }
  return alloc;
}

void Allocator::release(const PExtentVector& release_vec)
{
  interval_set<uint64_t> release_set;
  for (auto e : release_vec) {
    release_set.insert(e.offset, e.length);
  }
  release(release_set);
}

/**
 * Gives fragmentation a numeric value.
 *
 * Following algorithm applies value to each existing free unallocated block.
 * Value of single block is a multiply of size and per-byte-value.
 * Per-byte-value is greater for larger blocks.
 * Assume block size X has value per-byte p; then block size 2*X will have per-byte value 1.1*p.
 *
 * This could be expressed in logarithms, but for speed this is interpolated inside ranges.
 * [1]  [2..3] [4..7] [8..15] ...
 * ^    ^      ^      ^
 * 1.1  1.1^2  1.1^3  1.1^4 ...
 *
 * Final score is obtained by proportion between score that would have been obtained
 * in condition of absolute fragmentation and score in no fragmentation at all.
 */
double Allocator::get_fragmentation_score()
{
  // this value represents how much worth is 2X bytes in one chunk then in X + X bytes
  static const double double_size_worth_small = 1.2;
  // chunks larger then 128MB are large enough that should be counted without penalty
  static const double double_size_worth_huge = 1;
  static const size_t small_chunk_p2 = 20; // 1MB
  static const size_t huge_chunk_p2 = 27; // 128MB
  // for chunks 1MB - 128MB penalty coeffs are linearly weighted 1.2 (at small) ... 1 (at huge)
  static std::vector<double> scales{1};
  double score_sum = 0;
  size_t sum = 0;

  auto get_score = [&](size_t v) -> double {
    size_t sc = sizeof(v) * 8 - std::countl_zero(v) - 1; //assign to grade depending on log2(len)
    while (scales.size() <= sc + 1) {
      //unlikely expand scales vector
      auto ss = scales.size();
      double scale = double_size_worth_small;
      if (ss >= huge_chunk_p2) {
	scale = double_size_worth_huge;
      } else if (ss > small_chunk_p2) {
	// linear decrease 1.2 ... 1
	scale = (double_size_worth_huge * (ss - small_chunk_p2) + double_size_worth_small * (huge_chunk_p2 - ss)) /
	  (huge_chunk_p2 - small_chunk_p2);
      }
      scales.push_back(scales[scales.size() - 1] * scale);
    }
    size_t sc_shifted = size_t(1) << sc;
    double x = double(v - sc_shifted) / sc_shifted; //x is <0,1) in its scale grade
    // linear extrapolation in its scale grade
    double score = (sc_shifted    ) * scales[sc]   * (1-x) +
                   (sc_shifted * 2) * scales[sc+1] * x;
    return score;
  };

  auto iterated_allocation = [&](size_t off, size_t len) {
    ceph_assert(len > 0);
    score_sum += get_score(len);
    sum += len;
  };
  foreach(iterated_allocation);

  double ideal = get_score(sum);
  double terrible = (sum / block_size) * get_score(block_size);
  return (ideal - score_sum) / (ideal - terrible);
}

void Allocator::build_free_state_histogram(
  size_t alloc_unit, Allocator::FreeStateHistogram& hist)
{
  auto num_buckets = hist.size();
  ceph_assert(num_buckets);

  auto base = free_state_hist_bucket::base;
  auto base_bits = free_state_hist_bucket::base_bits;
  auto mux = free_state_hist_bucket::mux;
  // maximum chunk size we track,
  // provided by the bucket before the last one
  size_t max =
    free_state_hist_bucket::get_max(num_buckets - 2, num_buckets);

  auto iterated_allocation = [&](size_t off, size_t len) {
    size_t idx;
    if (len <= base) {
      idx = 0;
    } else if (len > max) {
      idx = num_buckets - 1;
    } else {
      size_t most_bit = cbits(uint64_t(len-1)) - 1;
      idx = 1 + ((most_bit - base_bits) / mux);
    }
    ceph_assert(idx < num_buckets);
    ++hist[idx].total;

    // now calculate the bucket for the chunk after alignment,
    // resulting chunks shorter than alloc_unit are discarded
    auto delta = p2roundup(off, alloc_unit) - off;
    if (len >= delta + alloc_unit) {
      len -= delta;
      if (len <= base) {
        idx = 0;
      } else if (len > max) {
        idx = num_buckets - 1;
      } else {
        size_t most_bit = cbits(uint64_t(len-1)) - 1;
        idx = 1 + ((most_bit - base_bits) / mux);
      }
      ++hist[idx].aligned;
      hist[idx].alloc_units += len / alloc_unit;
    }
  };

  foreach(iterated_allocation);
}
