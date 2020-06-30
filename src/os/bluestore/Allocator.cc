// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include "StupidAllocator.h"
#include "BitmapAllocator.h"
#include "AvlAllocator.h"
#include "HybridAllocator.h"
#ifdef HAVE_LIBZBC
#include "ZonedAllocator.h"
#endif
#include "common/debug.h"
#include "common/admin_socket.h"
#define dout_subsys ceph_subsys_bluestore

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;

class Allocator::SocketHook : public AdminSocketHook {
  Allocator *alloc;

  friend class Allocator;
  std::string name;
public:
  explicit SocketHook(Allocator *alloc,
                      const std::string& _name) :
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
	   Formatter *f,
	   std::ostream& ss,
	   bufferlist& out) override {
    int r = 0;
    if (command == "bluestore allocator dump " + name) {
      f->open_array_section("free_regions");
      auto iterated_allocation = [&](size_t off, size_t len) {
        ceph_assert(len > 0);
        f->open_object_section("free");
        char off_hex[30];
        char len_hex[30];
        snprintf(off_hex, sizeof(off_hex) - 1, "0x%lx", off);
        snprintf(len_hex, sizeof(len_hex) - 1, "0x%lx", len);
        f->dump_string("offset", off_hex);
        f->dump_string("length", len_hex);
        f->close_section();
      };
      alloc->dump(iterated_allocation);
      f->close_section();
    } else if (command == "bluestore allocator score " + name) {
      f->open_object_section("fragmentation_score");
      f->dump_float("fragmentation_rating", alloc->get_fragmentation_score());
      f->close_section();
    } else if (command == "bluestore allocator fragmentation " + name) {
      f->open_object_section("fragmentation");
      f->dump_float("fragmentation_rating", alloc->get_fragmentation());
      f->close_section();
    } else {
      ss << "Invalid command" << std::endl;
      r = -ENOSYS;
    }
    return r;
  }

};
Allocator::Allocator(const std::string& name)
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

Allocator *Allocator::create(CephContext* cct, string type,
                             int64_t size, int64_t block_size, const std::string& name)
{
  Allocator* alloc = nullptr;
  if (type == "stupid") {
    alloc = new StupidAllocator(cct, name, block_size);
  } else if (type == "bitmap") {
    alloc = new BitmapAllocator(cct, size, block_size, name);
  } else if (type == "avl") {
    return new AvlAllocator(cct, size, block_size, name);
  } else if (type == "hybrid") {
    return new HybridAllocator(cct, size, block_size,
      cct->_conf.get_val<uint64_t>("bluestore_hybrid_alloc_mem_cap"),
      name);
#ifdef HAVE_LIBZBC
  } else if (type == "zoned") {
    return new ZonedAllocator(cct, size, block_size, name);
#endif
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
  static const double double_size_worth = 1.1 ;
  std::vector<double> scales{1};
  double score_sum = 0;
  size_t sum = 0;

  auto get_score = [&](size_t v) -> double {
    size_t sc = sizeof(v) * 8 - clz(v) - 1; //assign to grade depending on log2(len)
    while (scales.size() <= sc + 1) {
      //unlikely expand scales vector
      scales.push_back(scales[scales.size() - 1] * double_size_worth);
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
  dump(iterated_allocation);


  double ideal = get_score(sum);
  double terrible = sum * get_score(1);
  return (ideal - score_sum) / (ideal - terrible);
}
