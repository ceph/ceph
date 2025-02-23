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
      if (alloc_unit <= 0  ||
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

      Allocator::FreeStateHistogram hist(num_buckets);
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
    return new HybridAvlAllocator(cct, size, block_size,
      cct->_conf.get_val<uint64_t>("bluestore_hybrid_alloc_mem_cap"),
      name);
  }  else if (type == "hybrid_btree2") {
    return new HybridBtree2Allocator(cct, size, block_size,
      cct->_conf.get_val<uint64_t>("bluestore_hybrid_alloc_mem_cap"),
      cct->_conf.get_val<double>("bluestore_btree2_alloc_weight_factor"),
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
  release_set_t release_set;
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

static void push_next_scale(std::vector<double>& scales)
{
  // this value represents how much worth is 2X bytes in one chunk then in X + X bytes
  static const double double_size_worth_small = 1.2;
  // chunks larger then 128MB are large enough that should be counted without penalty
  static const double double_size_worth_huge = 1;
  static const size_t small_chunk_p2 = 20; // 1MB
  static const size_t huge_chunk_p2 = 27; // 128MB
  // for chunks 1MB - 128MB penalty coeffs are linearly weighted 1.2 (at small) ... 1 (at huge)
  auto ss = scales.size();
  double scale = double_size_worth_small;
  if (ss >= huge_chunk_p2) {
    scale = double_size_worth_huge;
  } else if (ss > small_chunk_p2) {
    // linear decrease 1.2 ... 1
    scale = (double_size_worth_huge * (ss - small_chunk_p2) +
             double_size_worth_small * (huge_chunk_p2 - ss)) /
            (huge_chunk_p2 - small_chunk_p2);
  }
  scales.push_back(scales[scales.size() - 1] * scale);
}

double Allocator::get_score(size_t v)
{
  size_t sc = sizeof(v) * 8 - std::countl_zero(v) - 1; //assign to grade depending on log2(len)
  while (score_scaled.size() <= sc + 1) {
    //unlikely expand scales vector
    push_next_scale(score_scaled);
  }
  size_t sc_shifted = size_t(1) << sc;
  double x = double(v - sc_shifted) / sc_shifted; //x is <0,1) in its scale grade
  // linear extrapolation in its scale grade
  double score = (sc_shifted    ) * score_scaled[sc]   * (1-x) +
                 (sc_shifted * 2) * score_scaled[sc+1] * x;
  return score;
}

double Allocator::get_fragmentation_score_raw()
{
  double score_raw = 0;
  size_t sum = 0;
  auto iterated_allocation = [&](size_t off, size_t len) {
    ceph_assert(len > 0);
    score_raw += get_score(len);
    sum += len;
  };
  foreach(iterated_allocation);
  return score_raw;
}
double Allocator::get_fragmentation_score()
{
  double score_nom = get_fragmentation_score_raw();
  size_t free_size = get_free();

  double ideal = get_score(free_size);
  double terrible = (free_size / block_size) * get_score(block_size);
  return (ideal - score_nom) / (ideal - terrible);
}

/*************
* Allocator::FreeStateHistogram
*************/
using std::function;

void Allocator::FreeStateHistogram::record_extent(uint64_t alloc_unit,
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
void Allocator::FreeStateHistogram::foreach(
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

class FastScore::SocketHook : public AdminSocketHook
{
  FastScore* fscore;
  friend class FastScore;
  int call(std::string_view command, const cmdmap_t &cmdmap, const bufferlist &,
           Formatter *f, std::ostream &errss, bufferlist &out) override
  {
    // no need to check the command, we only registered one
    std::stringstream ss;
    fscore->debug_check(ss);
    out.append(ss);
    return 0;
  };

public:
  SocketHook(FastScore* fscore)
  : fscore(fscore)
  {
    AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
    if (!admin_socket) return;
    admin_socket->register_command(
      fmt::format("bluestore allocator score {} debug",fscore->alloc->get_name()), this,
      "perform various tests on fast allocator calculation, compare against slow method");
  }
  ~SocketHook() {
    AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
    if (!admin_socket) return;
    admin_socket->unregister_commands(this);
  }
};

FastScore::FastScore(CephContext* cct, Allocator* alloc)
  : alloc(alloc)
  , alloc_size(alloc->get_block_size())
  , buckets()
  , asok_hook(new FastScore::SocketHook(this))
{
  while ((int64_t)1 << alloc->score_scaled.size() <= alloc->get_capacity() * 2) {
    push_next_scale(alloc->score_scaled);
  }
}
FastScore::~FastScore() {}

// Splits input length into scale factor and value <0-1)
// Input: length   ;size of free space that has just been added or removed
// Output: first   ;log2 of length that determines a lower bucket for interpolation
//       : second  ;value that is <0,1) location between lower and higher(lower+1) bucket,
//                 ;it is multiplied by (1<<sc) to keep integer representation
inline std::pair<uint8_t, uint64_t>
  FastScore::split_by_log2(uint64_t length)
{
  size_t v = length;
  ceph_assert(v > 0);
  uint8_t sc = p2log_floor(v);
  // If we strip highest bit from v we get:
  // v = x + (1 << sc) with x = <0-1) * (1 << sc)
  // In notable border condition, for v == 1 we get x == 0.
  size_t x = v - ((uint64_t)1 << sc);
  return std::make_pair(sc, x);
}

inline bool FastScore::close_enough(double a, double b)
{
  return abs((a - b) / (a + b)) < 1.0e-6;
}

void FastScore::now_free(uint64_t offset, uint64_t length)
{
  if (check_allocated) {
    debug_allocated.insert(offset / alloc_size, length / alloc_size);
  }
  auto [sc, x] = split_by_log2(length);
  buckets[sc]     += ((uint64_t)1 << sc) - x;
  buckets[sc + 1] += 2 * x;
  // *2 above deceives to be divergent from get_score()
  // We do not scale by 2^n when reconstructing score from buckets,
  // so it needs to be adjusted here.
  if (check_drag_score) {
    score_sum_drag +=
      alloc->score_scaled[sc] * (((uint64_t)1 << sc) - x) +
      alloc->score_scaled[sc+1] * 2 * x;
  }
}

void FastScore::now_occupied(uint64_t offset, uint64_t length)
{
  if (check_allocated) {
    debug_allocated.erase(offset / alloc_size, length / alloc_size);
  }
  auto [sc, x] = split_by_log2(length);
  buckets[sc]     -= ((uint64_t)1 << sc) - x;
  buckets[sc + 1] -= 2 * x;
  if (check_drag_score) {
    score_sum_drag -=
      alloc->score_scaled[sc] * (((uint64_t)1 << sc) - x) +
      alloc->score_scaled[sc+1] * 2 * x;
  }
}

double FastScore::get_fragmentation_score_raw()
{
  double score_raw = 0;
  size_t v = 1;
  while ((int64_t)1 << v < alloc->get_capacity() * 2) {
    score_raw += alloc->score_scaled[v] * buckets[v];
    v++;
  }
  return score_raw;
}

void FastScore::debug_check(std::ostream& ss) {
#ifdef LOCAL_DOUBLE_PRINT
#error This is local macro, just change name.
#endif
#define LOCAL_DOUBLE_PRINT(line) { \
  lgeneric_subdout(g_ceph_context, bluestore, 0) << line << dendl; \
  ss << line << std::endl; }

  if (check_buckets) {
    std::array<int64_t, 64> buckets_redone = {0}; // bits in uint64_t
    std::array<int64_t, 64> buckets_copied = {0};
    bool do_once = true;
    auto iterated_allocation = [&](size_t off, size_t len) {
      if (do_once) {
        buckets_copied = buckets;
        do_once = false;
      }
      ceph_assert(len > 0);
      auto [sc, x] = split_by_log2(len);
      buckets_redone[sc] += ((uint64_t)1 << sc) - x;
      buckets_redone[sc + 1] += 2 * x;
    };
    alloc->foreach (iterated_allocation);
    bool buckets_exact = true;
    for (size_t i = 0; i < 64; i++) {
      if (buckets_redone[i] != buckets_copied[i])
        buckets_exact = false;
      if (!buckets_exact) {
        LOCAL_DOUBLE_PRINT(
          "FAIL, bucket " << i << " redone=" << buckets_redone[i]
          << " != copy=" << buckets_copied[i]);
      }
    }
    if (buckets_exact) {
        LOCAL_DOUBLE_PRINT("PASS, buckets match");
    }
    ceph_assert(!assert_on_verify || buckets_exact);
  }
  double score_slow = 0;
  double score_fast = 0;
  if (check_drag_score || check_slow) {
    score_slow = alloc->Allocator::get_fragmentation_score_raw();
    score_fast = get_fragmentation_score_raw();
    LOCAL_DOUBLE_PRINT(
      fmt::format("fast={:.15} slow={:.15} drag={:.15}", score_fast, score_slow, score_sum_drag)
    );
  }
  if (check_drag_score) {
    ceph_assert(!assert_on_verify || close_enough(score_slow, score_sum_drag));
  }
  if (check_slow) {
    ceph_assert(!assert_on_verify || close_enough(score_slow, score_fast));
  }
  if (check_allocated) {
    // assume that iterate extents will provide non-overlapping extents
    // check that we contain all listed ranges
    // check that our size matches iterated size
    bool do_once = true;
    interval_set<uint32_t> debug_copy;
    interval_set<uint32_t> alloc_copy;
    alloc->foreach ([&](uint64_t offset, uint64_t length) {
      if (do_once) {
        // snapshot here because we need allocator's lock
        debug_copy = debug_allocated;
        do_once = false;
      }
      alloc_copy.insert(offset / alloc_size, length / alloc_size);
    });

    auto it_d = debug_copy.begin();
    auto it_a = alloc_copy.begin();
    uint32_t fails = 0;
    while (it_d != debug_copy.end() && it_a != alloc_copy.end()) {
      if (it_d.get_start() != it_a.get_start() &&
          it_d.get_len() != it_a.get_len()) {
        LOCAL_DOUBLE_PRINT(
          "FAIL " << fails << std::hex
          << " alloc=0x" << it_a.get_start() * alloc_size << "~" << it_a.get_len() * alloc_size
          << " debug=0x" << it_d.get_start() * alloc_size << "~" << it_d.get_len() * alloc_size
        );
        if (it_d.get_start() <= it_a.get_start())
          ++it_d;
        if (it_d.get_start() >= it_a.get_start())
          ++it_a;
        fails++;
        if (fails == 1000)
          break;
      } else {
        ++it_d;
        ++it_a;
      }
    }
    if (it_d != debug_copy.end() || it_a != alloc_copy.end()) {
      fails++;
    }
    if (fails != 0) {
      LOCAL_DOUBLE_PRINT(
        "FAIL, allocator diverges from now_free/now_occupied");
    } else {
      LOCAL_DOUBLE_PRINT(
        "PASS, tracking allocations ok");
    }
    ceph_assert(!assert_on_verify || fails == 0);
  }
#undef LOCAL_DOUBLE_PRINT
}
