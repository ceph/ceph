// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/// \file generating scrub-related maps & objects for unit tests

#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "include/buffer.h"
#include "include/buffer_raw.h"
#include "include/object.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/pg_scrubber.h"

namespace ScrubGenerator {

/// \todo enhance the MockLog to capture the log messages
class MockLog : public LoggerSinkSet {
 public:
  void debug(std::stringstream& s) final
  {
    std::cout << "\n<<debug>> " << s.str() << std::endl;
  }
  void info(std::stringstream& s) final
  {
    std::cout << "\n<<info>> " << s.str() << std::endl;
  }
  void sec(std::stringstream& s) final
  {
    std::cout << "\n<<sec>> " << s.str() << std::endl;
  }
  void warn(std::stringstream& s) final
  {
    std::cout << "\n<<warn>> " << s.str() << std::endl;
  }
  void error(std::stringstream& s) final
  {
    err_count++;
    std::cout << "\n<<error>> " << s.str() << std::endl;
  }
  OstreamTemp info() final { return OstreamTemp(CLOG_INFO, this); }
  OstreamTemp warn() final { return OstreamTemp(CLOG_WARN, this); }
  OstreamTemp error() final { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp sec() final { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp debug() final { return OstreamTemp(CLOG_DEBUG, this); }

  void do_log(clog_type prio, std::stringstream& ss) final
  {
    switch (prio) {
      case CLOG_DEBUG:
	debug(ss);
	break;
      case CLOG_INFO:
	info(ss);
	break;
      case CLOG_SEC:
	sec(ss);
	break;
      case CLOG_WARN:
	warn(ss);
	break;
      case CLOG_ERROR:
      default:
	error(ss);
	break;
    }
  }

  void do_log(clog_type prio, const std::string& ss) final
  {
    switch (prio) {
      case CLOG_DEBUG:
	debug() << ss;
	break;
      case CLOG_INFO:
	info() << ss;
	break;
      case CLOG_SEC:
	sec() << ss;
	break;
      case CLOG_WARN:
	warn() << ss;
	break;
      case CLOG_ERROR:
      default:
	error() << ss;
	break;
    }
  }

  virtual ~MockLog() {}

  int err_count{0};
  int expected_err_count{0};
  void set_expected_err_count(int c) { expected_err_count = c; }
};

// ///////////////////////////////////////////////////////////////////////// //
// ///////////////////////////////////////////////////////////////////////// //

struct pool_conf_t {
  int pg_num{3};
  int pgp_num{3};
  int size{3};
  int min_size{3};
  std::string name{"rep_pool"};
};

using attr_t = std::map<std::string, std::string>;

using all_clones_snaps_t = std::map<hobject_t, std::vector<snapid_t>>;

struct RealObj;

// a function to manipulate (i.e. corrupt) an object in a specific OSD
using CorruptFunc =
  std::function<RealObj(const RealObj& s, [[maybe_unused]] int osd_num)>;
using CorruptFuncList = std::map<int, CorruptFunc>;  // per OSD

struct SnapsetMockData {

  using CookedCloneSnaps =
    std::tuple<std::map<snapid_t, uint64_t>,
	       std::map<snapid_t, std::vector<snapid_t>>,
	       std::map<snapid_t, interval_set<uint64_t>>>;

  // an auxiliary function to cook the data for the SnapsetMockData
  using clone_snaps_cooker = CookedCloneSnaps (*)();

  snapid_t seq;
  std::vector<snapid_t> snaps;	 // descending
  std::vector<snapid_t> clones;	 // ascending

  std::map<snapid_t, interval_set<uint64_t>> clone_overlap;  // overlap w/ next
							     // newest
  std::map<snapid_t, uint64_t> clone_size;
  std::map<snapid_t, std::vector<snapid_t>> clone_snaps;  // descending


  SnapsetMockData(snapid_t seq,
		  std::vector<snapid_t> snaps,
		  std::vector<snapid_t> clones,
		  std::map<snapid_t, interval_set<uint64_t>> clone_overlap,
		  std::map<snapid_t, uint64_t> clone_size,
		  std::map<snapid_t, std::vector<snapid_t>> clone_snaps)
      : seq(seq)
      , snaps(snaps)
      , clones(clones)
      , clone_overlap(clone_overlap)
      , clone_size(clone_size)
      , clone_snaps(clone_snaps)
  {}

  SnapsetMockData(snapid_t seq,
		  std::vector<snapid_t> snaps,
		  std::vector<snapid_t> clones,
		  clone_snaps_cooker func)
      : seq{seq}
      , snaps{snaps}
      , clones(clones)
  {
    auto [clone_size_, clone_snaps_, clone_overlap_] = func();
    clone_size = clone_size_;
    clone_snaps = clone_snaps_;
    clone_overlap = clone_overlap_;
  }

  SnapSet make_snapset() const
  {
    SnapSet ss;
    ss.seq = seq;
    ss.snaps = snaps;
    ss.clones = clones;
    ss.clone_overlap = clone_overlap;
    ss.clone_size = clone_size;
    ss.clone_snaps = clone_snaps;
    return ss;
  }
};

// an object in our "DB" - with its versioned snaps, "data" (size and hash),
// and "omap" (size and hash)

struct RealData {
  // not needed at this level of "data falsification": std::byte data;
  uint64_t size;
  uint32_t hash;
  uint32_t omap_digest;
  uint32_t omap_bytes;
  attr_t omap;
  attr_t attrs;
};

struct RealObj {
  // the ghobject - oid, version, snap, hash, pool
  ghobject_t ghobj;
  RealData data;
  const CorruptFuncList* corrupt_funcs;
  const SnapsetMockData* snapset_mock_data;
};

static inline RealObj crpt_do_nothing(const RealObj& s, int osdn)
{
  return s;
}

struct SmapEntry {
  ghobject_t ghobj;
  ScrubMap::object smobj;
};


ScrubGenerator::SmapEntry make_smobject(
  const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
  int osd_num);


/**
 * returns the object's snap-set
 */
void add_object(ScrubMap& map, const RealObj& obj_versions, int osd_num);

struct RealObjsConf {
  std::vector<RealObj> objs;
};

using RealObjsConfRef = std::unique_ptr<RealObjsConf>;

// RealObjsConf will be "developed" into the following of per-osd sets,
// now with the correct pool ID, and with the corrupting functions
// activated on the data
using RealObjsConfList = std::map<int, RealObjsConfRef>;

RealObjsConfList make_real_objs_conf(int64_t pool_id,
				     const RealObjsConf& blueprint,
				     std::vector<int32_t> active_osds);

/**
 * create the snap-ids set for all clones appearing in the head
 * object's snapset (those will be injected into the scrubber's mock,
 * to be used as the 'snap_mapper')
 */
all_clones_snaps_t all_clones(const RealObj& head_obj);
}  // namespace ScrubGenerator

template <>
struct fmt::formatter<ScrubGenerator::RealObj> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ScrubGenerator::RealObj& rlo, FormatContext& ctx)
  {
    using namespace ScrubGenerator;
    return fmt::format_to(ctx.out(),
			  "RealObj(gh:{}, dt:{}, snaps:{})",
			  rlo.ghobj,
			  rlo.data.size,
			  (rlo.snapset_mock_data ? rlo.snapset_mock_data->snaps
						 : std::vector<snapid_t>{}));
  }
};
