// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/// \file data-sets used by the scrubber unit tests

#include "./scrubber_test_datasets.h"


using namespace ScrubGenerator;
using namespace std::string_literals;

namespace ScrubDatasets {

static RealObj corrupt_object_size(const RealObj& s, [[maybe_unused]] int osdn)
{
  RealObj ret = s;
  ret.data.size = s.data.size + 1;
  return ret;
}

static RealObj corrupt_nothing(const RealObj& s, int osdn)
{
  return s;
}


static CorruptFuncList crpt_funcs_set0 = {{0, &corrupt_nothing}};

CorruptFuncList crpt_funcs_set1 = {{0, &corrupt_object_size},
				   {1, &corrupt_nothing}};


// object with head & two snaps

static hobject_t hobj_ms1{object_t{"hobj_ms1"},
			  "keykey",	// key
			  CEPH_NOSNAP,	// snap_id
			  0,		// hash
			  0,		// pool
			  ""s};		// nspace

SnapsetMockData::CookedCloneSnaps ms1_fn()
{
  std::map<snapid_t, uint64_t> clnsz;
  clnsz[0x20] = 222;
  clnsz[0x30] = 333;

  std::map<snapid_t, std::vector<snapid_t>> clnsn;
  clnsn[0x20] = {0x20};
  clnsn[0x30] = {0x30};

  std::map<snapid_t, interval_set<uint64_t>> overlaps;
  overlaps[0x20] = {};
  overlaps[0x30] = {};
  return {clnsz, clnsn, overlaps};
}

static SnapsetMockData hobj_ms1_snapset{/* seq */ 0x40,
					/* clones */ {0x20, 0x30},
					ms1_fn};

hobject_t hobj_ms1_snp30{object_t{"hobj_ms1"},
			 "keykey",  // key
			 0x30,	    // snap_id
			 0,	    // hash
			 0,	    // pool
			 ""s};	    // nspace

static hobject_t hobj_ms1_snp20{object_t{"hobj_ms1"},
				"keykey",  // key
				0x20,	   // snap_id
				0,	   // hash
				0,	   // pool
				""s};	   // nspace


ScrubGenerator::RealObjsConf minimal_snaps_configuration{
  /* RealObjsConf::objs */ {

    /* Clone 30  */ {
      ghobject_t{hobj_ms1_snp30, 0, shard_id_t{0}},
      RealData{
	333,
	0x17,
	17,
	21,
	attr_t{/*{"_om1k", "om1v"}, {"om1k", "om1v"},*/ {"om3k", "om3v"}},
	attr_t{{"_at1k", "_at1v"}, {"_at2k", "at2v"}, {"at3k", "at3v"}}},
      &crpt_funcs_set0,
      nullptr},

    /* Clone 20  */
    {ghobject_t{hobj_ms1_snp20, 0, shard_id_t{0}},
     RealData{222,
	      0x17,
	      17,
	      21,
	      attr_t{/*{"_om1k", "om1v"}, {"om1k", "om1v"},*/ {"om3k", "om3v"}},
	      attr_t{{"_at1k", "_at1v"}, {"_at2k", "at2v"}, {"at3k", "at3v"}}},
     &crpt_funcs_set0,
     nullptr},

    /* Head  */
    {ghobject_t{hobj_ms1, 0, shard_id_t{0}},
     RealData{100,
	      0x17,
	      17,
	      21,
	      attr_t{{"_om1k", "om1v"}, {"om1k", "om1v"}, {"om3k", "om3v"}},
	      attr_t{{"_at1k", "_at1v"}, {"_at2k", "at2v"}, {"at3k", "at3v"}}


     },
     &crpt_funcs_set0,
     &hobj_ms1_snapset}}

};

}  // namespace ScrubDatasets
