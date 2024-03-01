// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/osd/scrubber_generators.h"

#include <fmt/ranges.h>

using namespace ScrubGenerator;

// ref: PGLogTestRebuildMissing()
bufferlist create_object_info(const ScrubGenerator::RealObj& objver)
{
  object_info_t oi{};
  oi.soid = objver.ghobj.hobj;
  oi.version = eversion_t(objver.ghobj.generation, 0);
  oi.size = objver.data.size;

  bufferlist bl;
  oi.encode(bl,
	    0 /*get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr)*/);
  return bl;
}

std::pair<bufferlist, std::vector<snapid_t>> create_object_snapset(
  const ScrubGenerator::RealObj& robj,
  const SnapsetMockData* snapset_mock_data)
{
  if (!snapset_mock_data) {
    return {bufferlist(), {}};
  }
  /// \todo fill in missing version/osd details from the robj
  auto sns = snapset_mock_data->make_snapset();
  bufferlist bl;
  encode(sns, bl);

  // extract the set of object snaps
  return {bl, sns.snaps};
}

RealObjsConfList ScrubGenerator::make_real_objs_conf(
  int64_t pool_id,
  const RealObjsConf& blueprint,
  std::vector<int32_t> active_osds)
{
  RealObjsConfList all_osds;

  for (auto osd : active_osds) {
    RealObjsConfRef this_osd_fakes = std::make_unique<RealObjsConf>(blueprint);
    // now - fix & corrupt every "object" in the blueprint
    for (RealObj& robj : this_osd_fakes->objs) {

      robj.ghobj.hobj.pool = pool_id;
    }

    all_osds[osd] = std::move(this_osd_fakes);
  }
  return all_osds;  // reconsider (maybe add a move ctor?)
}

///\todo dispose of the created buffer pointers

ScrubGenerator::SmapEntry ScrubGenerator::make_smobject(
  const ScrubGenerator::RealObj& blueprint,
  int osd_num)
{
  ScrubGenerator::SmapEntry ret;

  ret.ghobj = blueprint.ghobj;
  ret.smobj.attrs[OI_ATTR] = create_object_info(blueprint);
  if (blueprint.snapset_mock_data) {
    auto [bl, snaps] =
      create_object_snapset(blueprint, blueprint.snapset_mock_data);
    ret.smobj.attrs[SS_ATTR] = bl;
    std::cout << fmt::format("{}: ({}) osd:{} snaps:{}",
			     __func__,
			     ret.ghobj.hobj,
			     osd_num,
			     snaps)
	      << std::endl;
  }

  for (const auto& [at_k, at_v] : blueprint.data.attrs) {
    // deep copy assignment
    ret.smobj.attrs[at_k].clear();
    ret.smobj.attrs[at_k].append(at_v.c_str(), at_v.size());
    {
      // verifying (to be removed after dev phase)
      std::string bkstr = ret.smobj.attrs[at_k].to_str();
      std::cout << fmt::format("{}: verification: {}", __func__, bkstr)
		<< std::endl;
    }
  }
  ret.smobj.size = blueprint.data.size;
  ret.smobj.digest = blueprint.data.hash;
  /// \todo handle the 'present' etc'

  ret.smobj.object_omap_keys = blueprint.data.omap.size();
  ret.smobj.object_omap_bytes = blueprint.data.omap_bytes;
  return ret;
}

all_clones_snaps_t ScrubGenerator::all_clones(
  const ScrubGenerator::RealObj& head_obj)
{
  std::cout << fmt::format("{}: head_obj.ghobj.hobj:{}",
			   __func__,
			   head_obj.ghobj.hobj)
	    << std::endl;

  std::map<hobject_t, std::vector<snapid_t>> ret;

  for (const auto& clone : head_obj.snapset_mock_data->clones) {
    auto clone_set_it = head_obj.snapset_mock_data->clone_snaps.find(clone);
    if (clone_set_it == head_obj.snapset_mock_data->clone_snaps.end()) {
      std::cout << "note: no clone_snaps for " << clone << std::endl;
      continue;
    }
    auto clone_set = clone_set_it->second;
    hobject_t clone_hobj{head_obj.ghobj.hobj};
    clone_hobj.snap = clone;

    ret[clone_hobj] = clone_set_it->second;
    std::cout << fmt::format("{}: clone:{} clone_set:{}",
			     __func__,
			     clone_hobj,
			     clone_set)
	      << std::endl;
  }

  return ret;
}

void ScrubGenerator::add_object(ScrubMap& map,
				const ScrubGenerator::RealObj& real_obj,
				int osd_num)
{
  // do we have data corruption recipe for this OSD?
  /// \todo c++20: use contains()
  CorruptFunc relevant_fix = crpt_do_nothing;

  auto p = real_obj.corrupt_funcs->find(osd_num);
  if (p != real_obj.corrupt_funcs->end()) {
    // yes, we have a corruption recepie for this OSD
    // \todo c++20: use at()
    relevant_fix = p->second;
  }

  // create a possibly-corrupted copy of the "real object"
  auto modified_obj = (relevant_fix)(real_obj, osd_num);

  std::cout << fmt::format("{}: modified: osd:{} ho:{} key:{}",
			   __func__,
			   osd_num,
			   modified_obj.ghobj.hobj,
			   modified_obj.ghobj.hobj.get_key())
	    << std::endl;

  auto entry = make_smobject(modified_obj, osd_num);
  std::cout << fmt::format("{}: osd:{} smap entry: {} {}",
			   __func__,
			   osd_num,
			   entry.smobj.size,
			   entry.smobj.attrs.size())
	    << std::endl;
  map.objects[entry.ghobj.hobj] = entry.smobj;
}
