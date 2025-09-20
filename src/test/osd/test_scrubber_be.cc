// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./scrubber_generators.h"
#include "./scrubber_test_datasets.h"

#include <gtest/gtest.h>
#include <signal.h>
#include <stdio.h>

#include <fmt/ranges.h>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "os/ObjectStore.h"
#include "osd/PG.h"
#include "osd/PGBackend.h"
#include "osd/PrimaryLogPG.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/pg_scrubber.h"
#include "osd/scrubber/scrub_backend.h"

#include "erasure-code/ErasureCodePlugin.h"

/// \file testing isolated parts of the Scrubber backend

using namespace std::string_literals;

int main(int argc, char** argv)
{
  std::map<std::string, std::string> defaults = {
    // make sure we have 3 copies, or some tests won't work
    {"osd_pool_default_size", "3"},
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    {"osd_crush_chooseleaf_type", "0"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(&defaults,
			 args,
			 CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


class TestScrubBackend : public ScrubBackend {
 public:
  TestScrubBackend(ScrubBeListener& scrubber,
		   PgScrubBeListener& pg,
		   pg_shard_t i_am,
		   bool repair,
		   scrub_level_t shallow_or_deep,
		   const std::set<pg_shard_t>& acting)
      : ScrubBackend(scrubber, pg, i_am, repair, shallow_or_deep, acting)
  {}

  bool get_m_repair() const { return m_repair; }
  bool get_is_replicated() const { return m_is_replicated; }
  auto get_omap_stats() const { return m_omap_stats; }

  const std::vector<pg_shard_t>& all_but_me() const { return m_acting_but_me; }

  /// populate the scrub-maps set for the 'chunk' being scrubbed
  void insert_faked_smap(pg_shard_t shard, const ScrubMap& smap);
};

// mocking the PG
class TestPg : public PgScrubBeListener {
  ErasureCodeInterfaceRef m_erasure_code_interface;

 public:
  ~TestPg() = default;

  TestPg(std::shared_ptr<PGPool> pool, pg_info_t& pginfo, pg_shard_t my_osd)
      : m_pool{pool}
      , m_info{pginfo}
      , m_pshard{my_osd}
  {}

  const PGPool& get_pgpool() const final { return *(m_pool.get()); }
  pg_shard_t get_primary() const final { return m_pshard; }
  void force_object_missing(ScrubberPasskey,
			    const std::set<pg_shard_t>& peer,
			    const hobject_t& oid,
			    eversion_t version) final
  {}

  const pg_info_t& get_pg_info(ScrubberPasskey) const final { return m_info; }

  uint64_t logical_to_ondisk_size(uint64_t logical_size,
                                  shard_id_t shard_id) const final
  {
    return logical_size;
  }

  bool ec_can_decode(const shard_id_set& available_shards) const final {
    return get_is_ec_optimized() &&
           available_shards.size() > get_ec_sinfo().get_k_plus_m() - 2;
  };

  // Fake encode function for erasure code tests in this class.
  // Just sets parities to sum of data shards.
  shard_id_map<bufferlist> ec_encode_acting_set(
      const bufferlist& chunks) const final {
    shard_id_map<bufferlist> encode_map(get_ec_sinfo().get_k_plus_m());
    for (shard_id_t i; i < get_ec_sinfo().get_k_plus_m(); ++i) {
      bufferlist bl;
      bl.append(buffer::create(get_ec_sinfo().get_chunk_size(), 0));
      bl.rebuild();
      encode_map.insert(i, bl);
    }

    for (shard_id_t i; i < get_ec_sinfo().get_k(); ++i) {
      for (int j = 0; std::cmp_less(j, get_ec_sinfo().get_chunk_size()); j++) {
        encode_map.at(i).c_str()[j] =
            chunks[j + (get_ec_sinfo().get_chunk_size() * i.id)];
        for (shard_id_t k{static_cast<int8_t>(get_ec_sinfo().get_k_plus_m())};
             k < get_ec_sinfo().get_k_plus_m(); ++k) {
          encode_map.at(k).c_str()[j] +=
              chunks[j + (get_ec_sinfo().get_chunk_size() * i.id)];
        }
      }
    }

    return encode_map;
  };

  // Fake encode function for erasure code tests in this class.
  // Just sets calculates missing parity by using sum of all data shards =
  // parity Tests using this will only have 1 missing shard.
  shard_id_map<bufferlist> ec_decode_acting_set(
      const shard_id_map<bufferlist>& chunks, int chunk_size) const final {
    shard_id_map<bufferlist> decode_map(get_ec_sinfo().get_k_plus_m());

    ceph_assert(chunks.size() > get_ec_sinfo().get_k_plus_m() - 2);

    for (shard_id_t i; i < get_ec_sinfo().get_k_plus_m(); ++i) {
      bufferlist bl;
      bufferptr ptr = buffer::create(chunk_size, 0);
      if (chunks.contains(i)) {
        ptr.copy_in(0, chunk_size, chunks.at(i).to_str().c_str());
      }
      bl.append(ptr);
      bl.rebuild();
      decode_map.insert(i, bl);
    }

    for (shard_id_t shard; shard < get_ec_sinfo().get_k_plus_m(); ++shard) {
      if (!chunks.contains(shard)) {
        for (int j = 0; j < chunk_size; j++) {
          if (shard < get_ec_sinfo().get_k()) {
            decode_map.at(shard).c_str()[j] =
                decode_map
                    .at(shard_id_t{static_cast<int8_t>(get_ec_sinfo().get_k())})
                    .c_str()[j];
          }
          for (shard_id_t i; i < get_ec_sinfo().get_k(); ++i) {
            if (shard < get_ec_sinfo().get_k() && chunks.contains(i)) {
              decode_map.at(shard).c_str()[j] -= decode_map.at(i).c_str()[j];
            } else if (chunks.contains(i)) {
              decode_map.at(shard).c_str()[j] += decode_map.at(i).c_str()[j];
            }
          }
        }
      } else {
        decode_map.insert(shard, chunks.at(shard));
      }
    }

    return decode_map;
  };

  bool get_ec_supports_crc_encode_decode() const final {
    return get_is_ec_optimized();
  }

  ECUtil::stripe_info_t get_ec_sinfo() const final { return *m_sinfo; }

  void set_stripe_info(unsigned int k, unsigned int m, uint64_t stripe_width,
                       const pg_pool_t* pool) {
    m_sinfo.reset(new ECUtil::stripe_info_t{k, m, stripe_width, pool});
  }

  bool is_waiting_for_unreadable_object() const final { return false; }

  std::shared_ptr<PGPool> m_pool;
  pg_info_t& m_info;
  pg_shard_t m_pshard;
  std::unique_ptr<ECUtil::stripe_info_t> m_sinfo;

  bool get_is_nonprimary_shard(const pg_shard_t &pg_shard) const final
  {
    return get_is_ec_optimized() &&
           m_pool->info.is_nonprimary_shard(pg_shard.shard);
  }
  bool get_is_hinfo_required() const final
  {
    return get_is_ec_optimized() &&
           !m_pool->info.has_flag(m_pool->info.FLAG_EC_OVERWRITES);
  }
  bool get_is_ec_optimized() const final {
    return m_pool->info.has_flag(m_pool->info.FLAG_EC_OPTIMIZATIONS);
  }
};


// ///////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////

// and the scrubber
class TestScrubber : public ScrubBeListener, public Scrub::SnapMapReaderI {
  using result_t = Scrub::SnapMapReaderI::result_t;
 public:
  ~TestScrubber() = default;

  TestScrubber(spg_t spg, OSDMapRef osdmap, LoggerSinkSet& logger)
      : m_spg{spg}
      , m_logger{logger}
      , m_osdmap{osdmap}
  {}

  std::ostream& gen_prefix(std::ostream& out) const final { return out; }

  CephContext* get_pg_cct() const final { return g_ceph_context; }

  LoggerSinkSet& get_logger() const final { return m_logger; }

  bool is_primary() const final { return m_primary; }

  spg_t get_pgid() const final { return m_info.pgid; }

  const OSDMapRef& get_osdmap() const final { return m_osdmap; }

  void add_to_stats(const object_stat_sum_t& stat) final { m_stats.add(stat); }

  // submit_digest_fixes() mock can be set to expect a specific set of
  // fixes to perform.
  /// \todo implement the mock.
  void submit_digest_fixes(const digests_fixes_t& fixes) final
  {
    std::cout << fmt::format("{} submit_digest_fixes({})",
			     __func__,
			     fmt::join(fixes, ","))
	      << std::endl;
  }

  int get_snaps(const hobject_t& hoid,
		std::set<snapid_t>* snaps_set) const;

  tl::expected<std::set<snapid_t>, result_t> get_snaps(
    const hobject_t& oid) const final;

  tl::expected<std::set<snapid_t>, result_t> get_snaps_check_consistency(
    const hobject_t& oid) const final
  {
    /// \todo for now
    return get_snaps(oid);
  }

  void set_snaps(const hobject_t& hoid, const std::vector<snapid_t>& snaps)
  {
    std::cout
      << fmt::format("{}: ({}) -> #{} {}", __func__, hoid, snaps.size(), snaps)
      << std::endl;
    std::set<snapid_t> snaps_set(snaps.begin(), snaps.end());
    m_snaps[hoid] = snaps_set;
  }

  void set_snaps(const ScrubGenerator::all_clones_snaps_t& clones_snaps)
  {
    for (const auto& [clone, snaps] : clones_snaps) {
      std::cout << fmt::format("{}: ({}) -> #{} {}",
			       __func__,
			       clone,
			       snaps.size(),
			       snaps)
		<< std::endl;
      std::set<snapid_t> snaps_set(snaps.begin(), snaps.end());
      m_snaps[clone] = snaps_set;
    }
  }

  bool m_primary{true};
  spg_t m_spg;
  LoggerSinkSet& m_logger;
  OSDMapRef m_osdmap;
  pg_info_t m_info;
  object_stat_sum_t m_stats;

  // the "snap-mapper" database (returned by get_snaps())
  std::map<hobject_t, std::set<snapid_t>> m_snaps;
};

int TestScrubber::get_snaps(const hobject_t& hoid,
			    std::set<snapid_t>* snaps_set) const
{
  auto it = m_snaps.find(hoid);
  if (it == m_snaps.end()) {
    std::cout << fmt::format("{}: ({}) no snaps", __func__, hoid) << std::endl;
    return -ENOENT;
  }

  *snaps_set = it->second;
  std::cout << fmt::format("{}: ({}) -> #{} {}",
			   __func__,
			   hoid,
			   snaps_set->size(),
			   *snaps_set)
	    << std::endl;
  return 0;
}

tl::expected<std::set<snapid_t>, Scrub::SnapMapReaderI::result_t>
TestScrubber::get_snaps(const hobject_t& oid) const
{
  std::set<snapid_t> snapset;
  auto r = get_snaps(oid, &snapset);
  if (r >= 0) {
    return snapset;
  }
  return tl::make_unexpected(Scrub::SnapMapReaderI::result_t{
    Scrub::SnapMapReaderI::result_t::code_t::not_found,
    r});
}


// ///////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////


/// parameters for TestTScrubberBe construction
struct TestTScrubberBeParams {
  ScrubGenerator::pool_conf_t pool_conf;
  ScrubGenerator::RealObjsConf objs_conf;
  int num_osds;
};


// ///////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////


// the actual owner of the OSD "objects" that are used by
// the mockers
class TestTScrubberBe : public ::testing::Test {
 public:
  // the test data source
  virtual TestTScrubberBeParams inject_params() = 0;

  // initial test data
  ScrubGenerator::MockLog logger;
  ScrubGenerator::pool_conf_t pool_conf;
  ScrubGenerator::RealObjsConf real_objs;
  int num_osds{0};

  // ctor & initialization

  TestTScrubberBe() = default;
  ~TestTScrubberBe() = default;
  void SetUp() override;
  void TearDown() override;

  /**
   * Create the set of scrub-maps supposedly sent by the replica (or
   * generated by the Primary). Then - create the snap-sets for all
   * the objects in the set.
   */
  void fake_a_scrub_set(ScrubGenerator::RealObjsConfList& all_sets,
                        std::set<pg_shard_t> acting_shards);

  std::unique_ptr<TestScrubBackend> sbe;

  spg_t spg;
  pg_shard_t i_am;  // set to 'my osd and no shard'
  std::set<pg_shard_t> acting_shards;
  std::vector<int> acting_osds;
  int acting_primary;

  std::unique_ptr<TestScrubber> test_scrubber;

  int64_t pool_id;
  pg_pool_t pool_info;

  OSDMapRef osdmap;

  std::shared_ptr<PGPool> pool;
  pg_info_t info;

  std::unique_ptr<TestPg> test_pg;

  // generated sets of "objects" for the active OSDs
  ScrubGenerator::RealObjsConfList real_objs_list;

 protected:
  /**
   * Create the OSDmap and populate it with one pool, based on
   * the pool configuration.
   * For now - only replicated pools are supported.
   */
  OSDMapRef setup_map(int num_osds, const ScrubGenerator::pool_conf_t& pconf);

  /**
   * Create a PG in the one pool we have. Fake the PG info.
   * Use the primary of the PG to determine "who we are".
   *
   * \returns the PG info
   */
  pg_info_t setup_pg_in_map();

  /**
   * EC requires that set_stripe_data() is called before the
   * ScrubBackend object is constructed
   */
  virtual void ec_set_stripe_info() {}
};


// ///////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////

void TestTScrubberBe::SetUp()
{
  std::cout << "TestTScrubberBe::SetUp()" << std::endl;
  logger.err_count = 0;

  // fetch test configuration
  auto params = inject_params();
  pool_conf = params.pool_conf;
  real_objs = params.objs_conf;
  num_osds = params.num_osds;

  // create the OSDMap

  osdmap = setup_map(num_osds, pool_conf);

  std::cout << "osdmap: " << *osdmap << std::endl;

  // extract the pool from the osdmap

  pool_id = osdmap->lookup_pg_pool_name(pool_conf.name);
  const pg_pool_t* ext_pool_info = osdmap->get_pg_pool(pool_id);
  pool =
    std::make_shared<PGPool>(osdmap, pool_id, *ext_pool_info, pool_conf.name);

  std::cout << "pool: " << pool->info << std::endl;

  // a PG in that pool?
  info = setup_pg_in_map();
  std::cout << fmt::format("PG info: {}", info) << std::endl;

  real_objs_list =
    ScrubGenerator::make_real_objs_conf(pool_id,
                                        real_objs,
                                        acting_osds,
                                        acting_shards,
                                        pool_conf
                                          .erasure_code_profile.has_value());

  // now we can create the main mockers

  // the "PgScrubber"
  test_scrubber = std::make_unique<TestScrubber>(spg, osdmap, logger);

  // the "PG" (and its backend)
  test_pg = std::make_unique<TestPg>(pool, info, i_am);
  std::cout << fmt::format("{}: acting: {}", __func__, acting_shards)
	    << std::endl;

  // an EC-only hook to allocate & init the 'stripe conf' structure
  ec_set_stripe_info();
  sbe = std::make_unique<TestScrubBackend>(*test_scrubber,
					   *test_pg,
					   i_am,
					   /* repair? */ false,
					   scrub_level_t::deep,
					   acting_shards);

  // create a osd-num only copy of the relevant OSDs
  acting_osds.reserve(acting_shards.size());
  for (const auto& shard : acting_shards) {
    acting_osds.push_back(shard.osd);
  }

  sbe->new_chunk();
  fake_a_scrub_set(real_objs_list, acting_shards);
}


// Note: based on TestOSDMap.cc.
OSDMapRef TestTScrubberBe::setup_map(int num_osds,
				     const ScrubGenerator::pool_conf_t& pconf)
{
  auto osdmap = std::make_shared<OSDMap>();
  uuid_d fsid;
  osdmap->build_simple(g_ceph_context, 0, fsid, num_osds);
  OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
  pending_inc.fsid = osdmap->get_fsid();
  entity_addrvec_t sample_addrs;
  sample_addrs.v.push_back(entity_addr_t());
  uuid_d sample_uuid;
  for (int i = 0; i < num_osds; ++i) {
    sample_uuid.generate_random();
    sample_addrs.v[0].nonce = i;
    pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    pending_inc.new_up_client[i] = sample_addrs;
    pending_inc.new_up_cluster[i] = sample_addrs;
    pending_inc.new_hb_back_up[i] = sample_addrs;
    pending_inc.new_hb_front_up[i] = sample_addrs;
    pending_inc.new_weight[i] = CEPH_OSD_IN;
    pending_inc.new_uuid[i] = sample_uuid;
  }
  osdmap->apply_incremental(pending_inc);

  // create a pool
  OSDMap::Incremental new_pool_inc(osdmap->get_epoch() + 1);
  new_pool_inc.new_pool_max = osdmap->get_pool_max();
  new_pool_inc.fsid = osdmap->get_fsid();
  uint64_t pool_id = ++new_pool_inc.new_pool_max;
  pg_pool_t empty;
  auto p = new_pool_inc.get_new_pool(pool_id, &empty);
  p->size = pconf.size;
  p->min_size = pconf.min_size;
  p->set_pg_num(pconf.pg_num);
  p->set_pgp_num(pconf.pgp_num);
  p->type = pconf.type;
  p->crush_rule = 0;
  p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  new_pool_inc.new_pool_names[pool_id] = pconf.name;
  if (pconf.erasure_code_profile) {
    osdmap->set_erasure_code_profile(
        pconf.erasure_code_profile->erasure_code_profile_name,
        pconf.erasure_code_profile->erasure_code_profile);
    p->erasure_code_profile =
        pconf.erasure_code_profile->erasure_code_profile_name;
    p->set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    p->set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  }
  osdmap->apply_incremental(new_pool_inc);
  return osdmap;
}

pg_info_t TestTScrubberBe::setup_pg_in_map()
{
  pg_t rawpg(0, pool_id);
  pg_t pgid = osdmap->raw_pg_to_pg(rawpg);
  std::vector<int> up_osds;
  int up_primary;

  osdmap->pg_to_up_acting_osds(pgid,
			       &up_osds,
			       &up_primary,
			       &acting_osds,
			       &acting_primary);

  std::cout << fmt::format(
		 "{}: pg: {} up_osds: {} up_primary: {} acting_osds: {} "
		 "acting_primary: "
		 "{}",
		 __func__,
		 pgid,
		 up_osds,
		 up_primary,
		 acting_osds,
		 acting_primary)
	    << std::endl;

  spg = spg_t{pgid};

  // the 'acting shards' set - the one actually used by the scrubber
  shard_id_t shard;
  std::for_each(acting_osds.begin(), acting_osds.end(), [&](int osd) {
    acting_shards.insert(pg_shard_t{osd, shard});
    ++shard;
  });
  std::cout << fmt::format("{}: acting_shards: {}", __func__, acting_shards)
	    << std::endl;

  shard_id_t osd_shard =
      std::find_if(
          acting_shards.begin(), acting_shards.end(),
          [&](pg_shard_t pg_shard) { return i_am.osd == pg_shard.osd; })
          ->shard;

  i_am = pg_shard_t{up_primary, osd_shard};
  std::cout << fmt::format("{}: spg: {} and I am {}", __func__, spg, i_am)
            << std::endl;

  pg_info_t info;
  info.pgid = spg;
  /// \todo: handle the epochs:
  // info.last_update = osdmap->get_epoch();
  // info.last_complete = osdmap->get_epoch();
  // info.last_osdmap_epoch = osdmap->get_epoch();
  // info.history.last_epoch_marked_removed = osdmap->get_epoch();
  info.last_user_version = 1;
  info.purged_snaps = {};
  info.last_user_version = 1;
  info.history.last_epoch_clean = osdmap->get_epoch();
  info.history.last_epoch_split = osdmap->get_epoch();
  info.history.last_epoch_marked_full = osdmap->get_epoch();
  info.last_backfill = hobject_t::get_max();
  return info;
}

void TestTScrubberBe::TearDown()
{
  EXPECT_EQ(logger.err_count, logger.expected_err_count);
}

void TestTScrubberBe::fake_a_scrub_set(
    ScrubGenerator::RealObjsConfList& all_sets,
    std::set<pg_shard_t> acting_shards) {
  for (int osd_num = 0; osd_num < pool_conf.size; ++osd_num) {
    ScrubMap smap;
    smap.valid_through = eversion_t{1, 1};
    smap.incr_since = eversion_t{1, 1};
    smap.has_omap_keys = true;	// to force omap checks

    // fill the map with the objects relevant to this OSD
    for (auto& obj : all_sets[osd_num]->objs) {
      std::cout << fmt::format("{}: object: {}", __func__, obj.ghobj.hobj)
		<< std::endl;
      ScrubGenerator::add_object(smap, obj, osd_num);
    }

    shard_id_t shard = std::find_if(acting_shards.begin(), acting_shards.end(),
                                    [&osd_num](pg_shard_t pg_shard) {
                                      return osd_num == pg_shard.osd;
                                    })
                           ->shard;

    std::cout << fmt::format("{}: {} inserting smap {:D}",
			     __func__,
			     osd_num,
			     smap)
	      << std::endl;
    sbe->insert_faked_smap(pg_shard_t{osd_num, shard}, smap);
  }

  // create the snap_mapper state

  for (const auto& robj : all_sets[i_am.osd]->objs) {

    std::cout << fmt::format("{}: object: {}", __func__, robj.ghobj.hobj)
	      << std::endl;

    if (robj.ghobj.hobj.snap == CEPH_NOSNAP) {
      // head object
      auto objects_snapset = ScrubGenerator::all_clones(robj);
      test_scrubber->set_snaps(objects_snapset);
    }
  }
}

void TestScrubBackend::insert_faked_smap(pg_shard_t shard, const ScrubMap& smap)
{
  ASSERT_TRUE(this_chunk.has_value());
  std::cout << fmt::format("{}: inserting faked smap for osd {}",
			   __func__,
			   shard.osd)
	    << std::endl;
  this_chunk->received_maps[shard] = smap;
}


// ///////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////


using namespace ScrubGenerator;

class TestTScrubberBe_data_1 : public TestTScrubberBe {
 public:
  TestTScrubberBe_data_1() : TestTScrubberBe() {}

  // test configuration
  pool_conf_t pl{3,           3, 3, 3, "rep_pool", pg_pool_t::TYPE_REPLICATED,
                 std::nullopt};

  TestTScrubberBeParams inject_params() override
  {
    std::cout << fmt::format("{}: injecting params (minimal snaps conf.)",
			     __func__)
	      << std::endl;
    return TestTScrubberBeParams{
      /* pool_conf */ pl,
      /* real_objs_conf */ ScrubDatasets::minimal_snaps_configuration,
      /*num_osds */ 3};
  }
};

// some basic sanity checks
// (mainly testing the constructor)

TEST_F(TestTScrubberBe_data_1, creation_1)
{
  /// \todo copy some osdmap tests from TestOSDMap.cc
  ASSERT_TRUE(sbe);
  ASSERT_TRUE(sbe->get_is_replicated());
  ASSERT_FALSE(sbe->get_m_repair());
  sbe->update_repair_status(true);
  ASSERT_TRUE(sbe->get_m_repair());

  // make sure *I* do not appear in 'all_but_me' set of OSDs
  auto others = sbe->all_but_me();
  auto in_others = std::find(others.begin(), others.end(), i_am);
  EXPECT_EQ(others.end(), in_others);
}


TEST_F(TestTScrubberBe_data_1, smaps_creation_1)
{
  ASSERT_TRUE(sbe);
  ASSERT_EQ(sbe->get_omap_stats().omap_bytes, 0);

  // for test data 'minimal_snaps_configuration':
  // scrub_compare_maps() should not emmit any error, nor
  // return any snap-mapper fix
  auto [incons, fix_list] = sbe->scrub_compare_maps(true, *test_scrubber);

  EXPECT_EQ(fix_list.size(), 0);  // snap-mapper fix should be empty

  EXPECT_EQ(incons.size(), 0);	// no inconsistency

  // make sure the test did execute *something*
  EXPECT_TRUE(sbe->get_omap_stats().omap_bytes != 0);
}


// whitebox testing (OK if failing after a change to the backend internals)


// blackbox testing - testing the published functionality
// (should not depend on internals of the backend)


/// corrupt the snap_mapper data
TEST_F(TestTScrubberBe_data_1, snapmapper_1)
{
  using snap_mapper_op_t = Scrub::snap_mapper_op_t;
  ASSERT_TRUE(sbe);

  // a bogus version of hobj_ms1_snp30 (a clone) snap_ids
  hobject_t hobj_ms1_snp30_inpool = hobject_t{ScrubDatasets::hobj_ms1_snp30};
  hobj_ms1_snp30_inpool.pool = pool_id;
  all_clones_snaps_t bogus_30;
  bogus_30[hobj_ms1_snp30_inpool] = {0x333, 0x666};

  test_scrubber->set_snaps(bogus_30);
  auto [incons, fix_list] = sbe->scrub_compare_maps(true, *test_scrubber);

  EXPECT_EQ(fix_list.size(), 1);

  // debug - print the fix-list:
  for (const auto& fix : fix_list) {
    std::cout << fmt::format("snapmapper_1: fix {}: {} {}->{}",
			     fix.hoid,
			     (fix.op == snap_mapper_op_t::add ? "add" : "upd"),
			     fix.wrong_snaps,
			     fix.snaps)
	      << std::endl;
  }
  EXPECT_EQ(fix_list[0].hoid, hobj_ms1_snp30_inpool);
  EXPECT_EQ(fix_list[0].snaps, std::set<snapid_t>{0x30});

  EXPECT_EQ(incons.size(), 0);	// no inconsistency
}

// a dataset similar to 'minimal_snaps_configuration',
// but with the hobj_ms1_snp30 clone being modified by a corruption
// function
class TestTScrubberBe_data_2 : public TestTScrubberBe {
 public:
  TestTScrubberBe_data_2() : TestTScrubberBe() {}

  // basic test configuration - 3 OSDs, all involved in the pool
  pool_conf_t pl{3, 3, 3, 3, "rep_pool", pg_pool_t::TYPE_REPLICATED,
                 std::nullopt};

  TestTScrubberBeParams inject_params() override
  {
    std::cout << fmt::format(
		   "{}: injecting params (minimal-snaps + size change)",
		   __func__)
	      << std::endl;
    TestTScrubberBeParams params{
      /* pool_conf */ pl,
      /* real_objs_conf */ ScrubDatasets::minimal_snaps_configuration,
      /*num_osds */ 3};

    // inject a corruption function that will modify osd.0's version of
    // the object
    params.objs_conf.objs[0].corrupt_funcs = &ScrubDatasets::crpt_funcs_set1;
    return params;
  }
};

TEST_F(TestTScrubberBe_data_2, smaps_clone_size)
{
  ASSERT_TRUE(sbe);
  EXPECT_EQ(sbe->get_omap_stats().omap_bytes, 0);
  logger.set_expected_err_count(1);
  auto [incons, fix_list] = sbe->scrub_compare_maps(true, *test_scrubber);

  EXPECT_EQ(fix_list.size(), 0);  // snap-mapper fix should be empty

  EXPECT_EQ(incons.size(), 1);	// one inconsistency
}

class TestTScrubberBeECCorruptShards : public TestTScrubberBe {
 private:
  int seed;

 protected:
  std::mt19937 rng;
  int8_t k;
  int8_t m;
  int m_chunk_size = 4;

 public:
  TestTScrubberBeECCorruptShards()
      : TestTScrubberBe(),
        seed(time(0)),
        rng(seed),
        k((rng() % 10) + 2),
        m((rng() % std::min(k - 1, 4)) + 1) {
    std::cout << "Using seed " << seed << std::endl;
  }

  std::size_t m_expected_inconsistencies = 0;
  std::size_t m_expected_error_count = 0;

  // basic test configuration - 3 OSDs, all involved in the pool
  erasure_code_profile_conf_t ec_profile{
      "scrub_ec_profile",
      {{"k", fmt::format("{}", k)},
       {"m", fmt::format("{}", m)},
       {"plugin", "isa"},
       {"technique", "reed_sol_van"},
       {"stripe_unit", fmt::format("{}", m_chunk_size)}}};

  pool_conf_t pl{3,         3, k + m, k + 1, "ec_pool", pg_pool_t::TYPE_ERASURE,
                 ec_profile};

  TestTScrubberBeParams inject_params() override {
    std::cout << fmt::format(
                     "{}: injecting params (minimal-snaps + size change)",
                     __func__)
              << std::endl;
    TestTScrubberBeParams params{
        /* pool_conf */ pl,
        /* real_objs_conf */
        ScrubGenerator::make_erasure_code_configuration(k, m),
        /*num_osds */ k + m};

    return params;
  }

  void ec_set_stripe_info() override
  {
    test_pg->set_stripe_info(k, m, k * m_chunk_size, &test_pg->m_pool->info);
  }
};

class TestTScrubberBeECNoCorruptShards : public TestTScrubberBeECCorruptShards {
 public:
  TestTScrubberBeECNoCorruptShards() : TestTScrubberBeECCorruptShards() {}
};

TEST_F(TestTScrubberBeECNoCorruptShards, ec_parity_inconsistency) {
  ASSERT_TRUE(sbe);  // Assert we have a scrubber backend
  logger.set_expected_err_count(
      0);  // Set the number of errors we expect to see

  auto [incons, fix_list] = sbe->scrub_compare_maps(true, *test_scrubber);

  EXPECT_EQ(incons.size(), 0);
}

class TestTScrubberBeECSingleCorruptDataShard
    : public TestTScrubberBeECCorruptShards {
 public:
  TestTScrubberBeECSingleCorruptDataShard()
      : TestTScrubberBeECCorruptShards() {}

  TestTScrubberBeParams inject_params() override {
    TestTScrubberBeParams params =
        TestTScrubberBeECCorruptShards::inject_params();
    corrupt_funcs = make_erasure_code_hash_corruption_functions(k + m);
    params.objs_conf.objs[(rng() % k)].corrupt_funcs = &corrupt_funcs;
    return params;
  }

 private:
  CorruptFuncList corrupt_funcs;
};

TEST_F(TestTScrubberBeECSingleCorruptDataShard, ec_parity_inconsistency) {
  ASSERT_TRUE(sbe);  // Assert we have a scrubber backend
  logger.set_expected_err_count(
      1);  // Set the number of errors we expect to see

  auto [incons, fix_list] = sbe->scrub_compare_maps(true, *test_scrubber);

  EXPECT_EQ(incons.size(), 1);
}

class TestTScrubberBeECCorruptParityShard
    : public TestTScrubberBeECCorruptShards {
 public:
  TestTScrubberBeECCorruptParityShard() : TestTScrubberBeECCorruptShards() {}

  TestTScrubberBeParams inject_params() override {
    TestTScrubberBeParams params =
        TestTScrubberBeECCorruptShards::inject_params();
    corrupt_funcs = make_erasure_code_hash_corruption_functions(k + m);
    params.objs_conf.objs[k].corrupt_funcs = &corrupt_funcs;
    return params;
  }

 private:
  CorruptFuncList corrupt_funcs;
};

TEST_F(TestTScrubberBeECCorruptParityShard, ec_parity_inconsistency) {
  ASSERT_TRUE(sbe);  // Assert we have a scrubber backend
  logger.set_expected_err_count(
      1);  // Set the number of errors we expect to see

  auto [incons, fix_list] = sbe->scrub_compare_maps(true, *test_scrubber);

  EXPECT_EQ(incons.size(), 1);
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub
// --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* " End:
