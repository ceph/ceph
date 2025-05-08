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

  bool is_waiting_for_unreadable_object() const final { return false; }

  std::shared_ptr<PGPool> m_pool;
  pg_info_t& m_info;
  pg_shard_t m_pshard;

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
  void fake_a_scrub_set(ScrubGenerator::RealObjsConfList& all_sets);

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
    ScrubGenerator::make_real_objs_conf(pool_id, real_objs, acting_osds);

  // now we can create the main mockers

  // the "PgScrubber"
  test_scrubber = std::make_unique<TestScrubber>(spg, osdmap, logger);

  // the "PG" (and its backend)
  test_pg = std::make_unique<TestPg>(pool, info, i_am);
  std::cout << fmt::format("{}: acting: {}", __func__, acting_shards)
	    << std::endl;
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
  fake_a_scrub_set(real_objs_list);
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

  // create a replicated pool
  OSDMap::Incremental new_pool_inc(osdmap->get_epoch() + 1);
  new_pool_inc.new_pool_max = osdmap->get_pool_max();
  new_pool_inc.fsid = osdmap->get_fsid();
  uint64_t pool_id = ++new_pool_inc.new_pool_max;
  pg_pool_t empty;
  auto p = new_pool_inc.get_new_pool(pool_id, &empty);
  p->size = pconf.size;
  p->set_pg_num(pconf.pg_num);
  p->set_pgp_num(pconf.pgp_num);
  p->type = pg_pool_t::TYPE_REPLICATED;
  p->crush_rule = 0;
  p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  new_pool_inc.new_pool_names[pool_id] = pconf.name;
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
  i_am = pg_shard_t{up_primary};
  std::cout << fmt::format("{}: spg: {} and I am {}", __func__, spg, i_am)
	    << std::endl;

  // the 'acting shards' set - the one actually used by the scrubber
  std::for_each(acting_osds.begin(), acting_osds.end(), [&](int osd) {
    acting_shards.insert(pg_shard_t{osd});
  });
  std::cout << fmt::format("{}: acting_shards: {}", __func__, acting_shards)
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
  ScrubGenerator::RealObjsConfList& all_sets)
{
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

    std::cout << fmt::format("{}: {} inserting smap {:D}",
			     __func__,
			     osd_num,
			     smap)
	      << std::endl;
    sbe->insert_faked_smap(pg_shard_t{osd_num}, smap);
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
  pool_conf_t pl{3, 3, 3, 3, "rep_pool"};

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
  pool_conf_t pl{3, 3, 3, 3, "rep_pool"};

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

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub
// --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* " End:
