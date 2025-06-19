#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>
#include <optional>
#include <string>
#include <utility>

#include "ProgramOptionReader.h"
#include "common/io_exerciser/IoOp.h"
#include "common/io_exerciser/IoSequence.h"
#include "common/io_exerciser/Model.h"
#include "common/split.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/random.h"
#include "librados/librados_asio.h"

/* Overview
 *
 * class SelectObjectSize
 *   Selects min and max object sizes for a test
 *
 * class SelectBlockSize
 *   Selects a block size for a test
 *
 * class SelectNumThreads
 *   Selects number of threads for a test
 *
 * class SelectErasureKM
 *   Selects an EC k and m value for a test
 *
 * class SelectErasureChunkSize
 *   Selects a chunk size/stripe unit for the test
 *
 * class SelectErasurePlugin
 *   Selects an plugin for a test
 *
 * class SelectErasurePacketSize
 *   Selects a packetsize to be used by jerasure
 *
 * class SelectErasureC
 *   Potentially selects a C value to be used for the shec plugin
 *
 * class SelectErasureW
 *   Potentially selects a W value to be used for the jerasure plugin
 *
 * class SelectErasurePool
 *   Selects an EC pool (plugin,k and m) for a test. Also creates the
 *   pool as well.
 *
 * class SelectErasureProfile
 *   Selects an EC profile for a test. Will create one if no name is specified
 *
 * class SelectSeqRange
 *   Selects a sequence or range of sequences for a test
 *
 * class SelectErasurePool
 *   Selects a pool name for a test
 *
 * class TestObject
 *   Runs a test against an object, generating IOSequence
 *   and applying them to an IoExerciser
 *
 * class TestRunner
 *   Determines test type to run, creates and orchestrates automated and
 *   interactive tests as well as creating a test object for each automated test
 *   we want to run in parallel
 *
 * main
 *   Run sequences of I/O with data integrity checking to
 *   one or more objects in parallel. Without arguments
 *   runs a default configuration against one object.
 *   Command arguments can select alternative
 *   configurations. Alternatively running against
 *   multiple objects with --objects <n> will select a
 *   random configuration for all but the first object.
 */

namespace po = boost::program_options;

namespace ceph {
class ErasureCodePlugin;

namespace io_sequence {
namespace tester {
// Choices for min and max object size
inline static constexpr size_t object_size_array_size = 10;
inline static constexpr std::array<std::pair<int, int>, object_size_array_size>
    object_size_choices = {{{1, 32},  // Default - best for boundary checking
                          {12, 14},
                          {28, 30},
                          {36, 38},
                          {42, 44},
                          {52, 54},
                          {66, 68},
                          {72, 74},
                          {83, 83},
                          {97, 97}}};

using SelectObjectSize =
    ProgramOptionSelector<std::pair<int, int>,
                          io_sequence::tester::object_size_array_size,
                          io_sequence::tester::object_size_choices>;

// Choices for block size
inline static constexpr int block_size_array_size = 5;
inline static constexpr std::array<uint64_t, block_size_array_size> block_size_choices = {
    {2048,  // Default - test boundaries for EC 4K chunk size
     512, 3767, 4096, 32768}};

// Choices for block size
inline static constexpr int block_size_array_size_stable = 2;
inline static constexpr std::array<uint64_t, block_size_array_size_stable> block_size_choices_stable = {
  {2048,  // Default - test boundaries for EC 4K chunk size
   32768}};


using SelectBlockSize =
  StableOptionSelector<uint64_t,
                        io_sequence::tester::block_size_array_size,
                        io_sequence::tester::block_size_choices,
                        io_sequence::tester::block_size_array_size_stable,
                        io_sequence::tester::block_size_choices_stable>;

// Choices for number of threads
inline static constexpr int thread_array_size = 4;
inline static constexpr std::array<int, thread_array_size> thread_count_choices = {
    {1,  // Default
     2, 4, 8}};

using SelectNumThreads =
    ProgramOptionSelector<int,
                          io_sequence::tester::thread_array_size,
                          io_sequence::tester::thread_count_choices>;

class SelectSeqRange
    : public ProgramOptionReader<std::pair<ceph::io_exerciser ::Sequence,
                                           ceph::io_exerciser ::Sequence>> {
 public:
  SelectSeqRange(po::variables_map& vm);
  const std::pair<ceph::io_exerciser::Sequence, ceph::io_exerciser::Sequence>
  select() override;
};

// Choices for plugin
inline static constexpr int plugin_array_size = 5;
inline static constexpr std::array<std::string_view, plugin_array_size>
    plugin_choices = {{"jerasure", "isa", "clay", "shec", "lrc"}};

inline static constexpr int plugin_array_size_stable = 2;
inline static constexpr std::array<std::string_view, plugin_array_size_stable>
    plugin_choices_stable = {{"jerasure", "isa"}};

using SelectErasurePlugin =
    StableOptionSelector<std::string_view,
                          io_sequence::tester::plugin_array_size,
                          io_sequence::tester::plugin_choices,
                          io_sequence::tester::plugin_array_size_stable,
                          io_sequence::tester::plugin_choices_stable>;

class SelectErasureKM
    : public ProgramOptionGeneratedSelector<std::pair<int, int>> {
 public:
  SelectErasureKM(ceph::util::random_number_generator<int>& rng,
                  po::variables_map& vm,
                  std::string_view plugin,
                  const std::optional<std::string>& technique,
                  bool first_use);

  const std::vector<std::pair<int, int>> generate_selections() override;

 private:
  ceph::util::random_number_generator<int>& rng;

  std::string_view plugin;
  std::optional<std::string> technique;
};

namespace shec {
class SelectErasureC : public ProgramOptionGeneratedSelector<uint64_t> {
 public:
  SelectErasureC(ceph::util::random_number_generator<int>& rng,
                 po::variables_map& vm,
                 std::string_view plugin,
                 const std::optional<std::pair<int, int>>& km,
                 bool first_use);

  const std::vector<uint64_t> generate_selections() override;

 private:
  ceph::util::random_number_generator<int>& rng;

  std::string_view plugin;
  std::optional<std::pair<int, int>> km;
};
}  // namespace shec

namespace jerasure {
class SelectErasureW : public ProgramOptionGeneratedSelector<uint64_t> {
 public:
  SelectErasureW(ceph::util::random_number_generator<int>& rng,
                 po::variables_map& vm,
                 std::string_view plugin,
                 const std::optional<std::string_view>& technique,
                 const std::optional<std::pair<int, int>>& km,
                 const std::optional<uint64_t>& packetsize,
                 bool first_use);

  const std::vector<uint64_t> generate_selections() override;

 private:
  ceph::util::random_number_generator<int>& rng;

  std::string_view plugin;
  std::optional<std::string_view> technique;
  std::optional<std::pair<int, int>> km;
  std::optional<uint64_t> packetsize;
};

class SelectErasurePacketSize
    : public ProgramOptionGeneratedSelector<uint64_t> {
 public:
  SelectErasurePacketSize(ceph::util::random_number_generator<int>& rng,
                          po::variables_map& vm,
                          std::string_view plugin,
                          const std::optional<std::string_view>& technique,
                          const std::optional<std::pair<int, int>>& km,
                          bool first_use);

  const std::vector<uint64_t> generate_selections() override;

 private:
  ceph::util::random_number_generator<int>& rng;

  std::string_view plugin;
  std::optional<std::string_view> technique;
  std::optional<std::pair<int, int>> km;
};
}  // namespace jerasure

namespace lrc {
// Choices for lrc mappings and layers. The index selected for the mapping
// matches what index will be chosen from the layers array.
inline static constexpr int mapping_layer_array_sizes = 15;

inline static std::array<std::string, mapping_layer_array_sizes> mapping_choices = {{
    "_DD",
    "_DDD",
    "_DDDD",
    "_DDDDD",
    "_DDDDDD",
    "_D_D",
    "_D_DD",
    "_D_DDD",
    "_D_DDDD",
    "_D_DDDDD",
    "_D_D_",
    "_D_D_D",
    "_D_D_DD",
    "_D_D_DDD",
    "_D_D_DDDD",
}};

inline static std::array<std::string, mapping_layer_array_sizes> layer_choices = {{
    "[[\"cDD\",\"\"]]",
    "[[\"cDDD\",\"\"]]",
    "[[\"cDDDD\",\"\"]]",
    "[[\"cDDDDD\",\"\"]]",
    "[[\"cDDDDDD\",\"\"]]",
    "[[\"cDcD\",\"\"]]",
    "[[\"cDcDD\",\"\"]]",
    "[[\"cDcDDD\",\"\"]]",
    "[[\"cDcDDDD\",\"\"]]",
    "[[\"cDcDDDDD\",\"\"]]",
    "[[\"cDcDc\",\"\"]]",
    "[[\"cDcDcD\",\"\"]]",
    "[[\"cDcDcDD\",\"\"]]",
    "[[\"cDcDcDDD\",\"\"]]",
    "[[\"cDcDcDDDD\",\"\"]]",
}};

using SelectMapping =
    ProgramOptionSelector<std::string,
                          io_sequence::tester::lrc::mapping_layer_array_sizes,
                          io_sequence::tester::lrc::mapping_choices>;

using SelectLayers =
    ProgramOptionSelector<std::string,
                          io_sequence::tester::lrc::mapping_layer_array_sizes,
                          io_sequence::tester::lrc::layer_choices>;

class SelectMappingAndLayers {
 public:
  SelectMappingAndLayers(ceph::util::random_number_generator<int>& rng,
                         po::variables_map& vm,
                         bool first_use);
  const std::pair<std::string, std::string> select();

 private:
  uint64_t rng_seed;

  ceph::util::random_number_generator<int> mapping_rng;
  ceph::util::random_number_generator<int> layers_rng;

  SelectMapping sma;
  SelectLayers sly;
};
}  // namespace lrc

class SelectErasureTechnique
    : public ProgramOptionGeneratedSelector<std::string> {
 public:
  SelectErasureTechnique(ceph::util::random_number_generator<int>& rng,
                         po::variables_map& vm,
                         std::string_view plugin,
                         bool first_use);

  const std::vector<std::string> generate_selections() override;

 private:
  ceph::util::random_number_generator<int>& rng;

  std::string_view plugin;
  bool stable;
};

class SelectErasureChunkSize : public ProgramOptionGeneratedSelector<uint64_t> {
 public:
  SelectErasureChunkSize(ceph::util::random_number_generator<int>& rng,
                         po::variables_map& vm,
                         ErasureCodeInterfaceRef ec_impl,
                         bool first_use);
  const std::vector<uint64_t> generate_selections() override;

 private:
  ceph::util::random_number_generator<int>& rng;

  ErasureCodeInterfaceRef ec_impl;
};

struct Profile {
  std::string name;
  std::string_view plugin;
  std::optional<std::string> technique;
  std::optional<std::pair<int, int>> km;
  std::optional<uint64_t> packet_size;
  std::optional<int> c;
  std::optional<int> w;
  std::optional<std::string> mapping;
  std::optional<std::string> layers;
  std::optional<uint64_t> chunk_size;
  std::optional<bool> jerasure_per_chunk_alignment;
};

class SelectErasureProfile : public ProgramOptionReader<Profile> {
 public:
  SelectErasureProfile(boost::intrusive_ptr<CephContext> cct,
                       ceph::util::random_number_generator<int>& rng,
                       po::variables_map& vm, librados::Rados& rados,
                       bool dry_run, bool first_use);
  const Profile select() override;
  void create(const Profile& profile);
  const Profile selectExistingProfile(const std::string& profile_name);

 private:
  boost::intrusive_ptr<CephContext> cct;
  librados::Rados& rados;
  bool dry_run;
  ceph::util::random_number_generator<int>& rng;
  po::variables_map& vm;

  bool first_use;

  SelectErasurePlugin spl;
  lrc::SelectMappingAndLayers sml;

  std::unique_ptr<ErasureCodePlugin> erasure_code;
};

class SelectErasurePool : public ProgramOptionReader<std::string> {
 public:
  SelectErasurePool(boost::intrusive_ptr<CephContext> cct,
                    ceph::util::random_number_generator<int>& rng,
                    po::variables_map& vm,
                    librados::Rados& rados,
                    bool dry_run,
                    bool allow_pool_autoscaling,
                    bool allow_pool_balancer,
                    bool allow_pool_deep_scrubbing,
                    bool allow_pool_scrubbing,
                    bool test_recovery,
                    bool disable_pool_ec_optimizations);
  const std::string select() override;
  std::string create();
  void configureServices(bool allow_pool_autoscaling,
                         bool allow_pool_balancer,
                         bool allow_pool_deep_scrubbing,
                         bool allow_pool_scrubbing,
                         bool test_recovery);

  inline bool get_allow_pool_autoscaling() { return allow_pool_autoscaling; }
  inline bool get_allow_pool_balancer() { return allow_pool_balancer; }
  inline bool get_allow_pool_deep_scrubbing() {
    return allow_pool_deep_scrubbing;
  }
  inline bool get_allow_pool_scrubbing() { return allow_pool_scrubbing; }
  inline bool get_allow_pool_ec_optimizations() {
    return !disable_pool_ec_optimizations;
  }
  inline std::optional<Profile> getProfile() { return profile; }

 private:
  librados::Rados& rados;
  bool dry_run;

  bool allow_pool_autoscaling;
  bool allow_pool_balancer;
  bool allow_pool_deep_scrubbing;
  bool allow_pool_scrubbing;
  bool test_recovery;
  bool disable_pool_ec_optimizations;

  bool first_use;

  SelectErasureProfile sep;

  std::optional<Profile> profile;
};

class TestObject {
 public:
  TestObject(const std::string oid,
             librados::Rados& rados,
             boost::asio::io_context& asio,
             ceph::io_sequence::tester::SelectBlockSize& sbs,
             ceph::io_sequence::tester::SelectErasurePool& spl,
             ceph::io_sequence::tester::SelectObjectSize& sos,
             ceph::io_sequence::tester::SelectNumThreads& snt,
             ceph::io_sequence::tester::SelectSeqRange& ssr,
             ceph::util::random_number_generator<int>& rng,
             ceph::mutex& lock,
             ceph::condition_variable& cond,
             bool dryrun,
             bool verbose,
             std::optional<int> seqseed,
             bool testRecovery);

  int get_num_io();
  bool readyForIo();
  bool next();
  bool finished();

 protected:
  std::unique_ptr<ceph::io_exerciser::Model> exerciser_model;
  std::pair<int, int> obj_size_range;
  std::pair<ceph::io_exerciser::Sequence, ceph::io_exerciser::Sequence>
      seq_range;
  ceph::io_exerciser::Sequence curseq;
  std::unique_ptr<ceph::io_exerciser::IoSequence> seq;
  std::unique_ptr<ceph::io_exerciser::IoOp> op;
  bool done;
  ceph::util::random_number_generator<int>& rng;
  bool verbose;
  std::optional<int> seqseed;
  std::optional<std::pair<int, int>> pool_km;
  std::optional<std::pair<std::string_view, std::string_view>>
      pool_mappinglayers;
  bool testrecovery;
};

class TestRunner {
 public:
  TestRunner(boost::intrusive_ptr<CephContext> cct,
             po::variables_map& vm,
             librados::Rados& rados);
  ~TestRunner();

  bool run_test();

 private:
  librados::Rados& rados;
  int seed;
  ceph::util::random_number_generator<int> rng;

  ceph::io_sequence::tester::SelectBlockSize sbs;
  ceph::io_sequence::tester::SelectObjectSize sos;
  ceph::io_sequence::tester::SelectErasurePool spo;
  ceph::io_sequence::tester::SelectNumThreads snt;
  ceph::io_sequence::tester::SelectSeqRange ssr;

  boost::asio::io_context asio;
  std::thread thread;
  std::optional<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
      guard;
  ceph::mutex lock = ceph::make_mutex("RadosIo::lock");
  ceph::condition_variable cond;

  bool input_valid;

  bool verbose;
  bool dryrun;
  std::optional<int> seqseed;
  bool interactive;

  bool testrecovery;

  bool allow_pool_autoscaling;
  bool allow_pool_balancer;
  bool allow_pool_deep_scrubbing;
  bool allow_pool_scrubbing;
  bool disable_pool_ec_optimizations;

  bool show_sequence;
  bool show_help;

  int num_objects;
  std::string object_name;

  std::string line;
  ceph::split split = ceph::split("");
  ceph::spliterator tokens;

  void clear_tokens();
  std::string get_token(bool allow_eof = false);
  std::optional<std::string> get_optional_token();
  uint64_t get_numeric_token();
  std::optional<uint64_t> get_optional_numeric_token();

  bool run_automated_test();

  bool run_interactive_test();

  void help();
  void list_sequence(bool testrecovery);
};
}  // namespace tester
}  // namespace io_sequence
}  // namespace ceph
